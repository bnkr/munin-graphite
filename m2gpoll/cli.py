import argparse
import ConfigParser
import logging
import logging.handlers
import pickle
import re
import socket
import struct
import sys
import time
import signal
import threading

RE_LEFTRIGHT = re.compile(r"^(?P<left>\S+)\s+(?P<right>\S+)$")
RE_MUNIN_NODE_NAME = re.compile(r"^# munin node at\s+(?P<nodename>\S+)$")

threads = []

from m2gpoll.munin import Telnet

logger = logging.getLogger()

class MuninThread(threading.Thread):
    """Custom Threading class, one thread for each host in configuration."""

    def __init__(self, params, cmdlineargs):
        threading.Thread.__init__(self)
        self.name = params['host']
        self.shutdown = False
        # construct new namespace to pass it to the new Munin class instance
        # for better manipulation, just prepare writable dcfg "link" to new namespace
        cfg = argparse.Namespace()
        dcfg = vars(cfg)

        #construct final arguments Namespace
        for v in vars(cmdlineargs):
            try:
                dcfg[v] = params[v]
            except KeyError:
                dcfg[v] = getattr(cmdlineargs, v, None)

        self.munin = Telnet(hostname=self.name, args=cfg, thread=self)

    def run(self):
        logger.info("Starting thread for %s." % self.name)
        self.munin.go()
        logger.info("Finishing thread for %s." % self.name)

    def dostop(self):
        logger.info("Thread %s: Got signal to stop." % self.name)
        Telnet.shutdown = True

    def reload(self):
        self.munin.reload_plugins = True
        logger.info("Thread %s: Got signal to reload." % self.name)


###
# bellow are common function
###
def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Send Munin statistics to Graphite.")
    parser.add_argument("--config", "-c",
                        action="store",
                        default=False,
                        help="Configuration file with list of hosts and their plugins to fetch.")
    parser.add_argument("--host",
                        action="store",
                        default="localhost",
                        help="Munin host to query for stats. You can specify indirect node after ':', "
                             "i.e. --host localhost:remotenode. Default: %(default)s")
    parser.add_argument("--displayname",
                        default=False,
                        help="If defined, use this as the name to store metrics in Graphite instead of the Munin"
                             " hostname.")
    parser.add_argument("--carbon",
                        action="store",
                        help="Carbon host and Pickle port (ex: localhost:2004).")
    parser.add_argument("--filter",
                        action="store",
                        default='.*',
                        help="Regular expression for selecting only defined subset of received plugins.")
    parser.add_argument("--interval",
                        type=int,
                        default=60,
                        help="Interval (seconds) between polling Munin host for statistics. If set to 0, exit after "
                             "polling once. Default: %(default)s")
    parser.add_argument("--noop",
                        action="store_true",
                        help="Don't actually send Munin data to Carbon. Default: %(default)s")
    parser.add_argument("--noprefix",
                        action="store_true",
                        default=False,
                        help="Do not use a prefix on graphite target's name. Default: %(default)s")
    parser.add_argument("--prefix",
                        action="store",
                        default="servers",
                        help="Prefix used on graphite target's name. Default: %(default)s")
    parser.add_argument("--logtosyslog",
                        action="store_true",
                        help="Log to syslog. No output on the command line.")
    parser.add_argument("--verbose", "-v",
                        choices=[1, 2, 3],
                        default=2,
                        type=int,
                        help="Verbosity level. 1:ERROR, 2:INFO, 3:DEBUG. Default: %(default)d")

    args = parser.parse_args()
    return args


###
# stop all threads and exit
###
def handler_term(signum=signal.SIGTERM, frame=None):
    global threads

    for t in threads:
        t.dostop()


###
# set all threads to reload information about all munin-node's plugins
###
def handler_hup(signum, frame=None):
    global threads

    for t in threads:
        t.reload()


def read_configuration(configfile):
    """
    Returns False if configuration file is not readable, list of dictionaries otherwise

    Configuration options follow parameters described as command line options. All parameters are optional except host,
    displayname parameter is built from section name, so it is always presented too.

    Non-existent options are superseded by defaults

    Example:
    [servername]
    host=fqdn[:remotenode]
    port=4949
    carbon=carbonhostfqdn:port
    interval=60
    prefix=prefix for Graphite's target
    noprefix=True|False
    filter=^cpu.*

    @param configfile: full filepath to configuration file
    @rtype : object
    """

    cf = ConfigParser.ConfigParser()
    hostscfg = []
    try:
        cf.read(configfile)
        for section in cf.sections():
            di = {}
            for ki, vi in cf.items(section):
                # construct dictionary item
                di[ki] = vi
            if "host" in di.keys():
                di["displayname"] = section
                hostscfg.append(di)
    except ConfigParser.Error as e:
        logger.critical("Failed to parse configuration or command line options. Exception was %s. Giving up." % e)

    return hostscfg


def console_main():
    """Entry point for the poller application."""
    global threads

    args = parse_args()
    if args.verbose == 1:
        logging_level = logging.ERROR
    elif args.verbose == 3:
        logging_level = logging.DEBUG
    else:
        logging_level = logging.INFO

    #logging.basicConfig(format=LOGGING_FORMAT, level=logging_level)
    logger = logging.getLogger()
    logger.setLevel(logging_level)
    syslog = logging.handlers.SysLogHandler(address='/dev/log')
    stdout = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter('MUNIN-GRAPHITE: %(levelname)s %(message)s')
    syslog.setFormatter(formatter)
    if args.logtosyslog:
        logger.addHandler(syslog)
    else:
        logger.addHandler(stdout)

    # block for setting handling of signals
    signal.signal(signal.SIGHUP, handler_hup)
    signal.signal(signal.SIGTERM, handler_term)
    signal.signal(signal.SIGINT, handler_term)

    hosts = list()
    if args.config:
        hosts = read_configuration(args.config)
    if not hosts:
        # no file configuration, trying to use commandline arguments only and construct one-item dictionary
        hosts.append({'host': args.host})
        # we have got some items in hosts's list
    for host in hosts:
        logging.info("Going to thread with config %s" % host)
        threads.append(MuninThread(host, args))

    for t in threads:
        t.start()

    while True:
        try:
            if not any([t.isAlive() for t in threads]):
                logging.info("All threads finished, exiting.")
                break
            else:
                time.sleep(1)
        except KeyboardInterrupt:
            handler_term()

    sys.exit(0)
