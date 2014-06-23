#!/usr/bin/python
"""Gather Munin statistics and deliver to Carbon for Graphite display."""

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
shutdown = False


class Munin():
    """Munin host object with querying getter functions."""

    def __init__(self, hostname, thread, port=4949, args=None):

        self.hostname = None
        self.remotenode = None
        self._sock = None
        self._conn = None
        self._carbon_sock = None
        self.hello_string = None
        self.reload_plugins = True
        self.plugins = {}
        self.plugins_config = {}

        if ':' in hostname:
            self.hostname, self.remotenode = hostname.split(":", 1)
        else:
            self.hostname = hostname
        self.port = port
        self.args = args

        if self.args.displayname:
            self.displayname = self.args.displayname.split(".")[0]
        else:
            self.displayname = self.hostname.split(".")[0]
        self.thread = thread

    def go(self):
        """Bootstrap method to start processing hosts's Munin stats."""
        global shutdown
        self.connect()
        self.update_hostname()
        processing_time = self.process_host_stats()
        interval = int(self.args.interval)

        while True and interval != 0 and not shutdown:
            sleep_time = max(interval - processing_time, 0)
            time.sleep(sleep_time)
            self.connect()
            processing_time = self.process_host_stats()

    def update_hostname(self):
        """Updating hostname from connection hello string."""
        if self.args.displayname:
            return
        try:
            node_name = RE_MUNIN_NODE_NAME.search(self.hello_string).group(1)
            self.displayname = node_name.split(".")[0]
        except AttributeError:
            logger.info("Thread %s: Unable to obtain munin node name from: %s",
                        self.thread.name, self.hello_string)
            return

    def connect(self):
        """Initial connection to Munin host."""
        try:
            self._sock = socket.create_connection((self.hostname, self.port), 10)
        except socket.error:
            logger.exception("Thread %s: Unable to connect to Munin host %s, port: %s",
                             self.thread.name, self.hostname, self.port)
            sys.exit(1)

        try:
            self._conn = self._sock.makefile()
            self.hello_string = self._readline()
        except socket.error:
            logger.exception("Thread %s: Unable to communicate to Munin host %s, port: %s",
                             self.thread.name, self.hostname, self.port)

        if self.args.carbon:
            self.connect_carbon()

    def connect_carbon(self):
        carbon_host, carbon_port = self.args.carbon.split(":")
        try:
            self._carbon_sock = socket.create_connection((carbon_host, carbon_port), 10)
        except socket.error:
            logger.exception("Thread %s: Unable to connect to Carbon on host %s, port: %s",
                             self.thread.name, carbon_host, carbon_port)
            sys.exit(1)

    def close_connection(self):
        """Close connection to Munin host."""
        self._sock.close()

    def close_carbon_connection(self):
        """Close connection to Carbon host."""
        if self._carbon_sock:
            self._carbon_sock.close()

    def _readline(self):
        """Read one line from Munin output, stripping leading/trailing chars."""
        return self._conn.readline().strip()

    def _iterline(self):
        """Iterator over Munin output."""
        while True:
            current_line = self._readline()
            logger.debug("Thread %s: Iterating over line: %s", self.thread.name, current_line)
            if not current_line:
                break
            if current_line.startswith("#"):
                continue
            if current_line == ".":
                break
            yield current_line

    def fetch(self, plugin):
        """Fetch plugin's data fields from Munin."""
        self._sock.sendall("fetch %s\n" % plugin)
        response = {None: {}}
        multigraph = None
        multigraph_prefix = ""
        for current_line in self._iterline():
            if current_line.startswith("multigraph "):
                multigraph = current_line[11:]
                multigraph_prefix = multigraph.rstrip(".") + "."
                response[multigraph] = {}
                continue
                # Some munin plugins have more than one space between key and value.
            try:
                full_key_name, key_value = RE_LEFTRIGHT.search(current_line).group(1, 2)
                key_name = multigraph_prefix + full_key_name.split(".")[0]
                response[multigraph][key_name] = key_value
            except (KeyError, AttributeError):
                logger.info("Thread %s: Plugin %s returned invalid data [%s] for host"
                            " %s\n", self.thread.name, plugin, current_line, self.hostname)

        return response

    def list_plugins(self):
        """Return a list of Munin plugins configured on a node. """
        self._sock.sendall("cap multigraph\n")
        self._readline()  # ignore response

        if self.remotenode:
            logger.info("Thread %s: Asking for plugin list for remote node %s", self.thread.name, self.remotenode)
            self._sock.sendall("list %s\n" % self.remotenode)
        else:
            logger.info("Thread %s: Asking for plugin list for local node %s", self.thread.name, self.hostname)
            self._sock.sendall("list\n")

        plugin_list = self._readline().split(" ")
        if self.args.filter:
            try:
                filteredlist = [plugin for plugin in plugin_list if re.search(self.args.filter, plugin, re.IGNORECASE)]
                plugin_list = filteredlist
            except re.error:
                logger.info("Thread %s: Filter regexp for plugin list is not valid: %s" % self.args.filter)
            # if there is no filter or we have got an re.error, simply return full list
        result_list = []
        for plugin in plugin_list:
            if len(plugin.strip()) > 0:
                result_list.append(plugin)
        return result_list

    def get_config(self, plugin):
        """Get config values for Munin plugin."""
        self._sock.sendall("config %s\n" % plugin)
        response = {None: {}}
        multigraph = None

        for current_line in self._iterline():
            if current_line.startswith("multigraph "):
                multigraph = current_line[11:]
                response[multigraph] = {}
                continue

            try:
                key_name, key_value = current_line.split(" ", 1)
            except ValueError:
                # ignore broken plugins that don't return a value at all
                continue

            if "." in key_name:
                # Some keys have periods in them.
                # If so, make their own nested dictionary.
                key_root, key_leaf = key_name.split(".", 1)
                if key_root not in response:
                    response[multigraph][key_root] = {}
                response[multigraph][key_root][key_leaf] = key_value
            else:
                response[multigraph][key_name] = key_value

        return response

    def process_host_stats(self):
        """Process Munin node data, potentially sending to Carbon."""
        start_timestamp = time.time()
        logger.info("Thread %s: Querying host %s", self.thread.name, self.hostname)
        # to be more efficient, load list of plugins just in case we do not have any
        if self.reload_plugins:
            self.plugins_config = {}
            self.plugins = self.list_plugins()
            self.reload_plugins = False
            logger.debug("Thread %s: Plugin List: %s", self.thread.name, self.plugins)
        epoch_timestamp = int(start_timestamp)

        for current_plugin in self.plugins:
            logger.info("Thread %s: Fetching plugin: %s (Host: %s)",
                        self.thread.name, current_plugin, self.hostname)

            # after (re)load of list of plugins we have to load their configurations too
            try:
                self.plugins_config[current_plugin]
            except KeyError:
                self.plugins_config[current_plugin] = self.get_config(current_plugin)
                logger.debug("Thread %s: Plugin Config: %s", self.thread.name, self.plugins_config[current_plugin])

            plugin_data = self.fetch(current_plugin)
            logger.debug("Thread %s: Plugin Data: %s", self.thread.name, plugin_data)
            if self.args.carbon:
                for multigraph in self.plugins_config[current_plugin]:
                    try:
                        self.send_to_carbon(epoch_timestamp,
                                            current_plugin,
                                            self.plugins_config[current_plugin][multigraph],
                                            plugin_data[multigraph])
                    except KeyError:
                        logger.info("Thread %s: Plugin returns invalid data:\n plugin_config: %r host %s.",
                                    self.thread.name, self.plugins_config[current_plugin], self.hostname)
        end_timestamp = time.time() - start_timestamp
        self.close_connection()
        self.close_carbon_connection()
        logger.info("Thread %s: Finished querying host %s (Execution Time: %.2f sec).",
                    self.thread.name, self.hostname, end_timestamp)
        return end_timestamp

    def send_to_carbon(self, timestamp, plugin_name, plugin_config, plugin_data):
        """Send plugin data to Carbon over Pickle format."""
        if self.args.noprefix:
            prefix = ''
        else:
            prefix = "%s." % self.args.prefix

        hostname = self.hostname
        if self.remotenode:
            hostname = self.remotenode

        data_list = []
        logger.info("Creating metric for plugin %s, timestamp: %d",
                    plugin_name, timestamp)

        for data_key in plugin_data:
            try:
                plugin_category = plugin_config["graph_category"]
                metric = "%s%s.%s.%s.%s" % (prefix, self.displayname, plugin_category, plugin_name, data_key)
                value = plugin_data[data_key]
                logger.debug("Creating metric %s, value: %s", metric, value)
                data_list.append((metric, (timestamp, value)))
            except KeyError:
                logger.info("plugin returns invalid data:\n plugin_config: %r host %s.", plugin_config, self.hostname)

        if self.args.noop:
            logger.info("NOOP: Not sending data to Carbon")
            return

        logger.info("Sending plugin %s data to Carbon for host %s.",
                    plugin_name, hostname)
        payload = pickle.dumps(data_list)
        header = struct.pack("!L", len(payload))
        message = header + payload
        try:
            self._carbon_sock.sendall(message)
            logger.info("Finished sending plugin %s data to Carbon for host %s.",
                        plugin_name, self.hostname)
        except socket.error:
            logger.exception("Unable to send data to Carbon")


###
# Custom Threading class, one thread for each host in configuration
###
class MuninThread(threading.Thread):
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

        self.munin = Munin(hostname=self.name, args=cfg, thread=self)

    def run(self):
        logger.info("Starting thread for %s." % self.name)
        self.munin.go()
        logger.info("Finishing thread for %s." % self.name)

    def dostop(self):
        global shutdown
        logger.info("Thread %s: Got signal to stop." % self.name)
        shutdown = True

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


def main():
    global threads
    global logger

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
    stdout = logging.StreamHandler(sys.stdout)
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


if __name__ == '__main__':
    main()
