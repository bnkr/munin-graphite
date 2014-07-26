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

global logger
logger = logging.getLogger()

class Telnet():
    """Query munin data over telnet.  This is the interface to the munin-node
    daemon."""

    shutdown = False
    """We'll edit this by reference temporarily."""

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
        self.connect()
        self.update_hostname()
        processing_time = self.process_host_stats()
        interval = int(self.args.interval)

        while True and interval != 0 and not self.shutdown:
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

    def close_connection(self):
        """Close connection to Munin host."""
        self._sock.close()

    def connect_carbon(self):
        carbon_host, carbon_port = self.args.carbon.split(":")
        try:
            self._carbon_sock = socket.create_connection((carbon_host, carbon_port), 10)
        except socket.error:
            logger.exception("Thread %s: Unable to connect to Carbon on host %s, port: %s",
                             self.thread.name, carbon_host, carbon_port)
            sys.exit(1)

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
