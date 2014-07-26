"""
Parts of the polling algoritm.  Dealing with concurrency and so on.
"""
from six.moves.queue import Queue
import threading
import logging

class EventQueue(object):
    """Event queue abstraction.

    (This class might be unnecessary in the long run but we could do some
    helpful stuff with timeouts and processing common messages)"""

    class Exit(object):
        """Don't process any more work."""

        def is_exit(self):
            return True

    class Interval(object):
        """Interval has been recched; do some things."""

        def is_exit(self):
            return False

    class Plugin(object):
        """Details of a plugin retrieved from a fetch operation."""

        def __init__(self, name):
            self.name = name

        def is_exit(self):
            return False

    def __init__(self, block=True):
        """Block is useful in tests to stop deadlocks."""
        self.queue = Queue()
        self.block = block

    def put(self, event):
        """Blocks forever if the queue is full."""
        self.queue.put(event, block=self.block)

    def get(self):
        """Wait forever or until something is on the queue.  This tends to mean
        that we crash horribly if the event producers stop working.  Ho hum."""
        return self.queue.get(block=self.block)

class ConcurrentHostPoller():
    """Polls a single host using a pool of runners.  This is intended to be run
    in its own thread and be sent events down a queue.  Usually each host would
    have its own pool of plugin runners because you don't want one host blocking
    another."""

    def __init__(self, events, runners, transports):
        self.events = events
        self.pool = runners
        self.transports = transports
        self.logger = logging.getLogger(__name__)

    def run(self):
        """Wait for events and handle errors.  It's important that errors are
        handled well here to avoid hanging the process when there are left-over
        threads on exit."""
        try:
            while True:
                event = self.events.get()
                if event.is_exit():
                    self.logger.info(u"exit event recieved")
                    return

                # TODO:
                #   Need a config reload singal.
                #
                #   Deal with overlapping events (some plugins might be on a
                #   different interval tho).
                self.poll()
        except:
            self.logger.exception(u"poller exception")
            raise
        finally:
            try:
                # It's sort of apropriate to exit here even though we didn't
                # create the object because we shoudln't try and kill the pool
                # until the host poller has finished using them.  Possibly the
                # calling process might want to do more in order to avoid
                # failure to terminate.
                self.pool.exit_all()
            except:
                self.logger.exception(
                    u"exception when closing runner pool forever now ({0} "
                    u"threads still running)".format(threading.active_count()))
                # Bad things if we were already dealing with an exception I
                # guess...
                raise

    def poll(self):
        """Poll for plugins and dispatch jobs to process each one."""
        self.pool.start_threads()
        transport = self.transports.get(thread_id=1)

        for plugin in transport.fetch():
            # TODO:
            #   Needs transport and config sent to plugin executor.

            # Sending an event rather than saying which function to run gives us
            # a bit more flexibility in how to implement the pool.
            self.logger.debug(u"push {0}".format(plugin))
            self.pool.push_plugin(EventQueue.Plugin(plugin))

            # TODO:
            #   needs some kind of logging at least if it hit the emergency pool
            #   etc.
