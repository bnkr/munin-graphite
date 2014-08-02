"""
Parts of the polling algoritm.  Dealing with concurrency and so on.
"""
from six.moves.queue import Queue
from six.moves import queue
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

class ThreadAdapter(logging.LoggerAdapter):
    """Adds information about the current running thread to log messages."""
    def process(self, msg, kwargs):
        thread = threading.current_thread()
        return u"Thread {0}: {1}".format(thread, msg), kwargs

class ConcurrentHostPoller():
    """Polls a single host using a pool of runners.  This is intended to be run
    in its own thread and be sent events down a queue.  Usually each host would
    have its own pool of plugin runners because you don't want one host blocking
    another."""

    # TODO:
    #   Naming a bit iffy.  This is technically not concurrent unless you give
    #   it a concurrent pool.

    def __init__(self, events, runners, transports):
        self.events = events
        self.pool = runners
        self.transports = transports
        self.logger = ThreadAdapter(logging.getLogger(__name__), {})

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
                #   different interval tho).  Might need some kind of finish
                #   run event.  We might need special handing when we don't have
                #   threads if we are to hendle running the interval at the
                #   right time.
                self.poll()
        except:
            self.logger.exception(u"poller exception")
            raise
        finally:
            try:
                # Possibly the calling process might want to do more to try and
                # exit in order to avoid failure to terminate.
                self.pool.finish()
            except:
                self.logger.exception(
                    u"exception when closing runner pool forever now ({0} "
                    u"threads still running)".format(threading.active_count()))
                raise

    def poll(self):
        """Poll for plugins and dispatch jobs to process each one."""
        self.logger.info(u"begining polling run")
        self.pool.start()
        transport = self.transports.get(thread_id=1)

        for plugin in transport.list_plugins():
            status = self.pool.run(EventQueue.Plugin(plugin))
            if status == "full":
                # TODO:
                #   Check that the threads didn't crash (or maybe just do that
                #   in the pool/).
                self.logger.warning("no space for plugin {0}".format(plugin))
            else:
                self.logger.debug("plugin {0} run: {1}".format(plugin, status))

class PluginRunner(object):
    """Runs plugins and sends their metrics to be processed."""

    def __init__(self, events, output):
        self.events = events
        self.output = output
        self.logger = ThreadAdapter(logging.getLogger(__name__), {})

    def run(self):
        while True:
            event = self.events.get()
            if event.is_exit():
                return

            self.logger.debug(u"process plugin {0}".format(event.name))
            # TODO: process the plugin now

        self.logger.info("finish runner")

class LimitedPool(object):
    """A thread pool of a particular size with a limited pipe to read from."""

    def __init__(self, name, pool_size, queue_size, logger, thread):
        self.pool_size = pool_size
        self.queue_size = queue_size
        self.events = Queue(maxsize=queue_size)
        self.threads = []
        self.name = name
        self.logger = logger
        self.thread_factory = thread
        self.started = False

    def create(self):
        if self.threads:
            return

        self.logger.info(
                u"creating {0} threads for {1}".format(self.pool_size, self.name))

        runners = [PluginRunner(events=self.events, output=None)
                   for _ in range(self.pool_size)]
        self.threads = [
                self.thread_factory(
                    name="{0} #{1}/{2}".format(
                        self.name, num + 1, self.pool_size),
                    target=lambda: runner.run(),)
                for num, runner in enumerate(runners)]

    def start(self):
        if self.started:
            return

        self.logger.info("starting {0} threads".format(self.name))
        [thread.start() for thread in self.threads]
        self.started = True

    def put(self, event):
        self.events.put(event, timeout=self.timeout)

    def spam_exit(self):
        for _ in self.threads:
            try:
                self.events.put(EventQueue.Exit(), block=False)
            except queue.Full:
                pass

    def wait_for_exit(self):
        # TODO:
        #   Branches not well covered.

        live_threads = [thread for thread in self.threads
                        if thread.is_alive()]
        while live_threads:
            still_alive = []

            for thread in live_threads:
                try:
                    self.events.put(EventQueue.Exit(), block=False)
                except queue.Full:
                    pass

                self.logger.info("wait for {0}".format(thread))
                thread.join(timeout=5)

                # Quite often the threads won't exit, even if you wait a long
                # time, so repeatedly spamming the event seems to be a good
                # idea.
                if thread.is_alive():
                    still_alive.append(thread)
                    self.logger.info("{0} did not exit".format(thread))
                else:
                    self.logger.info("{0} exitted".format(thread))

            live_threads = still_alive

class SynchronusPluginProcessor():
    """Plugin processing without any threads."""

class MultiPoolPluginProcessor():
    """
    Deals with a pool of plugin runners and the queues to send jobs to them.

    This would normally be used in a pool per host.  Its main value is that we
    can have an "overflow" pool for using when a plugin takes too long, meaning
    you can keep parallelism down but still don't lose the entire set of metrics
    for a host just because of one problem plugin.

    Might be better with multiprocessing . thread . ThreadPool or
    ThreadPoolExecutor (from py3 or library).  Certainly this will deal with
    dead threads better.
    """

    def __init__(self, general, overflow, thread_factory=None):
        self.logger = ThreadAdapter(logging.getLogger(__name__), {})

        thread_factory = thread_factory or threading.Thread
        self.general = LimitedPool(pool_size=general or 1, name="general",
                                   logger=self.logger, thread=thread_factory,
                                   queue_size=general or 1)
        self.overflow = LimitedPool(pool_size=overflow or 0, name="overflow",
                                    logger=self.logger, thread=thread_factory,
                                    queue_size=overflow or 1)

        self.overflow.timeout = 5
        self.general.timeout = 2

        self.finished = False
        self.started = False

    def start(self):
        """Can be called multiple times."""
        if self.started:
            return

        self.general.create()
        self.overflow.create()

        self.general.start()
        self.overflow.start()

        self.started = True

    def reset(self):
        """Hax for testing."""
        self.started = False
        self.finished = False

        self.general.threads = []
        self.overflow.threads = []
        self.general.started = False
        self.overflow.started = False

    def finish(self):
        """Finish with this pool.  Try our best to make sure all the threads
        close."""

        # Avoid filling up the queue if we already tried this one time.
        if not self.finished:
            self.logger.info("spam exit event")
            self.general.spam_exit()
            self.overflow.spam_exit()

        self.finished = True
        self.general.wait_for_exit()
        self.overflow.wait_for_exit()

    def run(self, plugin):
        """Run the plugin using the general or overflow pools."""
        self.logger.debug("put {0}".format(plugin.name))
        try:
            self.general.put(plugin)
            return "queued"
        except queue.Full:
            try:
                self.overflow.put(plugin)
                return "overflowed"
            except queue.Full:
                return "full"
