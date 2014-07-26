from _util import TestCase, Mock

import threading

from m2gpoll.munin import Telnet as TelnetTransport
from m2gpoll.poller import ConcurrentHostPoller, EventQueue, MultiPoolPluginProcessor

class ConcurrentHostPollerTest(TestCase):
    def _make_poller(self):
        pool = Mock(spec=MultiPoolPluginProcessor)
        transports = Mock()
        events = EventQueue(block=False)

        return ConcurrentHostPoller(
            runners=pool,
            transports=transports,
            events=events,)

    class SomeError(Exception):
        pass

    def test_exit_on_poller_crash(self):
        poller = self._make_poller()
        poller.events.put(poller.events.Interval())
        poller.poll = Mock(side_effect=self.SomeError())
        self.assertEquals(False, poller.pool.finish.called)
        self.assertRaises(self.SomeError, poller.run)
        self.assertEquals(True, poller.pool.finish.called)

    def test_pool_crash_covered(self):
        poller = self._make_poller()
        poller.events.put(poller.events.Exit())
        poller.pool.finish.side_effect = self.SomeError()
        self.assertRaises(self.SomeError, poller.run)

    def test_exit_on_event(self):
        poller = self._make_poller()
        poller.events.put(poller.events.Exit())
        self.assertEquals(False, poller.pool.finish.called)
        poller.run()
        self.assertEquals(True, poller.pool.finish.called)

    def test_poll_event(self):
        poller = self._make_poller()
        poller.events.put(poller.events.Interval())
        poller.events.put(poller.events.Exit())
        transport = Mock(spec=TelnetTransport)
        transport.fetch.return_value = ['a', 'b', 'c',]
        poller.transports.get.return_value = transport

        self.assertEquals(False, poller.pool.finish.called)
        poller.run()
        self.assertEquals(True, poller.pool.finish.called)

class MultiPoolPluginProcessorTest(TestCase):
    def _make_pool(self, **kw):
        args = dict(overflow=0, general=1)
        args.update(kw)
        pool = MultiPoolPluginProcessor(**args)
        self.pools.append(pool)
        return pool

    def setUp(self):
        self.pools = []

    def tearDown(self):
        for pool in self.pools:
            try:
                pool.finish()
            except:
                import traceback
                traceback.print_exc()

    def test_values_default_sensibly(self):
        pool = self._make_pool(general=0, overflow=0)
        self.assertEquals(1, pool.general.pool_size)
        self.assertEquals(0, pool.overflow.pool_size)

    def test_general_thread_pool_started(self):
        pool = self._make_pool(general=2, overflow=0)
        pool.start()
        self.assertEquals(2, len(pool.general.threads))
        self.assertEquals(0, len(pool.overflow.threads))
        pool.finish()
        self.assertEquals(False, any(thread.is_alive()
                                     for thread in pool.general.threads))

    def test_threads_are_not_joined_twice(self):
        def make_thread(**kw):
            mock = Mock(spec=threading.Thread)
            mock.is_alive.return_value = True

            def flip_alive(*args, **kw):
                mock.is_alive.return_value = False

            mock.join.side_effect = flip_alive
            return mock

        pool = self._make_pool(general=2, overflow=2,
                               thread_factory=make_thread)
        pool.overflow.timeout = 0
        pool.general.timeout = 0
        pool.start()
        pool.finish()
        self.assertEquals(set([1]), set([thread.join.call_count
                                    for thread in pool.general.threads]))
        pool.finish()
        self.assertEquals(set([1]), set([thread.join.call_count
                                    for thread in pool.general.threads]))

    def test_plugin_executed(self):
        pool = self._make_pool()
        pool.run(EventQueue.Plugin("name"))
        self.assertEquals("name", pool.general.events.get(block=False).name)

    def test_plugin_overflowed(self):
        pool = self._make_pool(general=2, overflow=1)
        pool.general.timeout = 0 # avoid slow test
        pool.run(EventQueue.Plugin("name"))
        pool.run(EventQueue.Plugin("name"))
        pool.run(EventQueue.Plugin("name"))
        self.assertEquals("name", pool.overflow.events.get(block=False).name)
