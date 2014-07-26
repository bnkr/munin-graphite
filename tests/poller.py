from _util import TestCase, Mock

from m2gpoll.munin import Telnet as TelnetTransport
from m2gpoll.poller import ConcurrentHostPoller, EventQueue

class ConcurrentHostPollerTest(TestCase):
    def _make_poller(self):
        pool = Mock()
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
        self.assertEquals(False, poller.pool.exit_all.called)
        self.assertRaises(self.SomeError, poller.run)
        self.assertEquals(True, poller.pool.exit_all.called)

    def test_pool_crash_covered(self):
        poller = self._make_poller()
        poller.events.put(poller.events.Exit())
        poller.pool.exit_all.side_effect = self.SomeError()
        self.assertRaises(self.SomeError, poller.run)

    def test_exit_on_event(self):
        poller = self._make_poller()
        poller.events.put(poller.events.Exit())
        self.assertEquals(False, poller.pool.exit_all.called)
        poller.run()
        self.assertEquals(True, poller.pool.exit_all.called)

    def test_poll_event(self):
        poller = self._make_poller()
        poller.events.put(poller.events.Interval())
        poller.events.put(poller.events.Exit())
        transport = Mock(spec=TelnetTransport)
        transport.fetch.return_value = ['a', 'b', 'c',]
        poller.transports.get.return_value = transport

        self.assertEquals(False, poller.pool.exit_all.called)
        poller.run()
        self.assertEquals(True, poller.pool.exit_all.called)
