import unittest

from announce.core import Broadcaster


class BasicTest(unittest.TestCase):
    def test_empty_queue_polls_nothing(self):
        self.assertEqual(Broadcaster(10, 1000, 10, 100000).poll(0, 5), [])

    def test_push_then_pending(self):
        queue = Broadcaster(10, 1000, 10, 100000)
        queue.push("m1", "world", 0, "hello", 0)
        self.assertEqual(queue.pending(), 1)

    def test_poll_returns_text(self):
        queue = Broadcaster(10, 1000, 10, 100000)
        queue.push("m1", "world", 0, "hello", 0)
        self.assertEqual(queue.poll(0, 1), ["hello"])

    def test_limits_are_stored(self):
        queue = Broadcaster(2, 1000, 3, 500)
        self.assertEqual((queue.rate_limit, queue.window_ms, queue.max_queue, queue.ttl_ms),
                         (2, 1000, 3, 500))

    def test_scanned_starts_at_zero(self):
        self.assertEqual(Broadcaster(10, 1000, 10, 100000).scanned_count(), 0)


if __name__ == "__main__":
    unittest.main()
