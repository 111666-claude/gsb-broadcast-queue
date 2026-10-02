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


class PriorityTest(unittest.TestCase):
    def test_higher_priority_broadcasts_first(self):
        queue = Broadcaster(10, 1000, 10, 100000)
        queue.push("m1", "world", 0, "low", 0)
        queue.push("m2", "world", 5, "high", 1)
        self.assertEqual(queue.poll(2, 2), ["high", "low"])

    def test_same_priority_orders_by_at_ms_then_msg_id(self):
        queue = Broadcaster(10, 1000, 10, 100000)
        queue.push("b", "world", 1, "late", 5)
        queue.push("z", "world", 1, "same-time-z", 1)
        queue.push("a", "world", 1, "same-time-a", 1)
        self.assertEqual(queue.poll(10, 3), ["same-time-a", "same-time-z", "late"])


class DedupTest(unittest.TestCase):
    def test_same_channel_and_msg_id_enqueued_once(self):
        queue = Broadcaster(10, 1000, 10, 100000)
        self.assertTrue(queue.push("m1", "world", 0, "hello", 0))
        self.assertFalse(queue.push("m1", "world", 0, "hello", 1))
        self.assertEqual(queue.pending(), 1)

    def test_same_msg_id_in_different_channels_allowed(self):
        queue = Broadcaster(10, 1000, 10, 100000)
        queue.push("m1", "a", 0, "hello", 0)
        queue.push("m1", "b", 0, "hello", 0)
        self.assertEqual(queue.pending(), 2)


class TtlTest(unittest.TestCase):
    def test_expired_message_is_not_broadcast(self):
        queue = Broadcaster(10, 1000, 10, 1000)
        queue.push("m1", "world", 0, "hello", 0)
        self.assertEqual(queue.poll(1001, 5), [])
        self.assertEqual(queue.pending(), 0)

    def test_message_within_ttl_is_broadcast(self):
        queue = Broadcaster(10, 1000, 10, 1000)
        queue.push("m1", "world", 0, "hello", 0)
        self.assertEqual(queue.poll(1000, 5), ["hello"])

    def test_purge_returns_expired_count(self):
        queue = Broadcaster(10, 1000, 10, 1000)
        queue.push("m1", "world", 0, "a", 0)
        queue.push("m2", "world", 0, "b", 500)
        queue.push("m3", "world", 0, "c", 1500)
        self.assertEqual(queue.purge(1600), 2)
        self.assertEqual(queue.pending(), 1)


class RateLimitTest(unittest.TestCase):
    def test_poll_respects_rate_limit(self):
        queue = Broadcaster(2, 1000, 10, 100000)
        for index in range(5):
            queue.push("m-%d" % index, "world", 0, "t-%d" % index, index)
        self.assertEqual(len(queue.poll(10, 10)), 2)
        self.assertEqual(queue.pending(), 3)

    def test_quota_refills_after_window_passes(self):
        queue = Broadcaster(2, 1000, 10, 100000)
        for index in range(4):
            queue.push("m-%d" % index, "world", 0, "t-%d" % index, index)
        self.assertEqual(len(queue.poll(0, 10)), 2)
        self.assertEqual(len(queue.poll(500, 10)), 0)
        self.assertEqual(len(queue.poll(1001, 10)), 2)

    def test_poll_never_exceeds_limit_or_quota(self):
        queue = Broadcaster(3, 1000, 10, 100000)
        for index in range(6):
            queue.push("m-%d" % index, "world", 0, "t-%d" % index, index)
        self.assertEqual(len(queue.poll(0, 2)), 2)
        self.assertEqual(len(queue.poll(1, 10)), 1)
        self.assertEqual(len(queue.poll(2, 10)), 0)


class CapTest(unittest.TestCase):
    def test_queue_never_exceeds_max_queue(self):
        queue = Broadcaster(10, 1000, 3, 100000)
        for index in range(5):
            queue.push("m-%d" % index, "world", index + 1, "t-%d" % index, index)
        self.assertEqual(queue.pending(), 3)

    def test_eviction_drops_lowest_priority(self):
        queue = Broadcaster(10, 1000, 2, 100000)
        queue.push("low", "world", 0, "low", 0)
        queue.push("high", "world", 9, "high", 1)
        queue.push("mid", "world", 5, "mid", 2)
        self.assertEqual(queue.poll(10, 2), ["high", "mid"])

    def test_same_priority_evicts_earliest_enqueued(self):
        queue = Broadcaster(10, 1000, 2, 100000)
        queue.push("first", "world", 1, "first", 0)
        queue.push("second", "world", 1, "second", 1)
        queue.push("third", "world", 1, "third", 2)
        self.assertEqual(queue.poll(10, 2), ["second", "third"])

    def test_new_low_priority_message_is_dropped_immediately(self):
        queue = Broadcaster(10, 1000, 1, 100000)
        queue.push("high", "world", 9, "high", 0)
        queue.push("low", "world", 0, "low", 1)
        self.assertEqual(queue.poll(10, 1), ["high"])


class InvariantTest(unittest.TestCase):
    def test_repeated_sequence_is_deterministic(self):
        def run():
            queue = Broadcaster(3, 100, 8, 500)
            out = []
            for index in range(20):
                queue.push("m-%d" % index, "world", index % 4, "t-%d" % index, index * 10)
            for tick in range(0, 400, 50):
                out.append(queue.poll(tick, 2))
            return out

        self.assertEqual(run(), run())


if __name__ == "__main__":
    unittest.main()
