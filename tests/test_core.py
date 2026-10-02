import random
import time
import unittest

from announce.core import Broadcaster


def make(rate_limit=10, window_ms=1000, max_queue=10, ttl_ms=100000):
    return Broadcaster(rate_limit, window_ms, max_queue, ttl_ms)


class BasicTest(unittest.TestCase):
    def test_empty_queue_polls_nothing(self):
        self.assertEqual(make().poll(0, 5), [])

    def test_push_then_pending(self):
        queue = make()
        queue.push("m1", "world", 0, "hello", 0)
        self.assertEqual(queue.pending(), 1)

    def test_poll_returns_text(self):
        queue = make()
        queue.push("m1", "world", 0, "hello", 0)
        self.assertEqual(queue.poll(0, 1), ["hello"])

    def test_limits_are_stored(self):
        queue = Broadcaster(2, 1000, 3, 500)
        self.assertEqual((queue.rate_limit, queue.window_ms, queue.max_queue, queue.ttl_ms),
                         (2, 1000, 3, 500))

    def test_scanned_starts_at_zero(self):
        self.assertEqual(make().scanned_count(), 0)


class PriorityTest(unittest.TestCase):
    def test_higher_priority_first_regardless_of_enqueue_order(self):
        queue = make()
        queue.push("low", "c", 0, "low", 0)
        queue.push("high", "c", 5, "high", 1)
        self.assertEqual(queue.poll(2, 10), ["high", "low"])

    def test_same_priority_earlier_at_ms_first(self):
        queue = make()
        queue.push("late", "c", 3, "late", 5)
        queue.push("early", "c", 3, "early", 1)
        self.assertEqual(queue.poll(10, 10), ["early", "late"])

    def test_same_priority_and_at_ms_msg_id_lexicographic(self):
        queue = make()
        queue.push("b2", "c", 3, "B2", 0)
        queue.push("a10", "c", 3, "A10", 0)
        self.assertEqual(queue.poll(0, 10), ["A10", "B2"])

    def test_full_ordering_priority_then_time_then_msg_id(self):
        queue = make()
        queue.push("a", "c", 1, "a", 9)
        queue.push("b", "c", 2, "b", 9)
        queue.push("c", "c", 2, "c", 1)
        queue.push("d", "c", 2, "d", 1)
        self.assertEqual(queue.poll(20, 10), ["c", "d", "b", "a"])


class DedupTest(unittest.TestCase):
    def test_same_channel_and_msg_id_enqueued_once(self):
        queue = make()
        self.assertTrue(queue.push("m1", "world", 0, "first", 0))
        self.assertFalse(queue.push("m1", "world", 9, "second", 1))
        self.assertEqual(queue.pending(), 1)
        self.assertEqual(queue.poll(2, 10), ["first"])

    def test_same_msg_id_different_channel_both_enqueued(self):
        queue = make()
        queue.push("m1", "world", 0, "w", 0)
        queue.push("m1", "china", 0, "c", 0)
        self.assertEqual(queue.pending(), 2)
        self.assertEqual(sorted(queue.poll(1, 10)), ["c", "w"])

    def test_can_re_enqueue_after_sent(self):
        queue = make()
        queue.push("m1", "world", 0, "hello", 0)
        queue.poll(0, 10)
        self.assertTrue(queue.push("m1", "world", 0, "hello-again", 5))
        self.assertEqual(queue.pending(), 1)
        self.assertEqual(queue.poll(6, 10), ["hello-again"])


class TtlTest(unittest.TestCase):
    def test_expired_message_not_sent(self):
        queue = make(ttl_ms=1000)
        queue.push("m1", "world", 9, "hello", 0)
        self.assertEqual(queue.poll(1001, 5), [])
        self.assertEqual(queue.pending(), 0)

    def test_message_age_equal_ttl_is_still_valid(self):
        queue = make(ttl_ms=1000)
        queue.push("m1", "world", 0, "hello", 0)
        self.assertEqual(queue.poll(1000, 5), ["hello"])

    def test_purge_drops_from_head_and_counts_only_expired(self):
        queue = make(ttl_ms=100)
        queue.push("old", "c", 0, "old", 0)
        queue.push("mid", "c", 0, "mid", 50)
        queue.push("new", "c", 0, "new", 100)
        self.assertEqual(queue.purge(120), 1)
        self.assertEqual(queue.pending(), 2)
        self.assertEqual(queue.poll(120, 10), ["mid", "new"])

    def test_purge_returns_zero_when_fresh(self):
        queue = make(max_queue=5000, ttl_ms=100000)
        for index in range(3000):
            queue.push("m-%d" % index, "c", 0, "t", index)
        self.assertEqual(queue.purge(0), 0)
        self.assertEqual(queue.pending(), 3000)


class RateLimitTest(unittest.TestCase):
    def test_poll_never_exceeds_rate_limit(self):
        queue = make(rate_limit=2, window_ms=1000)
        for index in range(5):
            queue.push("m-%d" % index, "c", 0, "t-%d" % index, index)
        self.assertEqual(len(queue.poll(10, 10)), 2)
        self.assertEqual(queue.pending(), 3)

    def test_excess_stays_in_queue_and_sent_later(self):
        queue = make(rate_limit=2, window_ms=1000)
        for index in range(5):
            queue.push("m-%d" % index, "c", 0, "t-%d" % index, index)
        self.assertEqual(len(queue.poll(10, 10)), 2)
        self.assertEqual(queue.poll(10, 10), [])
        self.assertEqual(len(queue.poll(1010, 10)), 2)
        self.assertEqual(len(queue.poll(2010, 10)), 1)
        self.assertEqual(queue.pending(), 0)

    def test_limit_parameter_caps_returned_count(self):
        queue = make(rate_limit=10)
        for index in range(5):
            queue.push("m-%d" % index, "c", 0, "t-%d" % index, index)
        self.assertEqual(len(queue.poll(0, 3)), 3)

    def test_window_boundary_timestamp_exactly_window_old_is_free(self):
        queue = make(rate_limit=1, window_ms=1000)
        queue.push("a", "c", 0, "a", 0)
        queue.poll(0, 10)
        queue.push("b", "c", 0, "b", 1)
        self.assertEqual(queue.poll(999, 10), [])
        self.assertEqual(queue.poll(1000, 10), ["b"])


class CapTest(unittest.TestCase):
    def test_queue_never_exceeds_max_queue(self):
        queue = make(max_queue=3)
        for index in range(5):
            self.assertLessEqual(queue.pending(), 3)
            queue.push("m-%d" % index, "c", index + 1, "t-%d" % index, index)
        self.assertEqual(queue.pending(), 3)

    def test_lowest_priority_is_dropped(self):
        queue = make(max_queue=2)
        queue.push("low", "c", 1, "low", 0)
        queue.push("high", "c", 5, "high", 1)
        queue.push("mid", "c", 3, "mid", 2)
        self.assertEqual(queue.pending(), 2)
        self.assertEqual(queue.poll(3, 10), ["high", "mid"])

    def test_same_priority_earliest_enqueued_is_dropped(self):
        queue = make(max_queue=2)
        queue.push("first", "c", 3, "first", 0)
        queue.push("second", "c", 3, "second", 1)
        queue.push("third", "c", 3, "third", 2)
        self.assertEqual(queue.poll(3, 10), ["second", "third"])

    def test_new_message_can_be_dropped_immediately(self):
        queue = make(max_queue=2)
        queue.push("a", "c", 5, "a", 0)
        queue.push("b", "c", 5, "b", 1)
        queue.push("c", "c", 1, "c", 2)
        self.assertEqual(queue.pending(), 2)
        self.assertEqual(queue.poll(3, 10), ["a", "b"])

    def test_evicted_message_can_be_pushed_again(self):
        queue = make(max_queue=1)
        queue.push("a", "c", 5, "a", 0)
        queue.push("b", "c", 1, "b", 1)
        self.assertTrue(queue.push("b", "c", 9, "b-new", 2))
        self.assertEqual(queue.poll(3, 10), ["b-new"])


class InvariantTest(unittest.TestCase):
    def test_poll_count_bounded_by_limit_and_rate(self):
        queue = make(rate_limit=3, window_ms=1000, max_queue=100)
        for index in range(20):
            queue.push("m-%d" % index, "c", 0, "t-%d" % index, index)
        self.assertEqual(len(queue.poll(0, 2)), 2)
        self.assertEqual(len(queue.poll(0, 10)), 1)
        self.assertEqual(queue.pending(), 17)

    def test_same_sequence_replays_identically(self):
        def replay(seed):
            rng = random.Random(seed)
            queue = make(rate_limit=3, window_ms=10, max_queue=8, ttl_ms=30)
            transcript = []
            now = 0
            for step in range(200):
                now += rng.randrange(0, 3)
                if rng.random() < 0.7:
                    mid = rng.choice(["a", "b", "c", "d", "e"])
                    ch = rng.choice(["x", "y"])
                    pri = rng.randrange(0, 4)
                    accepted = queue.push(mid, ch, pri, "%s/%s@%d" % (ch, mid, now), now)
                    transcript.append(("push", accepted))
                else:
                    out = queue.poll(now, rng.randrange(1, 4))
                    transcript.append(("poll", tuple(out)))
            return transcript
        self.assertEqual(replay(42), replay(42))

    def test_100k_messages_under_three_seconds(self):
        queue = make(rate_limit=10 ** 9, max_queue=10 ** 9, ttl_ms=10 ** 18)
        start = time.perf_counter()
        for index in range(100000):
            queue.push("m-%d" % index, "c", index % 7, "t-%d" % index, index)
        total = 0
        now = 0
        while queue.pending():
            now += 1
            total += len(queue.poll(now, 1000))
        elapsed = time.perf_counter() - start
        self.assertEqual(total, 100000)
        self.assertLess(elapsed, 3.0)

    def test_scan_cost_scales_with_expired_count(self):
        queue = make(max_queue=5000, ttl_ms=100000)
        for index in range(3000):
            queue.push("m-%d" % index, "c", 0, "t-%d" % index, index)
        for _ in range(3000):
            queue.purge(0)
        self.assertLessEqual(queue.scanned_count(), 6000)


if __name__ == "__main__":
    unittest.main()
