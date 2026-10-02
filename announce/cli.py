"""命令行入口：跑公告队列样例。"""

import argparse

from .core import Broadcaster


def build_parser():
    parser = argparse.ArgumentParser(prog="broadcast-queue", description="公告队列")
    parser.add_argument("--sample", default="priority",
                        help="priority / dedup / ttl / rate / cap / scan")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.sample == "priority":
        queue = Broadcaster(rate_limit=10, window_ms=1000, max_queue=10, ttl_ms=100000)
        queue.push("m1", "world", 0, "low", 0)
        queue.push("m2", "world", 5, "high", 1)
        out = queue.poll(1, 5)
        print("first=%s" % (out[0] if out else "none"))
    elif args.sample == "dedup":
        queue = Broadcaster(rate_limit=10, window_ms=1000, max_queue=10, ttl_ms=100000)
        queue.push("m1", "world", 0, "hello", 0)
        queue.push("m1", "world", 0, "hello", 1)
        print("pending=%d" % queue.pending())
    elif args.sample == "ttl":
        queue = Broadcaster(rate_limit=10, window_ms=1000, max_queue=10, ttl_ms=1000)
        queue.push("m1", "world", 0, "hello", 0)
        out = queue.poll(1001, 5)
        print("sent=%d" % len(out))
    elif args.sample == "rate":
        queue = Broadcaster(rate_limit=2, window_ms=1000, max_queue=10, ttl_ms=100000)
        for index in range(5):
            queue.push("m-%d" % index, "world", 0, "t-%d" % index, index)
        print("sent=%d" % len(queue.poll(10, 10)))
    elif args.sample == "cap":
        queue = Broadcaster(rate_limit=10, window_ms=1000, max_queue=3, ttl_ms=100000)
        for index in range(5):
            queue.push("m-%d" % index, "world", index + 1, "t-%d" % index, index)
        print("pending=%d" % queue.pending())
    elif args.sample == "scan":
        queue = Broadcaster(rate_limit=10, window_ms=1000, max_queue=5000, ttl_ms=100000)
        for index in range(3000):
            queue.push("m-%d" % index, "world", 0, "t-%d" % index, index)
        for _ in range(3000):
            queue.purge(0)
        print("scanned=%d" % queue.scanned_count())
    else:
        raise SystemExit("需要 --sample priority|dedup|ttl|rate|cap|scan")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
