"""公告队列：优先级、去重、过期、滑动窗口限流与队列上限。

缺陷：出队按先进先出、不去重、不过期、不限流、队列上限不生效、
清理过期时扫描整个队列。
"""


class Broadcaster:
    """公告广播队列。"""

    def __init__(self, rate_limit, window_ms, max_queue, ttl_ms):
        self.rate_limit = rate_limit
        self.window_ms = window_ms
        self.max_queue = max_queue
        self.ttl_ms = ttl_ms
        self.queue = []
        self.seen = set()
        self.sent_at = []
        self.scanned = 0

    def push(self, msg_id, channel, priority, text, at_ms):
        """入队一条公告。"""
        self.scanned += len(self.queue)
        self.seen.add((channel, msg_id))
        self.queue.append({"msg_id": msg_id, "channel": channel, "priority": priority,
                           "text": text, "at_ms": at_ms})
        return True

    def purge(self, now_ms):
        """清理过期公告，返回清理数量。"""
        self.scanned += len(self.queue)
        return 0

    def poll(self, now_ms, limit):
        """取出要播报的公告文本。"""
        self.scanned += len(self.queue)
        chosen = self.queue[:limit]
        self.queue = self.queue[limit:]
        out = []
        for msg in chosen:
            self.sent_at.append(now_ms)
            out.append(msg["text"])
        return out

    def pending(self):
        return len(self.queue)

    def scanned_count(self):
        return self.scanned
