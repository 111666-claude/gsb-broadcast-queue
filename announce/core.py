"""公告队列：优先级、去重、过期、滑动窗口限流与队列上限。

实现要点：
- 播报顺序用堆维护 (-priority, at_ms, msg_id, seq)，弹出的即是最高优先级。
- 去重用 (channel, msg_id) -> 公告 的字典，只保留仍在队列中的公告。
- 过期清理按入队顺序从队首进行，遇到第一条未过期的公告即停。
- 限流用滑动窗口：记录每次播报时间，任意 window_ms 内最多 rate_limit 条。
- 队列上限用 (priority, seq) 小根堆，超出时丢优先级最低、最早入队的。
"""

import heapq
from collections import deque


class Broadcaster:
    """公告广播队列。"""

    def __init__(self, rate_limit, window_ms, max_queue, ttl_ms):
        self.rate_limit = rate_limit
        self.window_ms = window_ms
        self.max_queue = max_queue
        self.ttl_ms = ttl_ms
        self._seq = 0
        self._by_priority = []
        self._by_evict = []
        self._by_time = deque()
        self._pending = {}
        self._sent_at = deque()
        self._scanned = 0

    def push(self, msg_id, channel, priority, text, at_ms):
        """入队一条公告；同 (channel, msg_id) 重复入队时拒绝并返回 False。"""
        key = (channel, msg_id)
        if key in self._pending:
            return False
        self._scanned += 1
        entry = {"msg_id": msg_id, "channel": channel, "priority": priority,
                 "text": text, "at_ms": at_ms, "seq": self._seq, "alive": True}
        self._seq += 1
        self._pending[key] = entry
        heapq.heappush(self._by_priority, (-priority, at_ms, msg_id, entry["seq"], entry))
        heapq.heappush(self._by_evict, (priority, entry["seq"], entry))
        self._by_time.append(entry)
        while len(self._pending) > self.max_queue:
            self._evict_lowest()
        return True

    def purge(self, now_ms):
        """清理过期公告，返回清理数量；按入队顺序从队首扫描，遇到未过期即停。"""
        removed = 0
        while self._by_time:
            entry = self._by_time[0]
            self._scanned += 1
            if not entry["alive"]:
                self._by_time.popleft()
                continue
            if now_ms - entry["at_ms"] <= self.ttl_ms:
                break
            self._by_time.popleft()
            self._drop(entry)
            removed += 1
        return removed

    def poll(self, now_ms, limit):
        """取出要播报的公告文本，条数不超过 limit 与限流剩余额度。"""
        self.purge(now_ms)
        self._trim_window(now_ms)
        count = min(limit, self.rate_limit - len(self._sent_at))
        out = []
        while count > 0:
            entry = self._next_alive()
            if entry is None:
                break
            self._drop(entry)
            self._sent_at.append(now_ms)
            out.append(entry["text"])
            count -= 1
        return out

    def pending(self):
        return len(self._pending)

    def scanned_count(self):
        return self._scanned

    def _drop(self, entry):
        entry["alive"] = False
        del self._pending[(entry["channel"], entry["msg_id"])]

    def _evict_lowest(self):
        while self._by_evict:
            _, _, entry = heapq.heappop(self._by_evict)
            self._scanned += 1
            if entry["alive"]:
                self._drop(entry)
                return

    def _next_alive(self):
        while self._by_priority:
            _, _, _, _, entry = heapq.heappop(self._by_priority)
            self._scanned += 1
            if entry["alive"]:
                return entry
        return None

    def _trim_window(self, now_ms):
        threshold = now_ms - self.window_ms
        while self._sent_at and self._sent_at[0] <= threshold:
            self._sent_at.popleft()
