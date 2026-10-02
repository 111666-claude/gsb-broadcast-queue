"""公告队列：优先级、去重、过期、滑动窗口限流与队列上限。

实现要点：
- 播报顺序用最小堆，键为 (-priority, at_ms, msg_id, seq)。
- 去重用 (channel, msg_id) -> seq 的字典，消息离队列即移除。
- 过期清理按入队顺序从队首进行，每条只碰一次，代价与过期条数成正比。
- 限流用滑动窗口：记录最近 window_ms 内的播报时间戳。
- 上限驱逐用另一个最小堆，键为 (priority, at_ms, seq)，丢最低优先级、
  同优先级丢最早入队的。
- 两个堆都用懒删除：entries 字典是唯一事实来源，堆里过期项弹出时跳过。
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
        self.entries = {}        # seq -> 公告，唯一事实来源
        self.seen = {}           # (channel, msg_id) -> seq，去重
        self.poll_heap = []      # (-priority, at_ms, msg_id, seq)，播报顺序
        self.cap_heap = []       # (priority, at_ms, seq)，上限驱逐顺序
        self.order = deque()     # (at_ms, seq)，入队顺序，用于过期清理
        self.sent_at = deque()   # 最近 window_ms 内的播报时间戳
        self.seq_next = 0
        self.scanned = 0

    def push(self, msg_id, channel, priority, text, at_ms):
        """入队一条公告；重复或立即被上限挤掉时仍可入队，重复返回 False。"""
        key = (channel, msg_id)
        if key in self.seen:
            return False
        self.scanned += 1
        seq = self.seq_next
        self.seq_next += 1
        self.entries[seq] = {"msg_id": msg_id, "channel": channel,
                             "priority": priority, "text": text, "at_ms": at_ms}
        self.seen[key] = seq
        heapq.heappush(self.poll_heap, (-priority, at_ms, msg_id, seq))
        heapq.heappush(self.cap_heap, (priority, at_ms, seq))
        self.order.append((at_ms, seq))
        while len(self.entries) > self.max_queue:
            self._drop_lowest()
        return True

    def purge(self, now_ms):
        """清理过期公告，返回清理数量；按入队顺序从队首扫描。"""
        expired = 0
        while self.order and now_ms - self.order[0][0] > self.ttl_ms:
            _, seq = self.order.popleft()
            self.scanned += 1
            if seq in self.entries:
                self._remove(seq)
                expired += 1
        return expired

    def poll(self, now_ms, limit):
        """取出要播报的公告文本，不超过 limit 与限流剩余额度。"""
        self.purge(now_ms)
        cutoff = now_ms - self.window_ms
        while self.sent_at and self.sent_at[0] <= cutoff:
            self.sent_at.popleft()
        count = min(limit, self.rate_limit - len(self.sent_at))
        out = []
        while count > 0 and self.poll_heap:
            _, _, _, seq = heapq.heappop(self.poll_heap)
            if seq not in self.entries:
                continue
            entry = self.entries[seq]
            self._remove(seq)
            self.sent_at.append(now_ms)
            out.append(entry["text"])
            count -= 1
        return out

    def pending(self):
        return len(self.entries)

    def scanned_count(self):
        return self.scanned

    def _remove(self, seq):
        entry = self.entries.pop(seq)
        del self.seen[(entry["channel"], entry["msg_id"])]

    def _drop_lowest(self):
        while self.cap_heap:
            _, _, seq = heapq.heappop(self.cap_heap)
            if seq in self.entries:
                self._remove(seq)
                return
