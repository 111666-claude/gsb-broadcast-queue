# broadcast-queue

公告队列：优先级、去重、过期、滑动窗口限流与队列上限，只用 Python 标准库。

```
python3 -m announce.cli --sample priority
python3 -m announce.cli --sample dedup
python3 -m announce.cli --sample ttl
python3 -m announce.cli --sample rate
python3 -m announce.cli --sample cap
python3 -m announce.cli --sample scan
python3 -m unittest discover -s tests -v
```

## 口径（README 为准）

- **优先级**：播报顺序按 `priority` 降序、`at_ms` 升序、`msg_id` 字典序升序。
- **去重**：同一个 `(channel, msg_id)` 只入队一次。
- **过期**：入队超过 `ttl_ms` 未播报的公告丢弃；清理按入队顺序从队首进行，
  代价只和过期条数有关。
- **限流**：任意 `window_ms` 内最多播报 `rate_limit` 条，超出部分留在队列里。
- **上限**：队列长度不超过 `max_queue`，超出时丢掉优先级最低的；
  同优先级丢最早入队的。
- **不变量**：一次 `poll` 返回的条数不超过 `limit`，也不超过限流剩余额度；
  同一串入队与播报序列重复执行结果相同。
- **规模**：10 万条公告、每秒 10 万次播报，单次摊还 O(limit + 过期条数)。

## 输出契约（不改格式）

```
first=..
pending=..
sent=..
scanned=..
```

## 目录

```
announce/core.py   优先级、去重、过期、限流与上限
announce/cli.py    命令行入口
tests/             unittest 用例
```
