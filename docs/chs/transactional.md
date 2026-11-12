# 事务化页面处理（Transactional Crawling）

## 背景

1.0.4 及之前的版本把“下载页面、发现链接、链接入队、写入已爬标记”放在同一个
`try` 块中顺序执行。这带来三个问题：

1. `discoverUrl` 等钩子一旦把链接 `add()` 进共享队列，其他 worker 立刻可见；
   若随后的钩子抛异常，当前页会被重抓，但已发现的链接已经泄漏，造成半批数据。
2. 重抓过程中 `queued()` 完成标记可能缺失，页面可能被重复抓取。
3. 异常处理与重抓逻辑耦合在默认异常处理器里，无法表达“按配置重试、耗尽后落终态”。

## 事务模型

每个页面的处理现在是一个可提交的事务（unit of work）：

```
begin(page)          打开私有暂存批次，并把页面登记为 in-flight
  download / hooks   钩子内的 queue()->add() 只进入暂存区，对其他 worker 不可见
commit()             一次性原子发布：暂存的新 URL + 当前页完成标记
                     任何异常 -> rollback()：丢弃暂存 URL，仅把当前页重新入队
```

- **提交点唯一**：只有 `commit()` 会修改共享队列，且在内存队列侧是单次
  GlobalData 原子请求、在 Redis 侧是单个 pipeline。
- **异常回滚**：`downloadPage`、`afterDownloadPage`、`discoverUrl`、
  `afterDiscover` 任意阶段抛异常，未提交的发现结果全部丢弃，只重试当前页。
- **受控跳过**：`error()` 抛出的 `BeanbunException` 属于显式跳过，不重试、
  不发布暂存结果，按 `reserve` 决定是否保留完成标记。
- **重试与终态**：硬异常按 `Beanbun::$retry` 重试（默认 3，即最多 4 次尝试，
  设为 0 表示首次失败即落终态）。重试耗尽后页面写入终态失败集合，永不再出队。
- **崩溃恢复**：`begin()` 登记的 in-flight 页面在 worker 重启时由
  `recover()` 重新入队，`resume` 不丢失已出队未提交的页面。

## 队列契约

新增 `Beanbun\Queue\TransactionalQueueInterface`（继承原 `QueueInterface`）：

| 方法 | 作用 |
| --- | --- |
| `begin(array $queue)` | 打开暂存批次，登记 in-flight |
| `commit($markQueued = true)` | 原子发布暂存 URL 与完成标记 |
| `rollback()` | 丢弃暂存结果，当前页重新入队 |
| `finish($markQueued = false)` | 受控跳过：不重排队、不发布暂存 |
| `failureCount()` / `incrementFailures()` | 重试计数 |
| `markFailed()` | 写入终态失败集合 |
| `recover()` | 恢复上次运行残留的 in-flight 页面 |

内置的 `MemoryQueue` 与 `RedisQueue` 均实现该接口。普通（非守护）模式使用进程内
`LocalQueue`，语义与两个共享后端完全一致。

仅实现旧 `QueueInterface` 的自定义队列保持原有单试次行为，无需任何改动即可继续运行。

## 兼容性

- 内存队列、Redis 队列的原有 key、深度/广度（`depth`/`breadth`）、`reserve`
  插队、Bloom 过滤与非 Bloom 集合模式、`max`/`maxQueueSize` 上限均保持不变。
- 新增的记账 key 为 `<name>InFlight`、`<name>Retry`、`<name>Failed`，
  `clean()` 会一并清空；对正在运行的旧服务端重启时 `add()` 幂等，不丢历史状态。
- 守护进程模式下，完成标记改由 `commit()` 写入；旧的 `afterDiscover` 自动
  `queued()` 钩子仅对非事务化的自定义队列保留。
