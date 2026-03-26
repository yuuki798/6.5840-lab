# Raft 3B 调试踩坑总结

这份文档总结了前面实现 6.5840 `raft1` 的 3B（log replication）时踩过的主要坑，按 **现象 -> 根因 -> 修复建议** 组织。

---

## 1. `Start()` 返回 index 错误

### 现象
- `TestBasicAgree3B` 中 index 对不上（比如返回旧 index）。

### 根因
- 在 `append` 前计算 index（`len(log)-1`）而不是 append 后。

### 修复建议
- `Start()` 中先 append，再 `index := len(rf.log)-1`。
- 非 leader 要立刻返回 `(-1, term, false)`（或框架约定值）。

---

## 2. 只做了心跳，没有真正复制日志

### 现象
- 只有 3A 心跳能过，3B 的 agreement 测试失败。

### 根因
- `AppendEntries` 参数只带 `Term/LeaderId`，没带 `PrevLogIndex/PrevLogTerm/Entries/LeaderCommit`。

### 修复建议
- 完整实现 Figure 2 的 `AppendEntries` 请求/回复结构。
- leader 发送时按每个 follower 的 `nextIndex[i]` 切日志。

---

## 3. `RequestVote` 没做日志新旧比较（5.4.1）

### 现象
- 分区后旧日志 candidate 当选，后续日志冲突、反复翻车。

### 根因
- 投票时只看 term/votedFor，忽略 lastLogTerm/lastLogIndex。

### 修复建议
- 严格按论文比较：
  - 先比 `LastLogTerm`；
  - term 相同再比 `LastLogIndex`。

---

## 4. follower 端 AppendEntries 冲突处理不完整

### 现象
- 日志不一致时不能正确回退覆盖；`Rejoin/Backup` 类测试失败。

### 根因
- 只判断成功/失败，没有把“冲突位置”和“冲突 term”信息返回给 leader。

### 修复建议
- 在 reply 中增加冲突提示（常见做法）：
  - `ConflictTerm`
  - `ConflictIndex`
- follower：
  - 日志太短：`ConflictTerm=-1, ConflictIndex=len(log)`
  - term 冲突：返回冲突 term 及该 term 首位置。

---

## 5. leader 回退 `nextIndex` 太慢

### 现象
- `TestBackup3B` 卡慢或超时。

### 根因
- 失败后仅 `nextIndex--`，遇到 50+ 错误日志时需要很多轮 RPC。

### 修复建议
- 用 conflict optimization 快速跳转：
  - leader 若有 `ConflictTerm`，跳到该 term 最后位置+1；
  - 否则跳到 `ConflictIndex`。

---

## 6. `checkCommit()` 调用时机错误

### 现象
- leader 明明复制成功，但 commit 推不动或时好时坏。

### 根因
- 在发送 RPC 前调用 commit 检查；当时 `matchIndex` 还没更新。

### 修复建议
- 只在 **AppendEntries 成功回调并更新 matchIndex 后** 执行 `checkCommit()`。
- 并确保只提交当前任期日志（`log[N].Term == currentTerm`）。

---

## 7. 选举协程阻塞（`wg.Wait()` 卡住）

### 现象
- leader 挂掉后新 leader 迟迟不稳定，`LeaderFailure3B` 容易超时。

### 根因
- 选举逻辑等待所有投票 RPC 返回；断网节点会拖慢流程。

### 修复建议
- 选举改异步，不等待所有节点。
- 票数达到多数就立即转 leader。

---

## 8. 心跳频率与 RPC 数量的平衡

### 现象
- 心跳太慢：故障恢复/提交慢；
- 心跳太快：`TestCount3B` 报 RPC 过多。

### 根因
- ticker 频率和心跳周期没有分层处理。

### 修复建议
- 采用“小步轮询 + 固定心跳周期”：
  - 例如 ticker 每 10ms 检查状态；
  - leader 每 ~100ms 发一次心跳；
  - 有新日志时立即触发一轮复制（不要等下个心跳周期）。

---

## 9. `Start()` 后不立即复制

### 现象
- 某些 `retry=false` 的测试偶发超时。

### 根因
- 新 entry 只能等到下一次周期心跳才会发出去。

### 修复建议
- `Start()` append 后立即触发 `broadcastAppendEntries()`。

---

## 10. `Make()` 初始化不全

### 现象
- index 异常、复制边界错、apply 行为错乱。

### 根因
- 缺少 dummy entry / nextIndex / matchIndex / applier 初始化。

### 修复建议
- `log` 以 dummy entry 开始（index=0, term=0）。
- 初始化：
  - `commitIndex=0`
  - `lastApplied=0`
  - `nextIndex/matchIndex` 数组
- 启动 applier goroutine，把 committed entry 顺序送到 `applyCh`。

---

## 11. apply 流程常见坑

### 现象
- `nCommitted` 看起来不增长，或顺序错。

### 根因
- 持锁发 `applyCh` 导致阻塞；
- 未保证 `lastApplied` 单调递增。

### 修复建议
- `lastApplied++` 后先构造消息，再释放锁发送 `applyCh`。
- 按 index 顺序推进，绝不跳号。

---

## 12. 调试策略经验

1. 先稳定 `TestBasicAgree3B`（最小闭环）。
2. 再打通 `LeaderFailure/FailAgree/Rejoin`（故障与重连）。
3. 最后优化 `Backup/Count`（性能与RPC数量）。
4. 每次只改一类问题，避免“修 A 坏 B”。

---

## 一句话复盘

3B 最难的不是“能复制”，而是：
- 在**分区/重连**下保持日志正确收敛；
- 在**2 秒窗口**内完成 leader 变更和提交；
- 同时把 RPC 数量控制在测试上限内。

