# kvsrv1

1. ErrMaybe = 可能成功但无回信，须再 Get 核验，不可直接当成功。
2. Put 返回 OK = 我们写成功了，不是“被别人占用”。
3. lk.ck.Put 是发 RPC 到服务器，非本地写。clerk与现代client是同个意义。
4. Get 失败时必须重试，不能直接返回。
5. Put 失败时须重试同一 Put 直到拿到响应，不能直接返回 ErrMaybe。
6. Release = Put(lockname, "", ver)，无 Del 操作。
7. lockname 是锁的 KV key，与业务 key（如 "l0"）不同。
8. mu 保护进程内并发，Lock 保护跨 client 临界区；不能把 mu 换成 Lock。
9. myId 用来标识持有者、ErrMaybe 后核验、Release 前校验。
10. Acquire 中 Put 返回 ErrMaybe 时须 Get 验证 val==LID，不可直接 return。
11. Release 中 Put 失败须重试，不可直接 return。
12. Porcupine nil panic 与锁无关，是可视化库内部问题；DescribeOperation 加 nil 检查可规避。
