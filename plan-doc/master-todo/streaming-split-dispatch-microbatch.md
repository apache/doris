# 待决项 1 —— 流式扫描的「分片派发」每次只送一个分片给后端分配器

> **状态**：⏳ 待定（**需用户签"接受后端分配语义变化"**才能立项）
> **改动层**：仅 fe-core 通用框架（`PluginDrivenScanNode` / `SplitAssignment` / `FederationBackendPolicy`），不触 BE、不触 thrift。
> **影响面**：流式路径由 **iceberg 和 trino 两个连接器**共用——通用改动同时影响两者（不能只改 iceberg，否则违反"通用节点禁按源名分支"）。
> **行号信 HEAD（55087e08d0c 附近，PERF-11 后）**，以 `grep` 为准。

---

## 背景

当一张外表非常大（匹配文件数 ≥ `num_files_in_batch_mode`，默认 1024；实际针对 10 万~100 万分片的扫描），扫描走**流式**模式：不一次性把所有分片算出来堆在 FE 内存里（会 OOM），而是一边惰性读元数据、一边把分片"泵"给调度器，靠背压（队列满就等）把 FE 堆压住。

问题不在"读元数据"（那是必要成本），而在**把分片交给"后端分配器"的方式**——分配器（`FederationBackendPolicy`）负责决定"这个分片让哪台 BE 去扫"。

---

## 当前代码的调用栈与问题

```
startStreamingSplit()                                     PluginDrivenScanNode.java:1600  [单个后台任务]
└─ while (needMoreSplit() && source.hasNext()):           :1638   ← 逐个分片的泵循环
     one = [ new PluginDrivenSplit(source.next()) ]       :1639-1640  ← 每次只包 1 个分片的 List
     splitAssignment.addToQueue(one)                      :1641 → SplitAssignment.java:143
       └─ synchronized (assignLock) { ... }               SplitAssignment.java:148   ← 每分片一次加锁往返
            backendPolicy.computeScanRangeAssignment(one)   :153 → FederationBackendPolicy.java:225
              ├─ Collections.shuffle(one, seeded)          :228   ← 对 1 个元素洗牌 = 空操作
              ├─ backends = flatten(backendMap)            :231-234 ← 把全部 ~100 台 BE 拷进新 List
              ├─ new ResettableRandomizedIterator(backends)  :235  ← 又拷一遍(默认 ROUND_ROBIN 用不到)
              ├─ 选 1 台 BE (ROUND_ROBIN: nextBe++)         :269-270
              └─ if (enableSplitsRedistribution)           :307   ← 默认 true、无生产关闭途径
                   equateDistribution(assignment)          :320
                     ├─ allNodes 排序 O(B·logB)            :329
                     └─ 建两个 IndexedPriorityQueue         :335-343  ← 对全部 BE 各建一遍堆
```

**问题**：泵循环每吐一个分片，`computeScanRangeAssignment` 就把"和这一个分片无关的固定开销"从头重算一遍——把上百台 BE 拷两遍、建 multimap、洗牌、加锁往返，**还有 `equateDistribution`**：它对全部后端做一次 `O(B·logB)` 排序 + 建两个堆，而且默认恒开（`enableSplitsRedistribution=true`，那个 setter 只有测试在调、生产没有关它的路径）。

100 万分片 × 上百台 BE，这些"每分片重建"的操作叠起来是纯 FE 的 CPU + 大量短命对象（GC 压力）。**没有远程 IO**，所以量级有限（10 万分片约 0.1~0.5 秒、100 万分片几秒），但确实是白干——这些固定开销本该一批分片摊一次。

> 对照：分区批风格的兄弟路径已经是"整批一起 `addToQueue`"（`PluginDrivenScanNode.java:1570` 附近），非流式的 legacy 路径也是"所有分片一次 `computeScanRangeAssignment`"（`FileQueryScanNode.java:431`）——唯独流式泵是"一次一个"。

---

## 解决方案的调用栈与问题

方向：泵侧**微批**——攒够 K 个（64~256）再一起送。

```
startStreamingSplit()
└─ batch = new ArrayList(K)
   while (needMoreSplit() && source.hasNext()):
        batch.add(new PluginDrivenSplit(source.next()))
        if (batch.size() >= K) {
            splitAssignment.addToQueue(batch)             ← 一次送 K 个
            batch = new ArrayList(K)
        }
   if (!batch.isEmpty()) splitAssignment.addToQueue(batch)  ← 末批必须 flush，否则丢分片
   splitAssignment.finishSchedule()
     └─ computeScanRangeAssignment(batch)                 ← 从"每分片一次"变"每 K 个一次"
```

**方案自身的问题（正是它需要用户签字的原因）**：

1. **不是字节等价——后端分配结果会变**（关键）。当前"一次一个"下，`Collections.shuffle`（对单元素是空操作）和 `equateDistribution`（单分片没什么可再平衡的）实际都不起作用；一旦变成"一批 K 个"，这两步就**开始真正做事**：洗牌重排这 K 个的顺序、`equateDistribution` 在这一批内部把分片从"堆得多的 BE"挪到"堆得少的 BE"。而 `nextBe`、`assignedWeightPerBackend` 是**跨批累积的实例字段**（`FederationBackendPolicy.java:84/88`）。所以**同一批分片最终落到哪些 BE 会和现在不同**。
   - 功能上安全：每个分片仍恰好被分配一次、所有数据都读到，变的只是"哪个分片去哪台机器"——负载均衡的启发式结果（甚至更均衡）。
   - 但它**违反"共享框架热路径须逐字节不变"**纪律，不能声称"透明无感"。且"一次一个"本身是照搬**上游** legacy `doStartSplit`（`:1635-1637` 注释写明），改它属于"在上游基线上演进"，不是"修回归"。→ **需要用户签"接受这个负载均衡分配变化"**。

2. **正确性不变式要小心**：`needMoreSplit()` 背压要**每批**重新检查（不能攒到一半该停了还继续攒）；末批一定要 flush；批边界不能丢/重分片；`source.close()` 仍在 finally 里吞异常。这些可单测。

3. **验证**：碰 fe-core → 两段验（iceberg 连接器不依赖 fe-core）。但"省了多少时延"和"分配分布变成什么样"这两个承载性结论**单测测不出来**，要真 BE 分布式跑 ≥1024 文件的流式扫描（iceberg + trino 各一次）才能观测；正确性不变式（不丢不重/背压）可用 mock split source 单测。

---

## 示例

设 100 台 BE、一次流式扫描 50 万分片、微批 K=128：
- **当前**：`computeScanRangeAssignment` 被调 **50 万次**，每次拷 100 台 BE 两遍 + 排序 100·log100 + 建两个堆 + 一次加锁 → 约 50 万次全套固定开销。
- **微批后**：被调 **≈3900 次**（50 万 / 128），固定开销摊薄到 1/128。
- **代价示例**：假设分片按 round-robin，当前第 7 个分片去 BE#7、第 8 个去 BE#8……微批后，这 128 个先被洗牌重排、再被 `equateDistribution` 按累积权重挪动，**第 7 个分片可能落到 BE#42**。查询结果完全一样，但"哪台机器扫哪个文件"的分布图变了——这就是需要用户点头的那个"语义变化"。

---

## 立项前须确认

- [ ] 用户签字：**接受微批带来的后端分片分配分布变化**（功能等价、负载均衡启发式变）。
- [ ] 定 K（批大小）与背压交互；确认微批不改"每分片恰分一次 / 不丢不重"。
- [ ] 两段验 + 真 BE 分布式 smoke（iceberg + trino 流式各一次）。

## 关联

审计原始证据 / 逐项调用链：`plan-doc/reviews/perf-audit-fe-connector-iceberg-2026-07-17*`（该簇 findings）；任务空间 `plan-doc/perf-hotpath-iceberg/`。
