> **优先级：低（暂定，用户 2026-06-28 决定）。** 本文件是并发/锁专项分析的存档（含证据链）。
> 结论：当前无锁内 IO；searcher cache 自身是 per-shard 互斥锁（lookup/release 各一次、无 IO、已分片）；
> H1（T04 共享块缓存）/H2（reader-open 无 single-flight）为潜在隐患。详见正文。

# CONCURRENCY — 并发查询下的锁机制 / 锁粒度 / 锁内 IO 专项审查

> 来源：针对“并发查询情况下 SNII 锁机制是否有问题；锁粒度与锁内 IO 是否拖并发”的专项代码实读审查。所有结论附 file:line 证据。

## 一、现状结论（基于代码实读）

锁面极小，且**当前实现没有“锁内 IO”、也没有“粗锁串行化并发查询”**：

### 1) `DorisSniiFileReader::_section_ranges_mutex`（`std::shared_mutex`，snii_doris_adapter.h:98）
- `_classify_section`（snii_doris_adapter.cpp:134-155）持 `std::shared_lock`（:141），仅在内存中线性扫描 ≤5 个 `SectionRange`，函数返回即释放（作用域在 :155 结束）。
- `read_at`（:166-180）顺序：`_classify_section`（:170，锁内/锁外完成分类）→ 锁已释放 → `_read_at`（:175）做真正 IO（`_reader->read_at`，:198）。**IO 在锁外**。
- `read_batch`（:209-283）同理：`_classify_section`（:265）→ 锁释放 → `_read_at`（:270）。**IO 在锁外**。
- `register_section_refs`（:108-132）持 `std::unique_lock`（:126）仅 `push_back` ≤5 个 range，无 IO，且只在 index open 时调用一次（冷路径）。
- `shared_mutex` ⇒ 并发读（classify）不互斥；唯一互斥是 register(写) vs classify(读)，register 冷，可忽略。
- 结论：**不是并发瓶颈**（与第一轮 review 的 R9/R10 被判低影响一致）。唯一微小成本是每次物理读前一次 shared_lock 的原子获取/释放，但远小于其后的 IO，且读并发。

### 2) `s3_object_store.cpp` 全局 `std::mutex g_api_mu`（:29）
- 位于 `#ifdef SNII_WITH_S3`（:6）内——standalone/bench 路径，**非 Doris 生产路径**（生产经 `DorisSniiFileReader::_reader->read_at`，即 Doris 自己的 IO/S3 栈）。
- 只保护 `Aws::InitAPI/ShutdownAPI` 的进程级引用计数（`api_acquire`/`api_release`，:33-49），**不保护 GetObject**。即便 standalone 也不是锁内 IO。
- 结论：**非生产并发问题**。仅需一行备注：若将来 standalone S3 store 进入生产，需复审。

### 3) 跨查询共享的 `LogicalIndexReader`（经 `InvertedIndexSearcherCache` 缓存，snii_index_reader.cpp:343-346）
- 读路径目前**无锁只读**：`lookup()` 为 const；常驻 dict block / BSBF bitset 在 open 后不可变；on-demand dict block 解码到**栈局部** `OnDemandDictBlock`（logical_index_reader.h:124-130）。
- 结论：并发安全且不互斥；代价是重复 IO/解压（性能 findings F08/F20），**不是锁问题**。

→ **回答“当前并发/锁机制有没有问题”：没有锁内 IO、没有粗锁串行化查询的问题。** 但存在以下两个并发隐患。

## 二、隐患 1（最关键，属“将来自己挖的坑”）：T04 的 dict-block MRU 缓存会引入并发回归

`LogicalIndexReader` 跨查询共享，给它加可变缓存 = 引入共享可变状态。若 naive 实现（一把 `std::mutex` 包住整个缓存，且在 miss 时**锁内执行 ~64KB zstd 解压 + CRC**），就同时犯了：
- **锁粒度过粗**：串行化所有并发 term lookup（最热路径）。
- **锁内做重活**：解压/CRC 在临界区内，等价于“锁内 IO”级别的阻塞。
并发吞吐会显著下降——这正是用户担心的两点。

### 设计红线（必须写入总设计文档，硬约束 T04 / T07 / 任何新增 per-reader 缓存）
- 方案 A（推荐之一）：**request-scoped（每查询）块缓存** —— 彻底消除共享可变状态、无锁；解决“同一查询多词命中同一 block 反复解压”这一主因；跨查询复用交给 Doris page/file cache + searcher cache。
- 方案 B：**分片 / lock-striped 缓存 + 锁外解压** —— 锁只保护 map 的查/插；解压在锁外的局部缓冲完成后再插入；并发 miss 容忍偶发重复解压（可选 single-flight 合并）。
- **红线：任何情况下不得在持锁期间执行 zstd 解压、CRC 或 FileReader IO。**

## 三、隐患 2（现存）：冷启动 / 缓存淘汰时 reader 重复打开（thundering herd）

`_get_logical_reader`（snii_index_reader.cpp:329-346）在 cache miss 时直接 `open_snii_index`（:334，执行 meta + resident dict + BSBF 的 IO）然后 insert，**无 in-flight 去重（single-flight）**。N 个并发 miss 同一 index ⇒ N 次打开 IO，N-1 次 insert 浪费。高 QPS + 冷缓存 / 淘汰风暴时拖并发。

### 修复方向
- 对 `searcher_cache_key` 做 single-flight：per-key in-flight map（`mutex` + `condition_variable`/`shared_future`），首个 miss 打开、其余等待复用。**注意：single-flight 的等待用条件变量，绝不在持有打开锁时做 IO 之外的阻塞；打开 IO 本身在 in-flight 占位之后、全局锁之外执行。**
- 或先确认 `InvertedIndexSearcherCache::lookup/insert`（Doris 侧）的并发语义：是否分片锁、是否对并发同 key 的 insert/open 去重。若仅去重 insert 而 open 仍各自发生，则仍需 SNII 侧 in-flight 合并。

## 四、待确认项
- `InvertedIndexSearcherCache`（storage/index/inverted/inverted_index_cache.h）的 lookup/insert 并发语义（分片锁？同 key 并发 insert 去重？）——决定隐患 2 是否需要 SNII 侧 single-flight。

## 五、单体可验证的并发测试要点（供 T26 / T04 复用）
1. **“锁内禁 IO”结构性不变量**：将“分类（持锁）”与“IO（无锁）”在代码结构上分离为独立函数；单测断言 IO 函数不接触 `_section_ranges_mutex`。或在 mock FileReader 的 read 回调中通过探针断言 `_section_ranges_mutex` 未被本对象持有。
2. **T04 缓存并发（确定性）**：N 线程并发 lookup —— 断言（a）解压次数 == 唯一 block 数（request-scoped）或 ≤ 唯一 block 数 ×(分片冗余上界)（分片方案）；（b）在解压函数入口断言“本缓存锁未被持有”（锁外解压不变量，计数器=0）；（c）TSAN 无数据竞争。
3. **single-flight（确定性）**：N 并发 miss 同 key —— 用 mock/计数 FileReader 断言底层 `open`/meta 读次数 == 1。
4. 吞吐（report-only，非 CI 门禁）：固定并发度下的 lookup QPS，对比加缓存前后，确认无并发回归。
