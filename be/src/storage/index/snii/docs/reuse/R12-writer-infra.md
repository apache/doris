# R12-writer-infra — TempDir / MemoryReporter / SpillableByteBuffer

## R12 writer-infra 决策文档（TempDir / MemoryReporter / SpillableByteBuffer）

### 结论与依据
**Verdict: keep-snii-doris-suboptimal（保留 SNII 实现；Doris 有近似物但对构建期索引 writer 次优），并附带一个轻量扩展建议（reuse-with-extension flavor）：把 MemoryReporter 的 consume_release 回调接到真实的 Doris MemTracker。**

三个子件均不触碰已发布的 on-disk format v2——它们写的是**临时/溢出文件**（spillable_byte_buffer.h:128-131 的 `snii_<tag>_<pid>_<this>.tmp`，RAII 删除 line 48），属构建期临时态，stream_into() 再原样拼回最终段（line 99-117）。故 `touches_on_disk=false`，无字节兼容性红线。

### Doris 等价物与"是否最优"
1. **temp_dir.h**（`resolve_temp_dir`/`temp_dir_available_bytes`，2 处调用）
   - Doris 等价：`SpillDataDir`/`SpillFileManager`（be/src/exec/spill/spill_file_manager.h），配置源 `config::spill_storage_root_path`（config.h:1569）。
   - 次优原因：SpillFileManager 为 query/pipeline 作用域——`create_spill_file` 接受 `query_id/operator` 相对路径（spill_file_manager.h:103-110），常驻 ExecEnv，带 GC 线程与 MetricEntity（line 151-156）。SNII spill 在 load/compaction 的 segment 索引写入路径执行，无 query_id/RuntimeState，复用会把 exec pipeline 依赖倒灌进存储写入层。temp_dir.h 仅 40 行纯函数。
   - **建议扩展（可选，M）**：让 `resolve_temp_dir()` 在 SNII_TEMP_DIR/TMPDIR 之外回退到 `config::spill_storage_root_path`（首个真实磁盘 data dir），以与 Doris 运维配置一致，并落实 temp_dir.h:14-16 关于"勿用 tmpfs"的告诫——这恰是 SpillDataDir 的设计意图。

2. **memory_reporter.h**（`MemoryReporter`，4 文件引用）
   - Doris 等价：`MemTracker`（mem_tracker.h:39-43，observe-only consume/release/consumption）；`MemTrackerLimiter`（mem_tracker_limiter.h:150-198，try_consume/limit/cache_consume）。
   - 次优原因：MemTracker 无 cap/gate；MemTrackerLimiter 的 limit 是进程/query 级，不等于"单 writer 统一 gate-2 buffer cap + over_cap() 自溢出"（memory_reporter.h:38-43）。且 MemoryReporter 的 `ConsumeReleaseFn`（line 19,33）是 snii core 对 Doris 的**零耦合缝**（off-Doris 传 null），必须保留以满足"core 0 CLucene/0 Doris 强依赖"。
   - **现状缺口**：snii_index_writer.cpp:50 当前 `MemoryReporter(nullptr, spill_threshold)`——consume_release 是 null，索引构建 RAM **未**计入任何 Doris MemTracker。
   - **建议扩展（M）**：在集成层（snii_index_writer）构造一个 segment/load 级 MemTracker，把 `consume_release = [t](int64_t d){ d>0? t->consume(d): t->release(-d); }` 注入。本体不变，仅填回调。

3. **spillable_byte_buffer.h**（`SpillableByteBuffer`，2 文件引用）
   - Doris 等价：`SpillFileWriter`（spill_file_writer.h:56 `write_block(RuntimeState*, const Block&)`）；`faststring`（util/faststring.h:105）。
   - 次优原因：SpillFileWriter 是 Block 粒度、带 part 轮转/footer/RuntimeProfile，granularity 完全错配原始字节 section；faststring 是单段几何倍增 vector，恰是 SpillableByteBuffer 的 chunk 链刻意规避的 slack/realloc 瞬时（spillable_byte_buffer.h:21-29，"resident cost 精确等于 appended bytes"）。SNII 复用 `doris::snii::io::LocalFileWriter/Reader`（已 Doris/CLucene-free）做 spill IO。

### 迁移设计（具体改动 / 签名 / 调用点 / 风险回滚）
- 改动 A（temp_dir 扩展，可选）：`resolve_temp_dir()` 末尾回退链追加 `config::spill_storage_root_path` 解析出的首个真实目录；签名不变。调用点：spillable_byte_buffer.h:129。
- 改动 B（MemoryReporter 接 MemTracker，推荐）：仅在 snii_index_writer.cpp:50 把第一个实参从 `nullptr` 改为绑定 segment/load MemTracker 的 lambda。MemoryReporter/SpillableByteBuffer 头文件零改动。
- 风险：B 的计数对账（dtor line 45-48、spill 负 delta line 136-141 必须净零）；若 MemTracker 生命周期短于 writer 会 use-after-free——须让 MemTracker 与 writer 同寿或更长。
- 回滚：回调置回 null 即恢复现状（off-Doris 路径），零格式影响。

### 为何整体 KEEP（Doris 次优总结）
本组件是 SNII 构建期 RAM/spill 的**解耦基础设施**：Doris 的 spill/MemTracker 设施均面向 exec pipeline 的 query 作用域且粒度/依赖不匹配存储写入路径。保留 SNII 薄实现 + 仅在集成层用回调接 Doris MemTracker，是同时满足"core 解耦"与"尽量复用 Doris 观测"的最优折中。

---

## TDD

## R12 writer-infra TDD 测试计划（gtest 目标：doris_be_test，置于 be/test/storage/index/snii/，文件 snii_writer_infra_test.cpp）

说明：本组件 `touches_on_disk=false`（仅临时/溢出文件，非 format v2），故**不需要 on-disk 字节黄金测试与 cross-decode**；重点是功能、RAM 对账与 RAM/spill 路径产出等价。

### RED -> GREEN -> REFACTOR
先写断言（RED），跑红，再补/接线实现（GREEN），最后清理（REFACTOR）。

### 1. 功能验证
- `TempDir_ResolveOrder`：设置 SNII_TEMP_DIR 优先、清空后回退 TMPDIR、再回退 /tmp；断言尾部 '/' 被剥离（temp_dir.h:22）。扩展实现后追加：两者皆空时回退 `config::spill_storage_root_path`。
- `TempDir_AvailableBytes_StatvfsFail`：对不可 stat 路径断言返回 `UINT64_MAX`（temp_dir.h:36）。
- `MemoryReporter_CounterAndCap`：report(+N)/report(-M) 后 current_bytes() 精确；cap=K 时 over_cap() 在 >=K 触发、cap=0 永不触发（memory_reporter.h:40-42）。
- `SpillableBuffer_RamOnly_RoundTrip`：cap=UINT64_MAX，多次 append/append_move 后 stream_into 到内存 FileWriter，断言字节顺序与拼接完全等于输入序列。
- `SpillableBuffer_Spill_RoundTrip`：cap 设小触发 spill（断言 spilled()==true），seal() 后 stream_into 产出与同输入的 RAM-only 路径**逐字节一致**。
- `SpillableBuffer_TempCleanup`：析构后 temp_path_ 文件不存在（line 48）。

### 2. 等价性验证（新旧 / 双路径一致）
- `Equivalence_RamVsSpill_SameBytes`：同一组 append 序列分别走 RAM-only 与强制 spill 两条路径，stream_into 输出 `EXPECT_EQ` 完全相同（确定性断言）。这是"实现差异不改变可观测结果"的核心保证。
- `Equivalence_ReporterWired_vs_Null`：分别用 null 回调与绑定到一个 fake MemTracker 的回调跑同一序列，断言产出字节一致（回调只观测不改数据）。

### 3. RAM 对账（替代 on-disk 黄金测试的等价守门）
- `Reporter_NetZero_OnDestroy_RamPath`：构造绑定计数器的 MemoryReporter，跑 RAM-only buffer 后析构，断言计数器净值回到 0（覆盖 dtor 平衡 line 45-48）。
- `Reporter_NetZero_OnSpill`：触发 spill，断言 spill 时一次性负 delta == 之前 ram_bytes_（line 136-141），spill 后 current_bytes 不再计 resident；buffer 析构不再二次释放。
- `Reporter_MirrorsToMemTracker`（接线后）：注入 `[t](d){d>0?t->consume(d):t->release(-d);}`，跑完整 build+spill+析构，断言 `t->consumption()==0` 且峰值>0。

### 4. cross-decode
n/a（不产生持久化格式字节，无跨实现解码需求）。

### 备注
若仅采纳 KEEP 而不做扩展接线，则第 1、2、3(前两项) 仍应作为现状回归测试补齐（当前 be/test 下无 SpillableByteBuffer/MemoryReporter/temp_dir 专项测试）；扩展接线部分（1 的 config 回退、2 的 wired、3 的 MirrorsToMemTracker）随改动落地。