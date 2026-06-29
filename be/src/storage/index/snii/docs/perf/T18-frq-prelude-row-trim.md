## T18 — FrqPrelude 窗口行裁剪可推导冗余字段（格式变更，pre-launch）

### 1. 目标与背景

**问题（finding F11，MEDIUM，io-amplification + parse-CPU）**
窗口化（df>=512）term 的 `.frq` prelude 是该 term 每次查询的**第一次串行远程抓取**，且会被 eager 全量解析（`decode_all_blocks` 是 O(windows)）。当前每个窗口行序列化了 4–5 个**可推导/冗余**字段：

- `dd_off`（`frq_prelude.cpp:95`）、`freq_off`（`:100`）、`prx_off`（`:106`）——纯累加和，零信息量。`validate_region_layout`（`frq_prelude.cpp:377,388`）已经**证明** `m.dd_off == running-sum(dd_disk_len)`、`m.freq_off == running-sum(freq_disk_len)`；writer 把 prx 连续流式写出，`m.prx_off = running prx_total_len`（`logical_index_writer.cpp:300`）。
- `dd_uncomp_len`（`:97`）、`freq_uncomp_len`（`:102`）——当 zstd mode bit 未置位时恒等于 `*_disk_len`。`open_region` 对 raw 区强制 `uncomp_len==disk_len`（`frq_pod.cpp:111-116`），而 writer **始终**以 `kRawFrqRegion=0` 编码 dd/freq（`logical_index_writer.cpp:73,82,106,110`；`should_compress` 对 level==0 返回 false，`frq_pod.cpp:67-70`），故这两列在实践中 100% 冗余。

**预期收益（验证器校准后）**：docs+positions 行约 33–37B，其中冗余约 30–40%；docs-only 行约 16–18B，冗余约 25–30%。收益集中在高 df term（窗口数 = ceil(df/256)，df>=8192 时 ceil(df/1024)）：df=500K → ~500 窗口 → prelude ~17KB，省 ~6KB；极端 stop-word ~5000 窗口 → 省 ~60KB。这是**字节量 + eager parse varint 解码次数**的常数因子下降，**不减少 round-trip**（抓取本就是单个 BatchRangeFetcher range）。这是 batch 4 中**唯一允许在盘字节变更**的任务（F11 风险：必须做格式门控）。

### 2. 影响的文件/函数（现签名）

- `be/src/storage/index/snii/core/src/format/frq_prelude.cpp`
  - `encode_window_row(const WindowMeta& m, bool has_freq, bool has_prx, uint64_t prev_last, ByteSink* block)`（`:90`）——序列化端，去掉 3 个 off + 条件化 2 个 uncomp_len。
  - `decode_window_row(ByteSource* src, bool has_freq, bool has_prx, bool first_window, uint64_t* prev_last, WindowMeta* m)`（`:281`）——解码端，新增 running off 推导。
  - `decode_one_block(...)`（`:326`）、`decode_all_blocks(...)`（`:345`）——需把 running dd/freq/prx 累加器穿过（与现有 `prev_last` 同样跨块链接）。
  - `validate_region_layout(...)`（`:372`）——`m.dd_off==dd_expect` 检查变为恒真（推导得到），保留长度溢出检查 + `dd_block_len/freq_block_len` 汇总。
- `be/src/snii/format/frq_prelude.h`——更新文件头的 on-disk layout 注释块（删除 `dd_off/freq_off/prx_off` 行，标注 `*_uncomp_len` 为条件字段）；`WindowMeta`/`FrqPreludeReader` 公开签名**不变**。
- 不改 writer 数据流：`LayoutWindowRegions`（`logical_index_writer.cpp:204`）仍填 `m->dd_off/freq_off`，`BuildWindowedPosting`（`:300`）仍填 `m->prx_off`——这些 in-memory 字段被读端继续暴露，只是不再被 `encode_window_row` 序列化。
- **调用方零改动**：`scoring_query.cpp`、`docid_conjunction.cpp:817`、`reader/windowed_posting.cpp:106-118` 仅消费 `FrqPreludeReader` 公开 API（`window()`/`dd_block_len()`/`locate_window()`），返回的 `WindowMeta` 仍含 `dd_off/freq_off/prx_off`（现为推导值）。

### 3. 变更设计

**新行格式（pre-launch 折叠进 v1，无双路径）**：
```
last_docid_delta : VInt
doc_count        : VInt
win_mode         : u8           # bit0 dd_zstd, bit1 freq_zstd
dd_disk_len      : VInt
[dd_uncomp_len   : VInt]        # 仅当 win_mode & kDdZstd
crc_dd           : u32
if has_freq:
  freq_disk_len    : VInt
  [freq_uncomp_len : VInt]      # 仅当 win_mode & kFreqZstd
  crc_freq         : u32
if has_prx:
  prx_len        : VInt        # prx_off 由 reader 累加推导
max_freq         : VInt
max_norm         : u8
```
删除：`dd_off`、`freq_off`、`prx_off`。条件化：`dd_uncomp_len`、`freq_uncomp_len`。

**reader 推导算法**（在 `decode_all_blocks` 中维护三个 u64 running 累加器，跨 super-block 链接，与 `prev_last` 同生命周期）：
- `decode_window_row` 读 `dd_disk_len` 后：`m->dd_off = *dd_run; *dd_run += m->dd_disk_len;`；`m->dd_uncomp_len = m->dd_zstd ? <读VInt> : m->dd_disk_len;`。
- has_freq 时对 `freq_off`/`freq_uncomp_len` 同理（`*freq_run`）。
- has_prx 时：`m->prx_off = *prx_run; *prx_run += m->prx_len;`。
- 累加前用 `checked_add_u64` 防溢出（沿用现有 helper），失败返回 `Corruption`。

**FORMAT-COMPATIBILITY 结论**：**有在盘字节变更**。`format_constants.h:18-24` 明确这是 from-scratch、pre-launch 格式（无已落盘索引），`kMetaFormatVersion=1` 注释要求"pre-launch 变更直接折叠进 v1"。本计划**折叠进 v1**：新编码即唯一编码，writer/reader 对称，无旧格式双路径，`kMetaFormatVersion` 保持 1（prelude 不受 meta version 管辖，是 `.frq` posting 内部格式）。**回退门控**：若合入前发生 launch（出现 `lifecycle: launched` 索引），改用 prelude header flags 字节新增 `kSlimRows`（`1u<<2`，现仅用 kHasFreq/kHasPrx）作门控位，reader 按位选解码路径——本计划保留该 flags 字节扩展点，但默认不启用双路径。

**CONCURRENCY 结论**：**N/A，无共享可变状态**。`encode_window_row`/`decode_*` 是纯函数，只操作 caller 提供的 `ByteSink`/`ByteSource`/局部 `WindowMeta`/`std::vector`。`FrqPreludeReader` 在调用方是 query-local（`scoring_query.cpp:169/331`、`docid_conjunction.cpp:817`、`windowed_posting.cpp` 栈上），非共享 reader 的 per-reader 可变状态。不触及 `LogicalIndexReader` 的 const 只读路径，不引入任何锁。

**鲁棒性补偿（F11 验证器纠正 #2/#3）**：删除独立存储的 off 后，`validate_region_layout` 的 off 交叉校验失效（推导值恒等）。保留并依赖：(a) `windowed_posting.cpp` 中 `CarveRegionSlices`/`windowed_window_range` 的 `InBounds(m.dd_off, m.dd_disk_len, dd_block_len)`、`InBounds(m.freq_off, ..., freq_block_len)`（`:79,83,132,137`）；(b) prx 的 `InBounds(meta.prx_off, meta.prx_len, entry.prx_len)`（`:148,244`）；(c) `decode_all_blocks` 的窗口区 `block_off+block_len<=window_region.size()` 检查（`:352`）；(d) 推导累加的 `checked_add_u64` 溢出保护。raw 区 `uncomp_len==disk_len` 由 `open_region`（`frq_pod.cpp:112`）继续 enforce。

### 4. 依赖
- `depends_on`: 无（自包含的格式 reader/writer 对称修改）。
- 提供给他人：更小的 prelude 字节（降低任何 prelude 抓取/解析任务的常数）。与 T04（per-reader dict cache）正交。

### 5. TDD 步骤（RED → GREEN → REFACTOR）

新测试文件 `be/test/storage/index/snii_frq_prelude_test.cpp`（GLOB_RECURSE `UT_FILES` 自动纳入 `doris_be_test`，见 `be/test/CMakeLists.txt:24`），suite `SniiFrqPreludeTest` / `SniiFrqPreludeConcurrencyTest`（如适用）。

1. **RED-1 round-trip 等价**：写 `TEST(SniiFrqPreludeTest, DerivedOffsetsMatchExplicit)`：构造含 N=200 窗口（has_freq, has_prx, 全 raw）的 `FrqPreludeColumns`，`build_frq_prelude` → `FrqPreludeReader::open` → 对每个 `w` 断言 `decoded.dd_off/freq_off/prx_off/dd_uncomp_len/freq_uncomp_len/dd_disk_len/...` 等于原始 column 值。当前实现仍会序列化旧字段，重构前该测试针对**新解码逻辑**会因 reader 仍读旧字段而失败/不一致 → FAIL。
2. **GREEN-1**：实现 `encode_window_row`（去字段）+ `decode_window_row`/`decode_one_block`/`decode_all_blocks`（推导累加器），使 round-trip 通过。
3. **RED-2 字节缩减（确定性性能）**：`TEST(SniiFrqPreludeTest, RowTrimShrinksPreludeBytes)`：同一 columns，断言新 `build_frq_prelude` 输出 `sink.size()` == 精确期望值，且 == 旧大小 − N×(每行被删字节)。先写期望常量（按新格式手算）→ 在实现前 FAIL。
4. **GREEN-2**：实现使字节数命中期望。
5. **RED-3 corrupt/边界**：(a) 空窗口 N=0；(b) 单窗口；(c) df>=8192 大窗口（unit=1024）多 super-block 跨块累加；(d) 截断 prelude → `open` 返回 `Corruption`；(e) zstd 窗口（人工置 `dd_zstd=true` 且 `dd_uncomp_len!=dd_disk_len`）→ 验证条件字段正确写读；(f) 篡改某行 `dd_disk_len` 使汇总越界 → `Corruption`。
6. **GREEN-3**：补齐条件 uncomp_len 编解码 + 溢出/越界检查。
7. **REFACTOR**：抽出 `RunningOffsets{dd,freq,prx}` 小结构体穿参，更新 `frq_prelude.h` 头注释（删除 off 行、标注条件 uncomp_len），保留 flags 扩展点注释。改实现不改测试。
8. **端到端**：`TEST(SniiFrqPreludeTest, WindowedEntryPreludeFetchShrinks)`：用 `build_reader()` + `MemoryFile` 写一个高 df 窗口化 term，查询走 `read_windowed_posting`，断言 (a) 结果 docid 集合正确（与现有 query path 等价），(b) `MemoryFile::reads()` 中 prelude 抓取的长度 == 新（更小）`entry.prelude_len`。

### 6. 功能验证

gtest target：`doris_be_test`；文件 `be/test/storage/index/snii_frq_prelude_test.cpp`（+ 复用 `snii_query_test.cpp` fixture 做 E2E）。

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| FP-01 | N=200 窗口, has_freq+has_prx, 全 raw | columns | build→open | 每 w 的 `dd_off/freq_off/prx_off` `EXPECT_EQ` 原始累加值；`uncomp_len==disk_len` | 推导正确性(等价) |
| FP-02 | 同 FP-01 | columns | build | `sink.size()==expected_new` 且 `< expected_old` | 字节缩减(确定性) |
| FP-03 | N=0 | 空 columns | build→open | `n_windows()==0`，`open` OK | 退化边界 |
| FP-04 | N=1 | 单窗口 | build→open | round-trip 字段相等 | 单元素边界 |
| FP-05 | df=20000(unit=1024), N=20, G=64 跨多 super-block | columns | build→open | 跨块 running off 连续，`dd_block_len()==Σdd_disk_len` | 跨块累加链接 |
| FP-06 | 含 zstd 窗口(dd_zstd=true, uncomp!=disk) | columns | build→open | 条件 `dd_uncomp_len` 正确写读且 `!=dd_disk_len` | 条件字段路径 |
| FP-07 | 合法 prelude 后截断末 N 字节 | 损坏 buf | open | 返回 `Status::Corruption` | corrupt-input |
| FP-08 | 篡改一行 dd_disk_len 致汇总越界 | 损坏 buf | open | 返回 `Corruption`（汇总/越界检查触发） | 越界保护 |
| FP-09 | 高 df 窗口化 term 经 build_reader | term 查询 | read_windowed_posting | docid 集合 `EXPECT_EQ` 期望全集（新路径==旧语义） | E2E 等价 |

### 7. 性能验证（单体）— 确定性优先

| 指标 | 隔离手法 | 基线 | 断言/阈值 | 确定性 | 测试文件/target |
|---|---|---|---|---|---|
| prelude 编码字节数 | 直接 `build_frq_prelude(columns)` 单体 | 旧格式手算大小 `expected_old` | `sink.size()==expected_new`，新比旧每行省 (sizeof off×{2~3}+条件 uncomp×0) 字节；N=200 docs+pos 行省约 30–40% | 是 | snii_frq_prelude_test.cpp / doris_be_test |
| prelude 首轮抓取字节 | `MemoryFile::reads()` 记录 range 长度 | 改前 `entry.prelude_len` | E2E 中 prelude 抓取 len == 新更小 `prelude_len` | 是 | snii_frq_prelude_test.cpp (E2E) |
| serial_rounds 无回归 | `MeteredFileReader::metrics().serial_rounds` | 改前 == 1 | 改后仍 == 1（F11：非 RTT 优化，仅防回归） | 是 | snii_frq_prelude_test.cpp |
| 解码字节区长度 | `FrqPreludeReader::open` 成功 + window 区 len | 改前 window_region_len | 改后 window_region_len 更小且 open OK | 是 | snii_frq_prelude_test.cpp |
| (report-only) eager parse wall-clock | Google Benchmark `benchmark_snii_prelude.hpp`（`-DBUILD_BENCHMARK=ON`, RELEASE） | 改前 ns/op | 仅记录，**非门禁** | 否 | be/benchmark |

主断言为字节数缩减（确定性，等价于 F11 的 io-amplification 目标），故 `perf_tests_deterministic=true`。varint 解码次数下降无 seam，列为 gaps（用字节数代理）。

### 8. 验收标准

- `[功能]` FP-01/04/05 round-trip：`decoded WindowMeta` 全字段 `EXPECT_EQ` 输入（推导 off/uncomp 与旧显式存储位级等价）。验证手段：gtest `SniiFrqPreludeTest`。
- `[功能]` FP-07/FP-08 corrupt：`open` 返回 `Status::Corruption`。验证手段：`EXPECT_TRUE(st.is_corruption())`。
- `[功能]` FP-09 E2E：高 df 窗口 term 查询结果集 `EXPECT_EQ` 期望全集（新路径==旧语义）。验证手段：`build_reader()`+`MemoryFile`。
- `[性能-确定性]` FP-02：N=200 docs+pos 全 raw prelude 字节数较旧格式缩减约 30–40%，命中精确 `expected_new`。验证手段：`ByteSink::size()`。
- `[性能-确定性]` E2E：prelude 抓取 `MemoryFile::reads()` 长度下降；`serial_rounds==1` 不变。验证手段：`MemoryFile::reads()` / `MeteredFileReader::metrics()`。
- `[格式]` 折叠进 v1，无 `kMetaFormatVersion` bump；如 launch 提前发生则启用 `kSlimRows` flag 门控（保留扩展点）。
- `[并发]` N/A（纯函数，无共享可变状态）。

### 9. 风险与回滚

- **格式风险（最高，F11 验证器 #1）**：在盘字节变更。缓解：依赖 `format_constants.h:18-24` 的 pre-launch 状态折叠进 v1；合入前用 `grep lifecycle: launched` 复核无已落盘索引；若有则切 `kSlimRows` flag 双路径门控。回滚：恢复 `encode_window_row`/`decode_window_row` 旧 5 字段写读即可（无 schema 迁移，因无已落盘数据）。
- **正确性风险（F11 #2）**：丢失 `validate_region_layout` 的 off 交叉校验。缓解：保留 writer 端 `m->dd_off==running` 的 DEBUG assert + reader 端 `windowed_posting.cpp` 的全部 `InBounds` 检查 + `checked_add_u64` 溢出保护 + prx_off vs `entry.prx_len` 边界。FP-08 专测越界。
- **prx 推导风险（F11 #3）**：`prx_off` 推导后须仍受 `entry.prx_len` 约束——该检查在 `windowed_posting.cpp:148,244` 保留，不在 prelude reader 内（reader 不知 entry.prx_len）。
- **线程安全**：无（纯函数，验证器确认）。
- **回滚成本**：低，单文件对称修改，git revert frq_prelude.cpp/.h + 测试文件即可。
