## 1. 目标与背景

**问题（finding F37，LOW，algorithmic，需改格式 False）**：
`FrqPreludeReader::open` 一次性解码**全部** N 个 window 行，与"两级（super-block → window）跳表"的设计初衷相悖。该格式专门携带两级目录，使 reader 能在不解码其余行的前提下定位单个 window；但当前 `open` 在 `decode_all_blocks`（`frq_prelude.cpp:345-367`）里遍历每个 super-block，`decode_one_block`（:326-342）逐行 `decode_window_row`（:281-322，每行 ~9 个 varint + 2 fixed32 + 2 u8 + 若干 checked-arith/validate），随后 `validate_region_layout`（:372-400）再次遍历所有 window。`locate_window`（:445-468）/`window`（:436-443）却只索引已全量解码的 `windows_`。

**真正受益路径（验证器确认）**：选择性 phrase / conjunction 校验。`docid_conjunction.cpp:817` 每 TermPlan open 一次 prelude；`select_covering_windows`（:497-514）对每个候选调 `locate_window`，当候选很少时只触达极少数 window（`should_scan_all_windows` 启发式 :516-524 已把稠密场景路由到 `all_windows`）。对一个高 df（数千 window）但只有少量 phrase 候选的 term，`open` 解析全部 N 行只为用其中几行 —— 真实 CPU 浪费。

**非受益路径（验证器纠正，本任务不触碰其语义）**：scoring 路径（`scoring_query.cpp` 的 `BuildWindowBounds` :92-107 / `BuildLazyWindowed` :415-437）**本就遍历全部 window** 来构建 block-max 边界与 win_start 前缀和，WAND 块跳跃天然需要每个 window 的 max_freq/max_norm/docid 范围，惰性解码对其无收益（且 scoring 未接入 Doris 查询执行，已 DEFERRED）。本任务在该路径上保持"全量访问"行为（仍调用 `window(w)` 逐个取，惰性缓存对其透明）。

**预期收益**：把 prelude 解析从 O(N windows) 降到 O(touched super-blocks)。无 IO 放大（prelude Slice 在 open 前已全驻内存：`windowed_posting.cpp:107-118`、`scoring_query.cpp:80-86`、`docid_conjunction.cpp:817`），收益为纯 varint 解析 CPU，量级 modest（亚毫秒，验证器评估 ~3-30us/普通高 df term）。

## 2. 影响的文件/函数

仅 reader 侧，**build 路径完全不动**（保证零在盘字节变更与字节级回归）。

- `be/src/snii/format/frq_prelude.h`
  - `class FrqPreludeReader`：新增 owned 字节缓冲与惰性 super-block 缓存成员；`window`/`locate_window` 保持 `const` 签名（内部用 `mutable` 缓存）；新增测试 seam 访问器 `uint32_t decoded_super_block_count() const`。
  - 更新类头注释（删除"eagerly decodes every window block"措辞，改述惰性语义；保留格式注释不变）。
- `be/src/storage/index/snii/core/src/format/frq_prelude.cpp`
  - `FrqPreludeReader::open(Slice, FrqPreludeReader*)`：改为只解析 header + super_block_dir（验 crc）+ 拷贝 window_region 原始字节 + 解码**最后一个** super-block 以派生 `dd_block_len_`/`freq_block_len_`。
  - `FrqPreludeReader::window(uint32_t, WindowMeta*) const` / `locate_window(...) const`：改为按需触发目标 super-block 的惰性解码。
  - 新增 `private` 帮助函数：`ensure_super_block_decoded(size_t sb) const`（解码并缓存第 sb 个 super-block 的 window 行；`mutable`）。
  - 复用现有 `decode_one_block`/`decode_window_row`/`SbDirRow`/`Header`（保持解码与校验逻辑一致）。

不改动的调用方（签名/语义不变，惰性对其透明）：
- `docid_posting_reader.cpp:164-181`（open 后立即用 `dd_block_len()` 校验前缀长度，再逐 window 全量遍历）。
- `windowed_posting.cpp:48-64`（`ResolveBlocks` 用 `dd_block_len()`/`freq_block_len()`）、:121-139（`windowed_window_range` 用 `window(w)`）、:238-241。
- `docid_conjunction.cpp:497-514, 631, 817`、`scoring_query.cpp:92-107, 357, 428-430`。

当前关键签名（不变）：
- `static Status FrqPreludeReader::open(Slice prelude, FrqPreludeReader* out);`
- `Status window(uint32_t w, WindowMeta* out) const;`
- `Status locate_window(uint32_t docid, bool* found, uint32_t* w) const;`
- `uint64_t dd_block_len() const; uint64_t freq_block_len() const; uint32_t n_windows() const; uint32_t n_super_blocks() const;`

## 3. 变更设计

### 数据结构（FrqPreludeReader 私有成员）
保留：`has_freq_/has_prx_/group_size_/n_super_/dd_block_len_/freq_block_len_/sb_last_docid_`。
删除：~~全量 `std::vector<WindowMeta> windows_`~~。
新增：
```cpp
// open 时拷贝的 window_region 原始字节（reader 必须自持，原因见下）。
std::vector<uint8_t> window_region_;            // owned copy
// 每个 super-block 在 window_region_ 内的 [off,len) 与其绝对 last_docid。
struct SbSpan { uint64_t off; uint64_t len; uint64_t last_docid; };
std::vector<SbSpan> sb_spans_;                  // size n_super_，resident
// 惰性解码缓存：每 super-block 一段已解码 WindowMeta。mutable 以支持 const 访问。
mutable std::vector<std::vector<WindowMeta>> sb_cache_;   // size n_super_，初始全空
mutable std::vector<char> sb_decoded_;          // size n_super_，0=未解码/1=已解码
mutable uint32_t decoded_sb_count_ = 0;         // 测试 seam：已解码的 distinct 块数
uint32_t n_windows_ = 0;                        // = h.n，open 时记录（不再由 windows_.size() 推导）
```

**为什么必须自持 window_region 拷贝（关键正确性约束）**：现行 reader 注释"does not retain the input"。`windowed_posting.cpp:114-118` 在局部 `BatchRangeFetcher fetcher` 上 fetch 后 `return FrqPreludeReader::open(fetcher.get(h), prelude)`，open 返回后 fetcher 析构，prelude Slice 指向已释放缓冲。要惰性解码就必须保留 window-row 字节，因此在 open 时把 `window_region` 这段（仅 ~20-30KB 量级，且 ≤ prelude 总长）一次性 memcpy 进 `window_region_`。该 memcpy 远比"解析全部行"廉价，故净收益为正。拷贝用 `snii::resize_uninitialized`（T19，立即被 memcpy 全量覆写）；无 T19 时退化为 `resize`。

### 算法

**open(prelude)**：
1. `parse_header` + `verify_covered_crc`（不变，crc 仍只覆盖 header+super_block_dir）。
2. `decode_super_block_dir` → `rows`（含每块 block_off/block_len/last_docid，已校验 contiguous、in-bounds、单调 last_docid）。
3. 计算 `window_region` 边界（不变），把该段 memcpy 进 `window_region_`；据 `rows` 填 `sb_spans_`、`sb_last_docid_`、`n_super_`、`n_windows_=h.n`；`sb_cache_/sb_decoded_` 按 n_super_ 置空。
4. **派生块长**：若 `n_super_>0`，`ensure_super_block_decoded(n_super_-1)`（解码最后一块），取其最后一个 window 的 `dd_off+dd_disk_len` 作为 `dd_block_len_`，`freq_off+freq_disk_len`（has_freq 时）作为 `freq_block_len_`；N==0 时二者为 0。利用 contiguous tiling 不变量：window i 的 `dd_off` = 前缀和，故末窗的 `dd_off+dd_disk_len` == 全块长。

**ensure_super_block_decoded(sb)（mutable，const 可调）**：
- 若 `sb_decoded_[sb]` 已置位则直接返回。
- 取 `prev_last = (sb==0) ? 0 : sb_last_docid_[sb-1]`（**跨块 win_base 链可由 resident 的 sb_last_docid_ 独立重建**，无需顺序解码）。
- `first_window = (sb==0)`（仅全局窗 0 的 doc_count 校验用 first_window=true；`decode_one_block` 内以 `windows->empty()` 判定 first，这里改为传入显式 first 标志或预置 prev_last 使语义等价 —— 实现上把"是否首窗"逐行计算为 `sb==0 && i==0`）。
- 用 `decode_one_block` 等价逻辑解码 `window_region_` 内 `[sb_spans_[sb].off, len)` 的 `rows=min(group_size_, n_windows_ - sb*group_size_)` 行进 `sb_cache_[sb]`；末尾校验 `prev_last == sb_spans_[sb].last_docid`（保留 sb 边界 last_docid 一致性校验）、块内无 trailing 字节、**块内 dd/freq contiguity**（首窗 dd_off 起点 = 该块在全局的起始 dd_off，块内逐窗 dd_off=running）。
- 置 `sb_decoded_[sb]=1`，`++decoded_sb_count_`。

**window(w)**：算 `sb=w/group_size_`，`ensure_super_block_decoded(sb)`，返回 `sb_cache_[sb][w - sb*group_size_]`。越界返回 `InvalidArgument`（不变）。

**locate_window(docid)**：Level-1 在 resident `sb_last_docid_` 上二分（无需解码）；定位到 sb 后 `ensure_super_block_decoded(sb)`，Level-2 在该块内线性/二分。`docid > 全局末窗 last_docid` 的 found=false 快路用 `sb_last_docid_.back()` 即可，**无需解码任何块**。

### FORMAT-COMPATIBILITY 结论
**reader/writer-only，零在盘变更**。build 路径一字未改，`build_frq_prelude` 输出字节级不变；在盘 prelude 布局、crc 覆盖范围、win_mode 语义全部不变。

### CONCURRENCY 结论
**N/A，无共享可变状态**。已读码确认 `FrqPreludeReader` 全部实例均为 **request-scoped（每查询）**：内嵌于 `TermPlan`（`docid_conjunction.h:33`，per-query plan 结构）、scoring 的 `LazyTermCursor`、以及 `windowed_posting`/`docid_posting_reader` 的栈局部；**从不**存放于被 `InvertedIndexSearcherCache` 跨线程共享的 `LogicalIndexReader`（grep 确认 logical_index_reader 未持有 FrqPreludeReader/TermPlan）。因此新增 `mutable` 惰性缓存为单线程（单查询）访问，**无需锁、无原子**；解码为纯 CPU、读自 owned 缓冲，临界区内无任何 FileReader IO / zstd / crc-over-IO，天然满足 NO-IO-UNDER-LOCK（因为根本没有锁）。**约束（必须在头注释固化为不变量）**：FrqPreludeReader 必须保持 per-query 生命周期，严禁置于跨线程共享的 reader 上；若未来需共享，须改为 request-scoped 副本或加分片锁（解压在锁外）。

## 4. 依赖
- 硬依赖：无。
- 软依赖 / shared infra：`snii::resize_uninitialized`（T19）用于 window_region 拷贝；缺失时 `std::vector::resize` 替代，功能不受影响。
- 不依赖、不修改其他任务；不与 T04（per-reader dict-block 缓存）共享状态（本任务缓存在 per-query 的 FrqPreludeReader 内，非共享 reader，H1/H2 风险不适用）。

## 5. TDD 步骤（RED → GREEN → REFACTOR）

> 测试落 `be/test/storage/index/snii_frq_prelude_test.cpp`（新文件，GLOB_RECURSE `test/**/*.cpp` 自动纳入 `doris_be_test`，见 `test/CMakeLists.txt:24`，无需改 CMake）。套件名 `SniiFrqPreludeTest` / `SniiFrqPreludeConcurrencyTest`（后者本任务标 N/A，不写）。

1. **RED-1（seam 访问器 + 惰性计数）**：先在头里加 `decoded_super_block_count()` 访问器但**不**改 open 行为，写 `TEST(SniiFrqPreludeTest, OpenDecodesOnlyLastSuperBlock)`：build N=4000、G=64（≈63 super-blocks）的 prelude，`open` 后断言 `decoded_super_block_count()==1`。当前 eager 实现会等于 63（或访问器尚不存在编译失败）→ FAIL。
2. **GREEN-1**：实现 open 惰性化 + `ensure_super_block_decoded` + window_region 自持拷贝 + 末块派生块长。使该断言转 GREEN。
3. **RED-2（等价性）**：`TEST(SniiFrqPreludeTest, WindowMetadataMatchesReference)`：对同一 `FrqPreludeColumns` 输入，逐 window `window(w)` 全量取出，与输入列（期望 WindowMeta，含派生 win_base/last_docid）逐字段 `EXPECT_EQ`；并断言 `dd_block_len()`/`freq_block_len()` 等于手算的 `sum(dd_disk_len)`/`sum(freq_disk_len)`。在 GREEN-1 后应已通过；若派生逻辑有误则 FAIL → 修正。
4. **RED-3（选择性解码计数）**：`TEST(SniiFrqPreludeTest, LocateDecodesOnlyTouchedSuperBlocks)`：open 后 `locate_window(落在第 3 块的 docid)`，断言 `decoded_super_block_count()==2`（末块 + 第 3 块）；再 locate 落在末块的 docid，计数仍为 2。RED→GREEN 由 ensure 缓存幂等保证。
5. **RED-4（损坏行为变化）**：`TEST(SniiFrqPreludeTest, CorruptWindowRowSurfacesAtAccess)`：手工翻转某中间 super-block 的一行字节（crc 不覆盖该区），断言 `open` 返回 OK（仅末块被解码），而 `locate_window`/`window` 触达该块时返回 `Corruption`。RED 先于实现 ensure 内的块内校验。
6. **GREEN-4**：在 `ensure_super_block_decoded` 内补齐块内 trailing/last_docid/contiguity 校验，使损坏在访问期被捕获。
7. **REFACTOR**：抽出 `decode_one_block` 复用、去重，确保 `decode_window_row` 逻辑零分叉（块内校验与原 `validate_region_layout` 的块内部分等价）；跑 `be-code-style`。
8. **回归（build 字节级不变）**：`TEST(SniiFrqPreludeTest, BuildOutputUnchangedByteIdentical)`：固定输入 build，断言 `ByteSink::buffer()` 与改动前 golden 逐字节相等（build 未改，作守护）。

每批自闭环：业务代码 + 上述 UT + 性能断言（计数）同批交付。

## 6. 功能验证

gtest target：`doris_be_test`，文件 `be/test/storage/index/snii_frq_prelude_test.cpp`，套件 `SniiFrqPreludeTest`。复用 `build_frq_prelude`/`FrqPreludeColumns` 直接构造输入（纯单元，无需 MemoryFile/reader fixture）。

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| FP-EQ | N=4000,G=64,has_freq,has_prx 全字段填值 | 构造 columns | open + 逐 w `window(w)` | 每窗各字段 `EXPECT_EQ` 期望（含 win_base/last_docid/doc_count/dd_*/freq_*/prx_*/max_*）；`dd_block_len()`==Σdd_disk_len，`freq_block_len()`==Σfreq_disk_len | 新路径==旧路径等价性 |
| FP-LOC | 同上 | 跨块边界 docid（每 super-block 首/末窗、最后一窗、超末窗+1） | `locate_window` | found 命中正确 w；超末窗 found=false 且 OK | locate 正确性、边界 |
| FP-EMPTY | N=0 | 空 windows | open + locate(任意) + dd/freq_block_len | open OK；`n_windows()==0`；`dd_block_len()==0`；`freq_block_len()==0`；locate found=false；**`decoded_super_block_count()==0`** | 退化：空 |
| FP-ONE | N=1,G=64 | 单窗单块 | open + window(0) + locate | 字段正确；open 后 decoded==1 | 退化：单元素 |
| FP-PARTIAL | N=130,G=64（末块 2 行） | 末块非满 | open + window(全部) | 全部正确，末块行数=2 | 非整除分块边界 |
| FP-NOFREQ | has_freq=false | 同 EQ 但无 freq | open + window | freq_* 为 0，无 freq 列；块长 freq=0 | has_freq 分支 |
| FP-CRC | 翻转 header/super_block_dir 1 字节 | 损坏 covered 区 | open | 返回 `Corruption`(crc mismatch)（行为不变） | 错误路径：crc |
| FP-CORRUPT-ROW | 翻转中间块某行 1 字节（crc 不覆盖） | 损坏 window 行 | open，再 locate/window 触达该块 | open OK 且只解末块；触达块 → `Corruption`（trailing/last_docid/contiguity 之一） | 错误路径：访问期损坏检测（行为变化） |
| FP-CORRUPT-OOB | 把某行 dd_off 改为越界但块内 | 损坏 locator | window 触达 | 访问期 `Corruption`（块内 contiguity）或下游 InBounds 捕获 | 降级校验仍兜底 |
| FP-BUILD-GOLD | 固定输入 | — | build_frq_prelude | `ByteSink::buffer()` 与 golden 逐字节相等 | 零在盘变更回归 |

## 7. 性能验证（单体）—— 确定性优先

隔离手法均为纯单元（直接 build prelude → open，不经 FileReader），用 reader 内置 `decoded_super_block_count()` 操作计数 seam（测试间随对象重建自动归零）。

| 指标 | 隔离手法 | 基线 | 断言/阈值 | 确定性 | 测试文件/target |
|---|---|---|---|---|---|
| open 解码块数 | `decoded_super_block_count()` after open，N=4000/G=64（63 块） | 旧 eager=63（全部） | `== 1`（仅末块派生块长） | 是 | snii_frq_prelude_test.cpp / doris_be_test |
| 选择性 locate 解码块数 | 1 次 `locate_window`（落第 3 块）后计数 | 旧=63 | `== 2`（末块+第3块）；再 locate 末块仍 `==2`（缓存幂等） | 是 | 同上 |
| 全量遍历解码块数（不退化） | 逐 w `window(w)` 后计数 | — | `== n_super_blocks()`（每块恰解一次，无重复） | 是 | 同上 |
| 空/单窗解码块数 | open 后计数 | — | 空 `==0`；单窗 `==1` | 是 | 同上 |
| build 字节不变 | `ByteSink::buffer()` 逐字节比对 golden | 改前字节 | 完全相等 | 是（位级） | 同上 |
| prelude 解析 wall-clock | google benchmark `benchmark_snii_frq_prelude.hpp`（`-DBUILD_BENCHMARK=ON` RELEASE） | 旧 eager 全解 | report-only：选择性场景解析耗时下降 | 否（仅报告，非门禁） | be/benchmark |

第 7 节以确定性"解码块数"操作计数 + 位级 build 回归为主，wall-clock 仅 report-only。`perf_tests_deterministic=true`。

## 8. 验收标准

- `[功能]` `FrqPreludeReader` open+逐窗 `window(w)` 字段与参考输入逐字段相等（FP-EQ，`EXPECT_EQ`）；`locate_window` 跨块边界全部命中正确、超末窗 found=false（FP-LOC）。验证手段：doris_be_test `SniiFrqPreludeTest.*`。
- `[功能]` 空/单窗/非整除分块/无 freq 全部正确（FP-EMPTY/ONE/PARTIAL/NOFREQ）；header crc 损坏在 open 报 `Corruption`，window 行损坏在**访问期**报 `Corruption`（FP-CRC/CORRUPT-ROW/CORRUPT-OOB）。
- `[性能-确定性]` N=4000/G=64 时 `open` 后 `decoded_super_block_count()==1`（基线 63）；单次选择性 `locate_window` 后 `==2`；全量遍历后 `==n_super_blocks()`。
- `[性能-确定性]` `build_frq_prelude` 输出字节级不变（FP-BUILD-GOLD）。
- `[格式]` 零在盘字节变更（build 路径未改，crc 覆盖范围不变）。
- `[并发]` N/A —— FrqPreludeReader 全部 request-scoped，无共享可变状态、无锁；头注释固化"禁止置于共享 reader"不变量。
- 命令：`./run-be-ut.sh --run --filter='SniiFrqPreludeTest.*'` 全绿；`be-code-style` 通过。

## 9. 风险与回滚

**正确性风险**：
- (R1) **跨 super-block contiguity 校验降级**（验证器 caveat 2）：原 `validate_region_layout` 全局扫描 dd/freq contiguity 是 open 期唯一对 window-row offset 的整体完整性守卫（trailing crc 不覆盖 window 行）。惰性化后改为"块内 contiguity（ensure 内）+ 访问期 `windowed_posting.cpp:79-86,132-137` InBounds + per-region crc_dd/crc_freq（实读时）+ 调用方长度交叉校验（`docid_posting_reader.cpp:171` prefix.size 必须 == prelude_len+dd_block_len；`windowed_posting.cpp:58-59` 块长 ≤ frq_region_len）"。缓解：这些既有交叉校验能捕获绝大多数损坏；FP-CORRUPT-ROW/OOB 覆盖访问期检测。残余风险为"in-bounds 但非 contiguous（重叠/空洞）且未被任何区域读触达"的损坏不再在 open 期报错 —— 属 LOW，已记入 gaps。
- (R2) **dd_block_len/freq_block_len 仅由末块派生**（trust contiguity）：若末窗 dd_off 被篡改，块长错误。缓解：调用方长度交叉校验（同上）会因实际区段长度不匹配而报 Corruption。
- (R3) **跨块 win_base 链重建**：依赖 resident `sb_last_docid_[sb-1]` 作为块首 prev_last，必须与原顺序解码语义逐位等价。FP-EQ 全量等价性测试守卫；ensure 内保留 `prev_last==sb_spans_[sb].last_docid` 块尾校验。

**线程安全风险**：`mutable` 缓存仅在 per-query 单线程下安全。缓解：头注释固化"FrqPreludeReader 必须 request-scoped、禁止置于共享 LogicalIndexReader"；已 grep 确认现状满足。若未来误用为共享，将出现数据竞争 —— 通过 code review + 注释约束防护（本任务不引入共享，故不需 TSAN 门禁）。

**格式风险**：无（build 未改，FP-BUILD-GOLD 守卫）。

**回滚**：纯 reader 局部改动，单文件 `frq_prelude.cpp` + 头。回滚 = 恢复 `windows_` 全量字段、`open` 调 `decode_all_blocks`+`validate_region_layout`、删除惰性缓存成员与 seam 访问器即可，调用方零改动，无需数据迁移。
