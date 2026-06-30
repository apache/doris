## T14 — prx 窗口自动模式避免双重编码

### 1. 目标与背景

**问题（finding F12，MEDIUM，confirmed）**：在 auto 模式（`kAutoZstd = -1`，`logical_index_writer.cpp:42`，为每个 .prx 窗口使用，调用点 `:126` 与 `:298`）下，`build_prx_window_flat`（`prx_pod.cpp:666-685`）对**每一个** .prx 窗口都会完整构建两份 payload：

- PFOR payload —— `encode_pfor_payload_flat`（`prx_pod.cpp:681`，内部 `:131-154`：遍历所有位置、推导 per-doc delta、跑 `check_flat_partition` + ascending 校验）。
- raw varint 明文 —— `encode_payload_flat`（`prx_pod.cpp:683`，内部 `:78-97`：**再次**遍历所有位置、**再次**推导相同的 delta、**再次**跑 `check_flat_partition` + ascending 校验）。

随后 `write_auto_pfor_or_zstd`（`prx_pod.cpp:245-257`）仅当 `plain_payload.size() >= kAutoZstdMinBytes(512)`（`:246`）时才使用明文去尝试 zstd；否则直接 `write_pfor`（`:255`），**明文被整体丢弃**。

**收益**：对占多数的小窗口（Zipfian 语料里绝大多数 distinct term 的 df 很小 → 明文 <512B），可完全省掉第二遍位置遍历、第二次 delta 推导、第二次 ascending/partition 校验，以及为丢弃用的 `plain` ByteSink 的**每窗堆分配**（验证器评估：跨数百万稀有 term 窗口，throwaway ByteSink 分配是更主要的成本）。同样的双重编码也存在于 vector 版 `build_prx_window`（`prx_pod.cpp:659-663`）。

**预期量级**：验证器结论为「真实但适中、单位数百分比级的 build-CPU/alloc 改进，非热点级跃变」。本任务为 reader/writer-only，零在盘字节变更。

### 2. 影响的文件/函数

文件：`be/src/storage/index/snii/format/prx_pod.cpp`，头 `be/src/storage/index/snii/format/prx_pod.h`。

当前签名/函数（匿名命名空间内除签名外均为 static）：
- `Status build_prx_window_flat(std::span<const uint32_t> positions_flat, std::span<const uint32_t> freqs, int zstd_level_or_negative_for_auto, ByteSink* sink)`（`:666`）—— 主要修改点（生产路径）。
- `Status build_prx_window(std::span<const std::vector<uint32_t>> per_doc_positions, int, ByteSink*)`（`:642`）—— 镜像修改（legacy/test 路径）。
- `Status encode_pfor_payload_flat(span flat, span freqs, ByteSink* out)`（`:131`）。
- `Status encode_payload_flat(span flat, span freqs, ByteSink* out)`（`:78`）。
- `Status check_flat_partition(span flat, span freqs)`（`:45`）。
- `size_t varint32_size(uint32_t)`（`:216`，**已存在**，复用于精确明文长度计算）。
- `Status write_auto_pfor_or_zstd(Slice pfor_payload, Slice plain_payload, ByteSink* sink)`（`:245`）—— 不改，仍用其精确的 zstd-vs-pfor frame-size 比较。

调用方（不改签名，行为不变）：`logical_index_writer.cpp:126`、`:298`。

### 3. 变更设计

**核心思路（采纳验证器的精确-尺寸纠正，保证字节级一致）**：把 per-doc delta 只推导一次进可复用缓冲，从该缓冲做 PFOR 编码；用**精确**的 varint 字节数（而非粗估）判定是否跨越 512 阈值；仅当精确明文长度 `>= kAutoZstdMinBytes` 时才真正物化 raw 明文 ByteSink 去尝试 zstd。小窗口完全跳过 raw 物化。

新增匿名命名空间 helper：

```cpp
// 一次性推导 per-doc delta（含 partition + ascending 校验）。
Status compute_flat_deltas(std::span<const uint32_t> flat, std::span<const uint32_t> freqs,
                           std::vector<uint32_t>* deltas);   // 复用 check_flat_partition

// 从已算好的 deltas 直接写 PFOR payload（与 encode_pfor_payload_flat 字节一致）。
void encode_pfor_payload_from_deltas(std::span<const uint32_t> freqs,
                                     std::span<const uint32_t> deltas, ByteSink* out);

// 不物化明文、纯算术得到 raw 明文 payload 的精确字节数。
// = varint32_size(doc_count) + Σ varint32_size(fc) + Σ varint32_size(delta)
size_t exact_plain_payload_size(std::span<const uint32_t> freqs, std::span<const uint32_t> deltas);

// 从已算好的 deltas 直接写 raw 明文（与 encode_payload_flat 字节一致，省第二遍 delta 推导）。
void encode_payload_from_deltas(std::span<const uint32_t> freqs,
                                std::span<const uint32_t> deltas, ByteSink* out);
```

`build_prx_window_flat` auto 分支改为：

```cpp
std::vector<uint32_t> deltas;
SNII_RETURN_IF_ERROR(compute_flat_deltas(positions_flat, freqs, &deltas));
ByteSink payload;
encode_pfor_payload_from_deltas(freqs, deltas, &payload);
const size_t plain_size = exact_plain_payload_size(freqs, deltas);
if (plain_size >= kAutoZstdMinBytes) {
    ByteSink plain;
    encode_payload_from_deltas(freqs, deltas, &plain);     // 仅大窗物化
    doris::snii::format::testing::note_prx_raw_build();           // 测试计数 seam
    return write_auto_pfor_or_zstd(payload.view(), plain.view(), sink);
}
write_pfor(payload.view(), sink);
return Status::OK();
```

vector 版 `build_prx_window` auto 分支：先把 `per_doc_positions` 扁平成局部 `flat/freqs`（与 `encode_pfor_payload`（`:157-165`）现有扁平逻辑一致），再走上面同一序列，保证两路输出仍字节一致。

**测试计数 seam（shared_infra）**：在 `prx_pod.h` 末尾新增（遵循 §4 `dict_decode_counter()` 约定）：

```cpp
namespace doris::snii::format::testing {
uint64_t prx_raw_build_count();   // 读取
void reset_prx_raw_build_count(); // 测试间清零
void note_prx_raw_build();        // 物化 raw 明文时 +1（实现用 std::atomic<uint64_t>）
}
```
原子计数，仅在 auto 路径物化 raw 明文时自增；非测试构建可忽略其开销（一次 relaxed 原子加）。

**FORMAT-COMPATIBILITY 结论**：字节级不变。
- `plain_size < 512`：改前 `write_auto_pfor_or_zstd` 因 `:246` 不满足而 `write_pfor`；改后直接 `write_pfor` —— 同一 PFOR 帧。
- `plain_size >= 512`：改后仍物化真实明文并调用 `write_auto_pfor_or_zstd`，其内部 zstd-vs-pfor frame-size 比较逻辑（`:249-250`）原样保留。
- 关键不变量：`exact_plain_payload_size` 必须等于 `encode_payload_flat` 实际产出字节数（两者都是 `put_varint32(doc_count)` + 每 doc `put_varint32(fc)` + 每 delta `put_varint32`），由边界测试保护。
- `encode_pfor_payload_from_deltas` 产出与 `encode_pfor_payload_flat` 逐字节一致。
- ascending/partition 校验不丢失：`compute_flat_deltas` 保留与 `encode_pfor_payload_flat:144-145` 等价的检查并恒定执行。reader（`read_prx_window*`，按存储 codec 字节分派）无需改动。

**CONCURRENCY 结论**：writer-only，所有 payload/delta 缓冲均函数局部，无共享可变状态，N/A。唯一全局可变是测试计数 seam（`std::atomic`），仅 build 写路径触碰；writer 的 segment 构建为单线程，原子计数不引入数据竞争。不触及 §5 所述共享 `LogicalIndexReader` 路径。

### 4. 依赖

- `depends_on`：无。`varint32_size`（`:216`）已存在可直接复用；不依赖 T19 `resize_uninitialized`（本任务缓冲是 `push_back` 累积，不是 resize-then-overwrite）。
- 本任务**提供** shared_infra：`doris::snii::format::testing` 的 prx raw-build 计数 seam（后续 prx 相关任务可复用）。

### 5. TDD 步骤（RED → GREEN → REFACTOR）

新测试文件：`be/test/storage/index/snii_prx_pod_test.cpp`（GLOB 自动纳入 `doris_be_test`，无需改 CMake），套件名 `SniiPrxPodTest` / `SniiPrxPodCounterTest`。

**Step 0（infra，先落计数 seam）**：在 `prx_pod.h`/`.cpp` 加入 `testing::{prx_raw_build_count,reset_prx_raw_build_count,note_prx_raw_build}`，并在**现有** auto 路径物化明文处（`build_prx_window_flat:683` 之后）调用 `note_prx_raw_build()`。此时行为仍是「每窗都物化」。

**Step 1（RED — 小窗跳过）**：写 `TEST(SniiPrxPodCounterTest, SmallWindowSkipsRawPlaintextBuild)`：构造一个明文 <512B 的小窗（如 3 doc、每 doc 2 个位置），`reset_prx_raw_build_count()` 后调用 `build_prx_window_flat(..., kAutoZstd, &sink)`，断言 `prx_raw_build_count() == 0`。当前代码恒物化 → 实际为 1 → **FAIL**。

**Step 2（GREEN）**：实现 §3 的 `compute_flat_deltas` / `encode_pfor_payload_from_deltas` / `exact_plain_payload_size` / `encode_payload_from_deltas`，改写 `build_prx_window_flat` auto 分支：仅当 `plain_size >= kAutoZstdMinBytes` 才物化明文并 `note_prx_raw_build()`。小窗计数归 0 → GREEN。补 `TEST(SniiPrxPodCounterTest, LargeWindowStillBuildsRawPlaintextOnce)`（明文 >=512B → 计数 ==1）。

**Step 3（RED — 字节一致 / 边界 codec）**：写 `TEST(SniiPrxPodTest, FlatAutoProducesByteIdenticalOutputAcrossSizes)`（flat 路径 vs per-doc 路径逐字节相等，含跨 512 的大窗）与 `TEST(SniiPrxPodTest, AutoCodecChoiceMatchesBruteForceAtThreshold)`（独立用 `pfor_frame_size`/`zstd_frame_size` 重算，断言发射的 codec 字节一致）。在仅改了 flat、未改 vector 路径的中间态，大窗用例会暴露不一致 → FAIL。

**Step 4（GREEN）**：把 vector 版 `build_prx_window` auto 分支也改为「扁平→`compute_flat_deltas`→同序列」，两路恢复字节一致 → GREEN。

**Step 5（REFACTOR）**：让旧 `encode_pfor_payload_flat`/`encode_payload_flat` 复用 `compute_flat_deltas`（或保留供 forced 分支调用，避免改 forced 路径行为）；清理重复 delta 推导；`be-code-style` 跑 clang-format。所有测试保持 GREEN，不改测试。

### 6. 功能验证

测试 target：`doris_be_test`（文件 `be/test/storage/index/snii_prx_pod_test.cpp`）。

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| F-RT-small | 3 doc，位置 `{{1},{3,5},{2}}`（明文<512） | flat+freqs | `build_prx_window_flat` 后 `read_prx_window_csr` 往返 | 解码 per-doc 位置 `EXPECT_EQ` 原始 | 小窗正确性 + codec=pfor 路径 |
| F-RT-large | 280 doc 高频位置使明文>=512 | flat+freqs | build→`read_prx_window` 往返 | 解码 `EXPECT_EQ` 原始 | 大窗 + zstd/pfor 择优路径 |
| F-EQUIV | 随机若干窗口（含跨 512） | 同数据两种入参 | `build_prx_window_flat` vs `build_prx_window(per_doc)` | 输出 `take()` **逐字节相等** | flat==vector 字节一致（新旧路径等价） |
| F-CODEC-512 | 精心构造明文恰=511 与=512 两窗 | flat+freqs | build 后读首字节 codec | codec 字节 == 独立 brute-force（比较 `pfor_frame_size` vs `zstd_frame_size`）所选 | 阈值边界 codec 不漂移 |
| F-EMPTY | doc_count=0（freqs 空、flat 空） | 空 span | build→read 往返 | 返回 OK，解码空，计数==0 | 退化/空输入 |
| F-SINGLE | 1 doc 1 位置 | flat+freqs | build→read | 往返 `EXPECT_EQ` | 单元素边界 |
| F-ERR-asc | doc 内位置非升序 `{5,3}` | flat+freqs | `build_prx_window_flat` | 返回 `Status::InvalidArgument`（"ascending"） | 错误路径不丢失校验 |
| F-ERR-part | `sum(freqs) != flat.size()` | 不一致 span | `build_prx_window_flat` | 返回 `Status::InvalidArgument`（partition） | partition 校验保留 |
| F-NULL | sink=nullptr | — | build | 返回 `Status::InvalidArgument` | 空参防御 |

必含：边界（空/单/跨阈值）、错误路径（升序/partition/null 返回 Status 错误码）、正确性等价（flat==vector + 往返解码）。

### 7. 性能验证（单体）—— 确定性优先

测试 target：`doris_be_test`，文件 `be/test/storage/index/snii_prx_pod_test.cpp`。

| 指标 | 隔离手法 | 基线 | 断言/阈值 | 确定性 | 测试文件/target |
|---|---|---|---|---|---|
| 小窗 raw 明文物化次数 | `testing::reset_prx_raw_build_count()` + `prx_raw_build_count()` seam | 改前=1（每窗必物化） | 单个 <512B 窗口 build 后 `== 0` | 是 | `SniiPrxPodCounterTest.SmallWindowSkipsRawPlaintextBuild` |
| 大窗 raw 明文物化次数 | 同上 | — | 单个 >=512B 窗口 build 后 `== 1`（按需物化，不多不少） | 是 | `SniiPrxPodCounterTest.LargeWindowStillBuildsRawPlaintextOnce` |
| 批量小窗物化总次数 | 连续 build N 个小窗，计数 reset 一次 | 改前=N | `== 0` | 是 | `SniiPrxPodCounterTest.ManySmallWindowsBuildZeroRawPlaintext` |
| 输出字节一致性 | flat vs per-doc 双路 `take()` 比对 | 改前字节 | **逐字节相等**（含跨 512 窗） | 是 | `SniiPrxPodTest.FlatAutoProducesByteIdenticalOutputAcrossSizes` |
| 阈值 codec 不漂移 | 独立重算 frame-size | 改前 codec 字节 | codec 字节相等 | 是 | `SniiPrxPodTest.AutoCodecChoiceMatchesBruteForceAtThreshold` |
| index-build 吞吐（positions-heavy 字段） | Google Benchmark 骨架 `benchmark_snii_prx_*.hpp`（`-DBUILD_BENCHMARK=ON`+RELEASE） | 改前 ns/window | report-only | 否（wall-clock） | 非 CI 门禁 |

性能门禁全部由确定性断言（物化计数 + 字节一致）承担；wall-clock 仅 report-only，因真实加速依赖语料 df 分布、不适合做门禁（理由见 gaps）。

### 8. 验收标准

- `[功能]` 全部 F-* 用例在 `./run-be-ut.sh --run --filter='SniiPrxPod*'` 下 GREEN；往返解码 `EXPECT_EQ` 原始位置（F-RT-small/large/single/empty）。
- `[功能]` 非法输入 F-ERR-asc/part/null 返回对应 `Status::InvalidArgument`。
- `[性能-确定性]` 小窗 `prx_raw_build_count() == 0`、大窗 `== 1`、批量 N 小窗 `== 0`（seam 计数）。
- `[性能-确定性]` flat 路径与 per-doc 路径输出**逐字节相等**，跨 512 阈值 codec 字节与 brute-force 一致 → 证明零在盘格式变更。
- `[格式]` 无在盘字节变更（由字节一致用例机验）；reader 路径未改。
- `[并发]` N/A —— 无共享可变 reader 状态；仅新增 test-only 原子计数，writer 单线程，不引入回归（验证手段：代码评审 + 计数 seam 为 `std::atomic`）。

### 9. 风险与回滚

- **风险 R1（codec 漂移，验证器 CAVEAT）**：若用粗估明文尺寸替代精确长度，512 边界附近可能翻转 codec 选择，破坏字节一致。**缓解**：本设计用 `exact_plain_payload_size`（逐 varint 精确求和，与 `encode_payload_flat` 产出字节严格相等），并由 F-CODEC-512 边界测试守护。
- **风险 R2（ascending/partition 校验丢失）**：第二遍编码被跳过可能漏掉校验。**缓解**：`compute_flat_deltas` 恒执行 `check_flat_partition` + ascending（等价 `prx_pod.cpp:144-145`），F-ERR-asc/part 覆盖。
- **风险 R3（两路不一致）**：仅改 flat 未改 vector 会导致 F-EQUIV 失败。**缓解**：Step 4 同步修改 vector 路径。
- **风险 R4（线程安全）**：测试计数全局原子，仅 build 路径触碰，writer 单线程；非测试场景可视作零行为影响。
- **回滚**：纯 writer 侧逻辑变更，无格式/无 reader 影响。回滚只需将 `build_prx_window_flat`/`build_prx_window` auto 分支还原为「无条件 `encode_pfor_payload_flat` + `encode_payload_flat` + `write_auto_pfor_or_zstd`」并移除 helper 与计数 seam；因输出字节一致，回滚不影响任何已写出的索引。