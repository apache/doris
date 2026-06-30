## 1. 目标与背景

PFOR 编码的宽度选择是 SPIMI build（flush/compaction）热点上的纯算法浪费，三处 finding 一致确认（均 MEDIUM、`需改格式: False`）：

- **F06 / F07 / F13**：`choose_width()`（`be/src/storage/index/snii/encoding/pfor.cpp:36-57`）先用一遍 O(n) 的 `bits_for` 求 `maxw`，再对每个候选宽度 `w∈[0,maxw]` **重新扫描全部 n 个值**调用 `bits_for(v[i])` 统计异常数 → O(maxw·n)，且 `bits_for`（`pfor.cpp:25-32`）本身是逐位 `while(v){++b;v>>=1;}` 移位循环。随后 `pfor_encode()`（`pfor.cpp:300-306`）**第三次**对每个值调用 `bits_for` 来切分异常。
- 调用链（确认真实命中 build 路径）：`pfor_encode` ← `encode_pfor_runs`（`be/src/storage/index/snii/format/frq_pod.cpp:34-40`、`prx_pod.cpp:102-108`）每 `kFrqBaseUnit=256` 元素一个 run，分别驱动 docid-delta（`build_dd_region`）、freq（`build_freq_region`）、prx pos_counts 与 position deltas（`encode_pfor_payload_flat`，`prx_pod.cpp:136,152`）。每个 term 的所有 postings/positions 值都过一遍，工作量随语料线性增长。
- 额外开销：`pfor_encode` 每个 run 都 `std::vector<uint32_t> low(values, values+n)`（`pfor.cpp:299`，最多 ~1KB 拷贝）+ `std::vector<pair> exc`（`pfor.cpp:298`）两次堆分配。

**预期收益**：把宽度选择从 O(maxw·n·bitwidth) + 一遍 maxw 扫描 + 一遍异常切分（合计每值约 `maxw+2` 次位宽求值）降为**单遍 O(n) + O(maxw) 后缀和**（每值恰 1 次位宽求值）。验证器校正：headline ~30x 是 maxw=32 的最坏情形，delta 数据典型 maxw~8-16，实际约 10-17x；且 build 还被 zstd 共同主导，故整库 wall-time 增益被稀释——这是 build-only CPU 改进，**非 query 路径**。

**关键约束（来自三份 finding 验证器）**：选出的宽度存于盘上（`put_u8(w)`，`pfor.cpp:307`），任何能解码的宽度都正确；只要保持**相同 cost 公式与相同 tie-break**，输出字节逐字节不变 → 零格式影响。

## 2. 影响的文件/函数

仅改写编码侧，单文件：
- `be/src/storage/index/snii/encoding/pfor.cpp`
  - `uint8_t bits_for(uint32_t v)`（`:25-32`，匿名命名空间）
  - `uint8_t choose_width(const uint32_t* v, size_t n)`（`:36-57`，匿名命名空间）
  - `void pfor_encode(const uint32_t* values, size_t n, ByteSink* out)`（`:296-316`）
- `be/src/storage/index/snii/encoding/pfor.h`：新增 test-only 计数 seam 的声明（`doris::snii::testing` 命名空间）。

解码侧（`pfor_decode`/`pfor_skip`/`bitunpack*`）**完全不动**。

## 3. 变更设计

### 3.1 单遍位宽求值（替代三遍 bits_for）

新增匿名命名空间辅助：
```cpp
// Bit-width of v, branch-light; bits_for(0)==0 must be preserved.
inline uint8_t value_width(uint32_t v) {
    return v ? static_cast<uint8_t>(32 - __builtin_clz(v)) : 0;
}
```
- **clz(0) 是 UB**（验证器 F07/F13 caveat (1)）：`v==0` 显式映射到 0，绝不写 `32-clz(v|1)`（那会把 0 误判为宽度 1，污染直方图）。
- `value_width` 与 `bits_for` 数值完全等价：`v∈[1,2^k)` → `32-clz(v)=k`；`v=0`→0；`v` 含 bit31 → 32。保留 `bits_for` 仅供需要时（实际可删，但保留供对照/计数 seam）。

### 3.2 直方图 + 后缀和的 choose_width

新签名（增加一个可复用的 per-value 宽度输出缓冲，供 `pfor_encode` 复用以消除第三遍）：
```cpp
// Fills widths[0..n) with value_width(v[i]); returns the chosen bit_width.
uint8_t choose_width(const uint32_t* v, size_t n, uint8_t* widths) {
    uint16_t hist[33] = {0};          // bucket b counts values with width==b
    uint8_t maxw = 0;
    for (size_t i = 0; i < n; ++i) {  // single O(n) pass
        uint8_t b = value_width(v[i]);
        widths[i] = b;
        ++hist[b];
        if (b > maxw) maxw = b;
    }
    // suffix_exc(w) = #values with width > w  (via running suffix sum)
    uint8_t best = maxw;
    size_t best_cost = SIZE_MAX;
    size_t exc_gt = 0;                 // exceptions for current w (width > w)
    // iterate w descending? must keep ASCENDING + strict '<' for identical tie-break.
    // Precompute suffix counts then iterate ascending:
    size_t suffix[34] = {0};          // suffix[k] = #values with width >= k
    for (int k = 32; k >= 0; --k) suffix[k] = suffix[k+1] + hist[k];
    for (uint8_t w = 0; w <= maxw; ++w) {
        size_t exc = suffix[w + 1];   // width > w  == width >= w+1
        size_t cost = (static_cast<size_t>(w) * n + 7) / 8 + exc * 6;
        if (cost < best_cost) {       // identical strict '<' → smallest w on tie
            best_cost = cost;
            best = w;
        }
    }
    return best;
}
```
- **cost 公式逐字节保持** `(w*n+7)/8 + exc*6`（验证器明确：保持此式与 `exc = #values with width>w` 才能选出同一宽度）。
- **tie-break 保持**：升序 `w` + 严格 `cost < best_cost` → 与原实现一样取达到最小 cost 的最小 `w`。
- `hist`/`suffix` 用栈数组（33/34 项），零堆分配。

### 3.3 pfor_encode：复用 widths，消除第三遍 + 减堆分配

```cpp
void pfor_encode(const uint32_t* values, size_t n, ByteSink* out) {
    // small-n stack buffer; falls back to a reused heap buffer only if n>256
    // (n is hard-capped at kFrqBaseUnit=256 by encode_pfor_runs, so stack path
    //  is always taken in practice).
    uint8_t widths_stack[256];
    std::vector<uint8_t> widths_heap;
    uint8_t* widths = widths_stack;
    if (n > sizeof(widths_stack)) { widths_heap.resize(n); widths = widths_heap.data(); }

    uint8_t w = choose_width(values, n, widths);
    out->put_u8(w);

    // First pass count exceptions to size varint; reuse cached widths (no 3rd bits_for).
    uint32_t n_exc = 0;
    for (size_t i = 0; i < n; ++i) n_exc += (widths[i] > w);
    out->put_varint32(n_exc);

    // Bit-pack low bits directly: exception positions contribute 0 (placeholder),
    // matching the old `low[i]=0`. Pass a predicate into bitpack instead of
    // materializing a `low` copy → removes the per-run `std::vector low` alloc.
    bitpack_masked(values, widths, n, w, out);

    // Exception table (index_delta, full_value), same order/format as before.
    uint32_t prev = 0;
    for (size_t i = 0; i < n; ++i) {
        if (widths[i] > w) {
            out->put_varint32(static_cast<uint32_t>(i) - prev);
            out->put_varint32(values[i]);
            prev = static_cast<uint32_t>(i);
        }
    }
}
```
- `bitpack_masked`：在 `bitpack` 基础上，对 `widths[i] > w` 的位置写 0、其余写 `v[i]&low_mask(w)`（异常位置低位本来就 < 2^w 不成立，故必须写 0 占位，与原 `low[i]=0` 字节一致）。这消除了 `std::vector<uint32_t> low` 的每-run 拷贝分配；`exc` 向量也被直接两遍流式输出取代，消除第二个分配。
- **字节等价性论证**：`w` 相同 → bit-packed 区相同；异常集合相同（`widths[i]>w` ⟺ `bits_for(v[i])>w`）→ 异常表的 `(index_delta, value)` 序列相同；`put_u8(w)`/`put_varint32(n_exc)` 相同。整体输出逐字节不变。

### 3.4 op-count seam（确定性性能断言用）

在 `pfor.cpp` 加 test-only 计数（默认零成本，单线程 build 路径，无并发顾虑）：
```cpp
// pfor.cpp
namespace { uint64_t g_width_evals = 0; }
namespace testing {
    uint64_t pfor_width_evals() { return g_width_evals; }
    void reset_pfor_width_evals() { g_width_evals = 0; }
}
// value_width 内 ++g_width_evals; （唯一计数点）
```
`pfor.h` 内 `namespace doris::snii::testing { uint64_t pfor_width_evals(); void reset_pfor_width_evals(); }`。
- 计数点唯一：所有 per-value 位宽求值都经 `value_width`。改后每个 run 恰 `n` 次（单遍直方图，`pfor_encode` 复用 `widths` 不再求值）；改前为 `n（maxw扫描）+ maxw_iter·n（choose 内每候选一遍，注意原是 bits_for 调用而非 value_width，故需在旧基线测试中临时也对 bits_for 计数对照——见 §5/§7）`。为可对照，统一在新实现下断言「== n」。
- **格式影响**：reader/writer-only，零在盘变更（非 T18）。
- **并发与锁影响**：N/A，无共享可变状态。`choose_width`/`pfor_encode` 是对调用方局部缓冲的纯函数，SPIMI writer 单线程/索引，计数变量仅 build 路径使用、测试间 reset；不触及共享 `LogicalIndexReader` 的 const 只读路径，与 CONCURRENCY.md 的 H1/H2 无关。

## 4. 依赖

- `depends_on`: 无。本任务自包含，仅改 `pfor.cpp`/`pfor.h`。
- 不依赖 T19 的 `resize_uninitialized`（本任务无 resize-then-overwrite 解码缓冲；`widths` 用栈数组）。
- **提供**：`doris::snii::testing::pfor_width_evals()` op-count seam，可作为后续 build-path 算法任务的确定性计数样板（列入 shared_infra）。

## 5. TDD 步骤（RED → GREEN → REFACTOR）

**Step 0（基线快照，GREEN 起点）**：在 `be/test/storage/index/snii_query_test.cpp` 既有 `SniiPforTest` 套件下，新增 `GoldenOutputForRepresentativeRuns`：用固定 RNG/构造的代表性输入集（见 §6 数据），跑 `pfor_encode` 把 `sink.buffer()` 复制为 golden 字节串常量（先用**当前**实现生成、内联进测试）。此测试在改动前 GREEN。

**Step 1（RED — 等价性失败保护）**：写 `HistogramWidthMatchesLinearScan`：对随机/边界输入，分别计算「新 `choose_width`」与一个测试内嵌的「朴素 O(maxw·n) 参考实现」选出的宽度，`EXPECT_EQ`。在尚未实现新 `choose_width` 时编译失败 → RED。

**Step 2（GREEN — 实现直方图）**：实现 §3.1/§3.2 的 `value_width` + `choose_width(…, widths)`。重跑 Step1 GREEN；重跑 Step0 golden 必须仍逐字节相等（证明宽度选择未变）。

**Step 3（RED — 消除第三遍 / 减分配）**：写 `WidthEvalsEqualsNPerRun`（op-count）：`reset_pfor_width_evals()` → `pfor_encode(run of n)` → `EXPECT_EQ(pfor_width_evals(), n)`。当前（仍三遍）会得到 `>n` → RED。

**Step 4（GREEN — pfor_encode 复用 widths + bitpack_masked）**：实现 §3.3。Step3 转 GREEN；Step0 golden 再次逐字节相等（异常表/bitpack 字节不变）。

**Step 5（REFACTOR）**：清理：删除/保留 `bits_for`（若仅 golden 对照测试用则移入测试），抽 `bitpack_masked` 与 `bitpack` 共用核心；跑 `be-code-style`。所有既有 `SniiPforTest.*`、`SniiPrxPodTest.*`（`snii_query_test.cpp:680-825`）保持 GREEN，证明 frq/prx round-trip 不回归。

纪律：改实现不改测试；每步自闭环（业务代码 + UT + 断言同批）。

## 6. 功能验证

gtest target：`doris_be_test`（GLOB 自动纳入 `be/test/storage/index/snii_*_test.cpp`，无需改 CMake）。用例落 `snii_query_test.cpp`，套件 `SniiPforTest`/`SniiPforPerfTest`。复用既有 `ByteSink`/`ByteSource` + `assert_ok`（`snii_query_test.cpp` 顶部已 include `snii/encoding/pfor.h`）。

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| FW-01 RoundTripRandom | 多组随机 u32（含 [0,255]、含偶发大值触发异常） n=256 | values | encode→decode | `decoded == values`（`EXPECT_EQ` 全量）& `source.eof()` | 正确性 |
| FW-02 WidthMatchesLinearScan | 随机 + 边界数据集 | values | 新 `choose_width` vs 测试内朴素参考实现 | 选出 `w` `EXPECT_EQ` | **新路径==旧路径**等价 |
| FW-03 GoldenByteIdentical | §Step0 代表性输入（全 0、全 1、单调 delta、freq-like 多数为 1、含 bit31 大值、混合异常） | values | `pfor_encode` | `sink.buffer()` == 内联 golden 字节串（逐字节 `EXPECT_EQ`） | **位级输出不变** |
| FW-04 AllZeros | `values=256×0` | values | encode→decode | 首字节 `w==0`；`decoded` 全 0；`n_exc==0` | 退化：clz(0) 守卫 |
| FW-05 SingleElement | `values={42}` n=1 | values | encode→decode | round-trip 相等 | 边界 n=1 |
| FW-06 EmptyRun | `n=0` | nullptr/empty | encode→decode | 不崩溃；首字节 `w==0`、`n_exc==0`；decode 写 0 值 | 边界 n=0 |
| FW-07 TopBitSet | 含 `0x80000000`（width=32）的值 | values | encode→decode | `decoded==values`；不触发 `w==32` 的移位 UB（验证器 caveat 2） | width=32 路径 |
| FW-08 AllException | 值跨度极大使最优 `w` 远小于 maxw（多数进异常表） | values | encode→decode | round-trip 相等 | 异常表大占比 |
| FW-09 SubRunTail | n=300（>256，经 `encode_pfor_runs` 分 256+44） | values | 经 `frq_pod` 或直接两 run | decode 相等 | 末尾不足 256 run |
| FW-10 CorruptExcIndex | 手工构造 exc index ≥ n 的字节流 | bad bytes | `pfor_decode` | 返回 `Status::Corruption`（不抛异常，`pfor.cpp:330`） | 错误路径 |

补充：保留并复跑既有 `SniiPforTest.LowBitWidthFastPathsRoundTrip`、`SniiPrxPodTest.SelectivePforCsr*` 作为集成回归。

## 7. 性能验证（单体）— 确定性优先

target：`doris_be_test`，套件 `SniiPforPerfTest`（确定性断言，可进 CI 门禁）。

| 指标 | 隔离手法 | 基线（改前） | 断言/阈值 | 是否确定性 | 测试文件/target |
|---|---|---|---|---|---|
| per-value 宽度求值次数/run | `doris::snii::testing::reset_pfor_width_evals()` + `pfor_width_evals()`（新 seam，唯一计数点 `value_width`） | 改前每 run ≈ `(maxw+2)·n` 次 `bits_for`（maxw 扫描 + choose 内 maxw 遍 + encode 切分） | `EXPECT_EQ(pfor_width_evals(), n)`（单 run）；多 run = Σ nᵢ = 总元素数 | 是 | `SniiPforPerfTest.WidthEvalsEqualsTotalValues` / `doris_be_test` |
| 编码输出字节 | golden 逐字节对比（FW-03） | 当前实现字节 | 改前后 `sink.buffer()` 位级相等 | 是 | `SniiPforTest.GoldenByteIdentical` |
| 每-run 堆分配次数 | `CountingAllocator` 注入 `ByteSink`/或对 `pfor_encode` 内部缓冲计数；优先断言不再构造 `std::vector low`（n≤256 走栈缓冲，0 次内部 vector 分配） | 改前每 run 2 次 vector 分配（`low`+`exc`） | 内部宽度/异常缓冲分配次数 `== 0`（n≤256） | 是 | `SniiPforPerfTest.NoPerRunHeapAllocForSmallRuns` |
| choose_width 候选-宽度内层扫描次数 | 计数 seam 推论（求值次数 == n 即证明不再有 maxw·n 内层重扫） | maxw·n | 由 WidthEvals==n 蕴含（无独立断言） | 是 | 同上 |
| build wall-time（report-only，非门禁） | Google Benchmark `benchmark_snii_pfor.hpp` + `#include` 进 `benchmark_main.cpp`，`-DBUILD_BENCHMARK=ON` RELEASE | 旧 choose_width | 仅报告 choose_width/pfor_encode μs 下降 | 否（report-only） | `benchmark_test` |

确定性断言占主导（宽度求值计数 + 位级 golden + 分配计数）。wall-clock 仅 report-only：理由——整库 build 受 zstd/IO 共同主导，验证器明确 headline 倍率被稀释，不可作门禁（见 gaps）。

## 8. 验收标准

- `[功能]` FW-01..FW-10 全 GREEN：`./run-be-ut.sh --run --filter='SniiPforTest.*:SniiPforPerfTest.*:SniiPrxPodTest.*'`。重点：FW-02 新==朴素参考宽度 `EXPECT_EQ`；FW-04 `w==0`；FW-07 width=32 无 UB；FW-10 返回 `Status::Corruption`。
- `[性能-确定性]` `SniiPforPerfTest.WidthEvalsEqualsTotalValues`：`pfor_width_evals()==Σnᵢ`（改前 >>，改后 ==）。`SniiPforTest.GoldenByteIdentical`：编码字节改前后逐字节相等。`NoPerRunHeapAllocForSmallRuns`：n≤256 内部缓冲分配 `==0`。
- `[格式]` 零在盘变更：golden 字节级不变即证明；`pfor_decode`/`pfor_skip` 未改、既有 round-trip 测试 GREEN。
- `[并发]` N/A（无共享可变状态）；无需 TSAN run。不触碰共享 reader const 路径，CONCURRENCY.md H1/H2 不受影响。
- 提交前 `be-code-style` 通过。

## 9. 风险与回滚

- **正确性（clz(0) UB）**：F07/F13 caveat (1)——`value_width` 必须显式 `v?…:0`。FW-04（全 0）专测。
- **正确性（w==32 移位 UB）**：F07 caveat (2)——异常切分用缓存的 `widths[i] > w` 比较，**不**用 `values[i]>>w`。FW-07（含 bit31）专测；`bitpack` 内已有的 `low_mask(w)`（`pfor.cpp:59-61`）对 w≥32 返回全 1，安全。
- **tie-break 漂移导致字节变化**：必须升序 `w` + 严格 `cost<best_cost` + cost 公式逐字节保持。FW-03 golden 是唯一可靠护栏；若 golden 失配立即说明宽度/异常逻辑偏离。
- **bitpack_masked 引入 bug**：风险点在「异常位置写 0 占位」。golden（FW-03）+ 全量 round-trip（FW-01/08）双重覆盖；若不放心可保守保留原 `low` 拷贝路径（仅去掉重复 bits_for，分配收益略减但零正确性风险）作为降级方案。
- **回滚**：单文件改动，`git revert` 即恢复 `choose_width`/`pfor_encode` 原状；解码侧从未改动，已写盘数据完全兼容（任何历史/新写宽度都解码一致）。op-count seam 为 test-only，移除不影响生产。
