> 约束：以下计划严格遵循 §1 章节顺序、§2 TDD 纪律、§3-§4 验证强制项、§5 并发红线、§7 命名、§8 编码规范。所有 finding（F23/F29/F30/F36/F41/F43）均为 LOW，验证器一致结论：冗余 zero-fill 真实存在且在热路径上，但量级被高估，且“删一行”不可行（std::vector 必须被 size 到 n）。本计划据此把“可实现的主收益（warm-reuse memset 消除）”作为强制交付，把“冷增长消除（需改公共签名/类型 ripple）”降级为可延后 Batch B。

# 1. 目标与背景

## 1.1 问题
SNII 解码热路径在 PFOR/zstd 解码前对“随后会被全量覆写的缓冲”做一次 O(n) 的 value-initialization（zero-fill），属纯冗余内存写：

- **F23/F29/F36/F41**（`prx_pod.cpp:111-118`）：`decode_pfor_runs()` 先 `out->assign(n, 0)`（整缓冲 memset），随后 `pfor_decode()` 对每个 run 全量写入 `out->data()+off`。验证器逐路径核实：`bitunpack`（`pfor.cpp:250-292`）对 w==0 走 `memset`、w1..w8/generic+tail 对每个 i 写 `out[i]`，`pfor_decode` 的异常回填（`pfor.cpp:325-334`）只 patch 已写槽位 —— 故 `assign` 的清零从不被观测，是死写。命中 MATCH_PHRASE 位置验证路径：`phrase_query.cpp:458/460` → `read_prx_window_csr` → `decode_pfor_payload_csr`（`prx_pod.cpp:305` 解 pos_off、`:310` 解 pos_flat=total_pos）。
- **F43**（`frq_pod.cpp:43-50`）：同款 `decode_pfor_runs` 复制体，命中每个 dd/freq 窗口解码：`decode_dd_region`（`frq_pod.cpp:163`）、`decode_freq_region`（`:189`），被 phrase/scoring/docid 路径调用。`decode_dd_region` 随后还有前缀和回写（`frq_pod.cpp:167-172`），即同一缓冲被 3 趟遍历而 2 趟足矣。
- **F30**（`zstd_codec.cpp:20-30`）：`zstd_decompress()` 先 `out->resize(expected_uncomp_len)`（对全新/增长缓冲会 zero-fill），随后 `ZSTD_decompress` 全量写并校验 `n == expected_uncomp_len`（`zstd_codec.cpp:26`）。命中 .prx zstd 窗口（`prx_pod.cpp:702/722/743`）、frq region（`frq_pod.cpp:118`）、dict-block（`logical_index_reader.cpp:101`）。

## 1.2 验证器纠正（必须尊重，已并入设计）
- 量级被高估：被消除的是 3 趟遍历中最便宜的一趟（纯流式 memset，带宽受限），bitunpack 的移位/掩码与前缀和的依赖链才是瓶颈；窗口数据为 256/1024-doc 量级（KB 级，不是 64M cap）。结论：真实但低收益，单体可验。
- “删一行”不可行：`assign`/`resize` 同时承担 **size 到 n** 的职责，删掉会让 `out->data()+off` 越界。
- std::vector 无原生“无初始化 resize”：要真正跳过 value-init 必须用 default-init allocator 或 C++23 `resize_and_overwrite`（本仓 C++20，不可用）。`resize()` 仅在 **缓冲被复用且当前 size>=n** 时（warm-reuse）才省掉 memset；从 0 增长仍 zero 新尾。
- 并发（F43 关键）：`LogicalIndexReader` 被 InvertedIndexSearcherCache 跨并发查询共享。任何“跨窗口复用的 scratch”必须 per-query/per-thread，**绝不可**挂到共享 reader 上。

## 1.3 预期收益
统一一个 `doris::snii::resize_uninitialized` 原语（§8 强制），把 3 处“resize-then-overwrite”收敛到单一可审计 seam；在 warm-reuse 热缓冲（`PosChunkDecoder` 复用的 pos_flat、跨窗口复用的 docid/freq scratch）上消除每窗口一次 memset；对冷路径零回归。冷增长消除与 dict-block 路径降级为可延后（见 gaps）。

# 2. 影响的文件/函数（精确签名）

**新增**
- `be/src/storage/index/snii/common/uninitialized_buffer.h`（头文件，header-only）

**修改（reader/writer-only，零在盘字节）**
- `be/src/storage/index/snii/format/prx_pod.cpp`
  - `Status decode_pfor_runs(ByteSource* src, size_t n, std::vector<uint32_t>* out)`（`:111`，`out->assign(n,0)` @ `:112`）
  - `Status decode_pfor_payload_csr(Slice plain, std::vector<uint32_t>* pos_flat, std::vector<uint32_t>* pos_off)`（`:291`，删冗余 `pos_flat->reserve(total_pos)` @ `:309`；**保留** `pos_off->reserve(doc_count+1)` @ `:304`，F36 验证器明确：`:325` 的 `push_back(next_off)` 依赖它）
  - （可选清理）`decode_selected_pfor_count_ranges` / `decode_selected_pfor_positions` 的 `std::array<uint32_t,kFrqBaseUnit> run_buf {}`（`:403`/`:432`）的 `{}`
- `be/src/storage/index/snii/format/frq_pod.cpp`
  - `Status decode_pfor_runs(ByteSource* src, size_t n, std::vector<uint32_t>* out)`（`:43`，`out->assign(n,0)` @ `:44`）
- `be/src/storage/index/snii/encoding/zstd_codec.cpp`
  - `Status zstd_decompress(Slice input, size_t expected_uncomp_len, std::vector<uint8_t>* out)`（`:20`，`out->resize(...)` @ `:21`）

**新增测试**
- `be/test/storage/index/snii_uninitialized_buffer_test.cpp`（原语单测，GLOB 自动纳入 `doris_be_test`，无需改 CMake）
- 在 `be/test/storage/index/snii_prx_pod_test.cpp` / `snii_frq_pod_test.cpp`（若不存在则新建，命名沿用 §7 `SniiPrxPodTest` / 新增 `SniiFrqPodTest` / `SniiZstdCodecTest`）补解码等价 + warm-reuse 用例。

# 3. 变更设计

## 3.1 原语（`snii/common/uninitialized_buffer.h`）
提供两层能力，均 header-only、无状态、纯函数（线程安全）：

(A) **通用 seam（§8 强制，本任务实际落地）** —— 作用于既有 `std::vector<T>` 缓冲，不改任何调用方类型：
```cpp
namespace doris::snii {
// Resize a vector of trivially-copyable T to `n` without an extra zero-fill pass
// beyond what std::vector mandates. The caller MUST fully overwrite [0, n) before
// reading any element (PFOR/zstd decode guarantee this). For a warm-reused buffer
// whose current size() >= n this performs NO value-initialization (resize shrinks
// in place); for a cold/grown buffer std::vector still value-initializes the new
// tail — that is unavoidable for std::vector and is documented as such.
template <class T>
inline void resize_uninitialized(std::vector<T>& v, std::size_t n) {
    static_assert(std::is_trivially_copyable_v<T>);
    v.resize(n);
}
}
```
语义要点：把分散的 `assign(n,0)` / `resize(n)` 收敛成单一意图明确的入口；消除 `assign(n,0)` 在 warm-reuse 时的整缓冲 memset（`assign` 即使 size 不变也重写全部；`resize` 在 size>=n 时不写）。对 zstd（本已用 `resize`）为纯重命名/语义统一，零行为变化。

(B) **真·无初始化容器（提供给可迁移缓冲；本任务作基础设施 + 单测消费者，公共签名迁移延后到 Batch B）**：
```cpp
namespace doris::snii {
template <class T>
struct default_init_allocator : std::allocator<T> {
    template <class U> struct rebind { using other = default_init_allocator<U>; };
    template <class U, class... Args>
    void construct(U* p, Args&&... args) {
        if constexpr (sizeof...(Args) == 0) {
            ::new (static_cast<void*>(p)) U;     // default-init: NO zeroing for trivial U
        } else {
            ::new (static_cast<void*>(p)) U(std::forward<Args>(args)...);
        }
    }
};
template <class T> using uninitialized_vector = std::vector<T, default_init_allocator<T>>;

template <class T>
inline void resize_uninitialized(uninitialized_vector<T>& v, std::size_t n) {
    v.resize(n);   // grow path does default-init == no zeroing for trivial T
}
}
```
此重载在“冷增长”时也不清零（default-init 对 trivial T 为空操作），是 F30/F36/F43 期望的彻底版本。本任务先提供并单测，公共解码缓冲的迁移因 ripple/收益（全 LOW）延后。

## 3.2 落点
- `prx_pod.cpp:112`、`frq_pod.cpp:44`：`out->assign(n, 0);` → `doris::snii::resize_uninitialized(*out, n);`
- `zstd_codec.cpp:21`：`out->resize(expected_uncomp_len);` → `doris::snii::resize_uninitialized(*out, expected_uncomp_len);`
- `prx_pod.cpp:309`：删 `pos_flat->reserve(total_pos);`（F36 验证器：冗余；`pos_flat` 随后被 `resize_uninitialized`/`resize` size 到 `total_pos`，reserve 多此一举；`pos_off` 的 `reserve(doc_count+1)` 保留）。
- 三个 .cpp 各 `#include "storage/index/snii/common/uninitialized_buffer.h"`。

## 3.3 FORMAT-COMPATIBILITY 结论
**reader/writer-only，零在盘字节变更。** 仅改内存解码暂存的初始化方式，编码/在盘布局与所有 CRC 不变（解码输出位级不变，详见 §6 等价用例）。非 T18。

## 3.4 CONCURRENCY 结论
**无共享可变状态（针对原语本身）。** `resize_uninitialized` / `default_init_allocator` 无状态、纯函数。所触缓冲全为 per-query/per-thread-local：`decode_pfor_runs` 的 `out` 来自 `PosChunkDecoder` 的 per-verifier 成员（`phrase_query.cpp:521`）或调用方栈局部；`zstd_decompress` 的 `out` 为调用方私有缓冲（dict-block resident 缓冲在 open 阶段单线程填充，on-demand 走查询局部）。**不引入任何挂在共享 `LogicalIndexReader` 上的跨窗口复用 scratch**（F43 红线）。无锁、无 IO-under-lock 变化（§5）。本任务不新增 per-reader 可变状态，共享 reader 仍为 const 无锁只读。

# 4. 依赖
- **provides**：`snii/common/uninitialized_buffer.h`（§8 规定的全局 decode-buffer 原语），供后续任何 decode 缓冲优化复用。
- **depends_on**：无。本任务为基础设施型 polish（Batch 5）。

# 5. TDD 步骤（RED → GREEN → REFACTOR）

## 步骤 1 —— 原语：无初始化语义（RED→GREEN）
- RED：先写 `snii_uninitialized_buffer_test.cpp`：
  - `UninitVectorGrowSkipsZeroFill`：建 `uninitialized_vector<uint32_t> v`，`resize_uninitialized(v,N)`，用 sentinel 0xAA 填满，`v.clear()`（容量保留），再 `resize_uninitialized(v,N)`；通过 `const unsigned char*`（访问对象表示，标准允许、无 UB）读回，断言再生长后字节仍为 0xAA（证明无清零）。对照 `std::vector<uint32_t>` 同操作字节为 0x00。
  - `ResizeUninitializedShrinkKeepsCapacity`：plain `std::vector`，先 resize 大、记录 `capacity()`，`resize_uninitialized` 到更小，断言 `capacity()` 不变（warm-reuse 无 realloc）。
  - 编译期 `static_assert` 路径用 `EXPECT` 间接覆盖（trivially_copyable）。
  - 此时头文件不存在 → 编译失败（RED）。
- GREEN：实现 `uninitialized_buffer.h`（§3.1），测试转 GREEN。

## 步骤 2 —— PFOR 解码等价（RED→GREEN）
- RED：在 `snii_prx_pod_test.cpp` / `snii_frq_pod_test.cpp` 写解码等价用例（见 §6 FV-1..FV-6），先以“golden = 当前 assign 版输出（手工构造期望向量 / round-trip 原始输入）”断言。改实现前若用断点桩或先把期望写成预期值，此步主要锁定 baseline；真正 RED 来自步骤 3 引入 seam 后若实现写错则 FAIL。
- GREEN：把 `assign(n,0)` 换成 `resize_uninitialized`（`prx_pod.cpp:112`/`frq_pod.cpp:44`），等价用例保持 GREEN（位级一致）。

## 步骤 3 —— zstd 解码等价 + warm-reuse（RED→GREEN）
- RED：写 `SniiZstdCodecTest.DecompressByteIdentical`（往返一致）、`SniiZstdCodecTest.ReusedBufferNoRealloc`（同 buffer 连续两次解压，第二次 `capacity()` 不变）。先跑确认通过/失败基线。
- GREEN：`zstd_codec.cpp:21` 换 `resize_uninitialized`，断言不变（纯语义统一，行为等价）。

## 步骤 4 —— warm-reuse 不泄露 + 删冗余 reserve（RED→GREEN）
- RED：`SniiPrxPodTest.CsrReuseLargeThenSmallNoStaleTail`：同一 `pos_flat`/`pos_off` 先解大窗口再解小窗口，断言小窗口结果集与独立解码完全一致（`EXPECT_EQ` 全量），且 `pos_flat.size()` 等于小窗口 total_pos（不暴露旧尾）。
- GREEN：删 `prx_pod.cpp:309` 的 `pos_flat->reserve`，用例保持 GREEN。

## 步骤 5 —— REFACTOR
- 统一三处注释引用同一原语；去掉 `run_buf {}` 的 `{}`（可选，附注释说明 `pfor_decode`/`pfor_skip` 全量覆写 [0,run_len)、且只读 [0,run_len)）；跑 `be-code-style`（clang-format）。
- 全量 `doris_be_test --gtest_filter='Snii*'` 回归确保 phrase/term/segment 既有套件不回归。

# 6. 功能验证（gtest target：`doris_be_test`；GLOB 自动纳入）

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| FV-1 | 构造已知 per-doc positions（含 w==0 全相等、含异常值 outlier 触发 PFOR exception、含多 run n>256） | 编码后的 prx PFOR 窗口 | `read_prx_window_csr` 解码 | pos_flat/pos_off `EXPECT_EQ` 与原始输入逐元素相等（位级等价：新 resize_uninitialized 路径 == 旧 assign 路径） | PFOR 全宽度覆写正确性；等价 |
| FV-2 | n==0（空窗口）、n==1（单元素）、n==256（恰一 run）、n==257（跨 run 边界） | 各 case 一个窗口 | 解码 | 结果集与期望全量相等；空窗口返回 OK 且 size==0 | 边界/退化 |
| FV-3 | freqs 全为相同值（PFOR w==0 路径） | dd/freq region | `decode_dd_region`/`decode_freq_region` | docids/freqs `EXPECT_EQ` 期望；前缀和后 docids 升序正确 | w==0 死写消除后正确 |
| FV-4 | zstd 压缩的已知字节串（含空、含 64KB 量级） | disk slice + 正确 uncomp_len | `zstd_decompress` | out 字节与原文 `EXPECT_EQ`；长度==expected | zstd 等价 |
| FV-5（corrupt） | zstd 错误的 expected_uncomp_len / 截断输入；prx pos_count 和不匹配（`prx_pod.cpp:308`）；total_pos 超 cap | 非法输入 | 解码 | 返回非 OK `Status`（`Corruption`），不读未初始化、不崩溃 | 错误路径 |
| FV-6（warm-reuse） | 同一 pos_flat/pos_off 缓冲 | 先大窗口后小窗口 | 连续两次 `read_prx_window_csr` | 第二次结果 `EXPECT_EQ` 与独立解码一致；`size()`==小窗口 total_pos（旧尾不泄露） | warm-reuse 安全性 |
| FV-7（原语） | uninitialized_vector vs std::vector | regrow + sentinel | 见 §5 步骤1 | uninit 版字节保留 0xAA、std 版为 0x00 | 无初始化语义 |

# 7. 性能验证（单体）—— 确定性优先

| 指标 | 隔离手法 | 基线 | 断言/阈值 | 是否确定性 | 测试文件/target |
|---|---|---|---|---|---|
| 解码输出位级一致 | 同一编码字节，比较解码结果向量 | 旧 assign 版输出 | 改前后逐元素/逐字节 `EXPECT_EQ`（FV-1/FV-3/FV-4） | 是 | `snii_prx_pod_test.cpp` / `snii_frq_pod_test.cpp` / `doris_be_test` |
| value-init/zero-fill 消除（原语级） | `uninitialized_vector` regrow 后经 `const unsigned char*` 读对象表示 | std::vector regrow 字节==0 | uninit 版字节 == 预填 sentinel（证明无清零）；std 版 ==0（对照） | 是（字节级，FV-7） | `snii_uninitialized_buffer_test.cpp` |
| warm-reuse 无 realloc | 复用同缓冲连续两次解码，记录 `capacity()` | 第一次 capacity | 第二次 `capacity()` 不变（resize 复用存储，不重分配/不重清零路径生效） | 是（capacity 计数，FV-6 / zstd ReusedBufferNoRealloc） | 同上 |
| 冗余 reserve 移除 | 读码 + 单测 | `prx_pod.cpp:309` 存在 reserve | 删除后 FV-1/FV-6 仍全绿；`pos_off` 仍 reserve(doc_count+1)（无 push_back 触发的 realloc，capacity 稳定） | 是 | `snii_prx_pod_test.cpp` |
| 每窗口 memset 趟数（report-only） | Google Benchmark 微基准 `benchmark_snii_pfor_decode.hpp`，对比 assign vs resize_uninitialized 解码 256/1024-doc 窗口的 ns/op + bytes_written | 旧 assign 版 wall-clock | 仅记录、非门禁（带宽型，验证器确认绝对收益小） | 否（wall-clock） | `benchmark_test`（`-DBUILD_BENCHMARK=ON`，RELEASE，默认关闭） |

说明：本任务为内存带宽型微优化，**确定性门禁**由“位级等价 + 原语无初始化字节证明 + warm-reuse capacity 稳定”三项构成；wall-clock 仅 report-only（§4）。

# 8. 验收标准（可勾选、可机验）

- [功能] `read_prx_window_csr` 对含 w==0/异常值/多 run 的窗口解码结果与原始输入逐元素相等（`EXPECT_EQ`，FV-1）；验证手段：`doris_be_test --gtest_filter='SniiPrxPodTest.*'`。
- [功能] 边界 n∈{0,1,256,257} 解码正确（FV-2）；空/截断/和不匹配/超 cap 输入返回非 OK `Status` 不崩溃（FV-5）。
- [功能] `decode_dd_region`/`decode_freq_region`（含 w==0）输出与期望相等（FV-3）；`zstd_decompress` 往返字节一致（FV-4）。
- [功能] warm-reuse（大→小）第二次解码结果与独立解码全量一致且 `size()` 正确、无旧尾泄露（FV-6）。
- [性能-确定性] `uninitialized_vector` regrow 后对象表示字节保留 sentinel（== 无 zero-fill），对照 `std::vector` 为 0（FV-7）；验证手段：`snii_uninitialized_buffer_test.cpp` 字节断言。
- [性能-确定性] 复用缓冲第二次解码 `capacity()` 不变（warm-reuse 无 realloc / 无重清零路径）；验证手段：capacity 断言。
- [性能-确定性] PFOR/zstd 解码输出在 `assign→resize_uninitialized` 改动前后位级相等（等价回归，FV-1/FV-3/FV-4）。
- [格式] 无在盘字节变更：所有既有 `Snii*` 套件（phrase/term/segment/adapter）全绿；CRC 校验不变。验证手段：`./run-be-ut.sh --run --filter='Snii*'`。
- [并发] N/A 升级说明：原语无共享可变状态；不新增 per-reader 可变状态；不在锁内做 IO/解压（§5 红线未触及）。无需 TSAN 新增门禁（未引入共享状态），但回归运行 `BUILD_TYPE_UT=TSAN ./run-be-ut.sh --run --filter='SniiPhraseQueryTest.*'` 确认 phrase 路径解码无新增告警。
- [构建] `be-code-style` 通过；`doris_be_test` 编译链接通过。

# 9. 风险与回滚

## 风险（含验证器/CONCURRENCY.md 提示）
- **R1 未初始化读取（正确性）**：原语前提是“[0,n) 在读前被全量覆写”。已逐路径核实 `pfor_decode`/`bitunpack`（含 w==0 memset、异常仅 patch 已写槽）与 `ZSTD_decompress`+长度校验满足该不变量。缓解：在 `resize_uninitialized` 与两处 `decode_pfor_runs` 加注释声明该契约；`static_assert(is_trivially_copyable)`；FV-1..FV-7 等价/边界用例守护。**禁止**对“可能短路某个 run 而不写”的未来改动套用本原语。
- **R2 std::vector 冷增长无收益的误解**：验证器明确 `resize()` 仅 warm-reuse 省 memset。计划已据实标注主收益为 warm-reuse，冷增长零回归（不优于亦不劣于现状），避免过度承诺。
- **R3 删 reserve 引入 realloc（F36）**：仅删 `pos_flat` 的冗余 reserve（随后被 resize 到 total_pos），**保留** `pos_off` 的 `reserve(doc_count+1)`（`:325` push_back 依赖）。用例 FV-1/FV-6 守护。
- **R4 并发（F43 红线 / CONCURRENCY.md H1）**：共享 `LogicalIndexReader` 跨并发查询。本任务**不**引入任何挂在共享 reader 上的复用 scratch，所有受影响缓冲 per-query/per-thread；resident dict-block 缓冲在 open 单线程填充。故不引入数据竞争，不触发 H1（无新增锁/解压-under-lock）。
- **R5 default_init_allocator 误用**：(B) 容器目前仅作基础设施 + 单测消费者；若后续误把它用于“非 trivially-copyable 且依赖零初始化”的类型会读到不确定值。缓解：`resize_uninitialized` 重载与文档限定 trivial T，使用点 code review。
- **R6 格式风险**：无。reader/writer-only，零在盘变更，CRC 不变（R 由位级等价用例兜底）。

## 回滚
- 单提交内完成。回滚 = `git revert`：删 `uninitialized_buffer.h`、把三处 `resize_uninitialized` 还原为 `assign(n,0)`/`resize(...)`、恢复 `prx_pod.cpp:309` 的 reserve。无在盘/接口签名变更，回滚零迁移成本、无数据兼容影响。
