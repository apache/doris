## 1. 目标与背景

**Finding 映射：F25 [LOW] (cross-cutting-systems)** — `be/src/storage/index/snii/core/src/format/frq_prelude.cpp:441` 的 `*out = windows_[w];` 是一次对调用方栈对象的整结构体拷贝（`WindowMeta` 经验证为 112 字节）。`window()` 位于独立 TU（frq_prelude.cpp），在无 LTO 下每次调用是一次跨 TU 的非内联调用 + 边界检查 + 112 字节 memcpy。

热点为四个 per-window 循环，对高 df（windowed，df>=512）词每查询调用 N 次（N≈doc_count/256，1M 文档词约 1000~4000 窗）：
- `docid_conjunction.cpp:631`（`collect_windowed_docids_only`，**已接入 Doris**：term/match/all/any 的 docid 过滤）
- `windowed_posting.cpp:129`（`windowed_window_range`）与 `:241`（`read_windowed_posting`，**已接入**：phrase / 窗口解码）
- `docid_posting_reader.cpp:181`（docs-only 全量解码，**已接入**）
- `scoring_query.cpp:97`（`BuildWindowBounds`）/`:357`（`MaterializeWindow`）/`:430`（`BuildLazyWindowed`）（**DEFERRED**：BM25 评分未接入 Doris）

**预期收益**：消除高 df 词每查询数千次 112 字节结构体拷贝；其中三处循环体内本就含 PFOR/zstd/区间切分/fetch，拷贝占比小；`BuildWindowBounds` 每窗工作量轻、拷贝占比相对显著但每评分词仅执行一次。整体为 query CPU/cache 的温和改进（F25 验证器评级 LOW，但修复 sound、零风险）。证据见 frq_prelude.cpp:441、frq_prelude.h:86-115（结构体）、frq_prelude.h:175（`windows_` 由 reader 拥有并在 `open()` 后不可变）。

## 2. 影响的文件/函数

**头文件** `be/src/snii/format/frq_prelude.h`
- 现有：`Status FrqPreludeReader::window(uint32_t w, WindowMeta* out) const;`（:157，拷贝语义，返回 `InvalidArgument` 越界）
- 现有：`std::vector<WindowMeta> windows_;`（:175，private，`open()` 后不可变）

**实现** `be/src/storage/index/snii/core/src/format/frq_prelude.cpp`
- `FrqPreludeReader::window`（:436-443，整结构体拷贝实现，保留不动）

**调用点（迁移到新访问器）**
- `docid_conjunction.cpp:629-631`：`WindowMeta meta; window(w,&meta);` 随后存入 `WindowWork.meta`（:646/:656 仍需拷贝进 work 结构）
- `docid_posting_reader.cpp:178-182`：纯读字段（`window_dd_slice`/`is_dense_full_window`/`first_docid_in_window`/`decode_window_slices`），不存副本 → 可直接用引用
- `windowed_posting.cpp:128-129`：纯读字段（`InBounds`/赋值 `out->dd_off=...`）→ 可直接用引用
- `windowed_posting.cpp:240-241`：拷贝进 `WindowSlices.meta`
- `scoring_query.cpp:96-97`（DEFERRED）：纯读 → 引用；`:356-357`/`:429-430`（DEFERRED）：纯读 → 引用

## 3. 变更设计

在 `FrqPreludeReader` 新增**头内联、零拷贝**只读访问器，返回指向 `windows_` backing 的 const 引用：

```cpp
// frq_prelude.h，紧邻 window() 声明之后：
// Zero-copy const view of window w's materialized metadata. The reference is
// valid for the reader's lifetime (windows_ is built at open() and immutable
// thereafter). Read-only per-window loops should prefer this over window().
// Bounds are the caller's contract: w < n_windows() (DCHECKed). Use window()
// when a bounds-checked Status is required.
const WindowMeta& window_at(uint32_t w) const {
    DCHECK_LT(w, windows_.size());
    return windows_[w];
}
```

要点：
- **内联在头**：消除跨 TU 调用 + 边界检查 + 112 字节 memcpy（验证器明确指出内联带来的不仅是去拷贝，还有去跨 TU 调用开销）。
- **保留 `window(uint32_t, WindowMeta*)`**：契约不变，留给需 `Status` 越界返回或未来需可变副本的调用方。
- **DCHECK 边界**：四个热点循环均以 `w < n_windows()`（或 `windows` 列表来自 prelude）为前置，已天然满足；release 下零开销。
- 调用点改写：
  - 纯读字段处（docid_posting_reader/windowed_posting:129/scoring 三处）改为 `const WindowMeta& meta = prelude.window_at(w);`，循环体引用字段，零拷贝。
  - 存副本处（docid_conjunction `WindowWork.meta`、windowed_posting:241 `WindowSlices.meta`）改为 `f.meta = prelude.window_at(w);` —— 由原来的"两次拷贝（window()→local，local→struct）"降为"一次拷贝"（验证器确认）。

**FORMAT-COMPATIBILITY**：reader/writer-only，零在盘字节变更（纯内存访问器，不触碰序列化/反序列化路径）。

**CONCURRENCY**：`windows_` 在 `open()` 中完全物化、之后不可变（frq_prelude.cpp:432 后无写入）。`FrqPreludeReader` 随共享 `LogicalIndexReader` 缓存并被并发查询共享，但本访问器只返回指向不可变 backing 的 const 引用，不引入任何新的 per-reader 可变状态、无锁、无 IO、无解压。符合 §5"共享 `LogicalIndexReader` 现为 const 无锁只读"约束。**无 NO-IO-UNDER-LOCK 风险（不涉及任何锁）。**

## 4. 依赖

- **依赖**：无（F25 为独立改动，不依赖其他任务）。
- **提供**：`FrqPreludeReader::window_at` 作为后续 windowed 读/合并/评分循环可复用的零拷贝访问器（shared_infra）。
- 不依赖 T19 `resize_uninitialized`（本任务无解码缓冲）。

## 5. TDD 步骤（RED → GREEN → REFACTOR）

新建单测文件 `be/test/storage/index/snii_frq_prelude_test.cpp`（GLOB_RECURSE 自动纳入 `doris_be_test`，已在 be/test/CMakeLists.txt:24 确认，无需改 CMake）。测试通过 `build_frq_prelude(FrqPreludeColumns, ByteSink*)` + `FrqPreludeReader::open(Slice, &reader)` 直接构造 reader，无需走完整 segment fixture。

**Step 1 — RED（访问器不存在 → 编译失败）**
写 `TEST(SniiFrqPreludeTest, WindowAtMatchesCopyingWindow)`：构造含 N=5 窗（覆盖 has_freq/has_prx 两种 flags）的 prelude，对每个 w 断言 `window_at(w)` 的每个字段与 `window(w,&copy)` 返回的副本逐字段相等（`last_docid/win_base/doc_count/dd_*/freq_*/prx_*/max_freq/max_norm/verify_crc`）。当前因 `window_at` 未声明编译失败（RED）。

**Step 2 — GREEN（最小实现）**
在 frq_prelude.h 加入上述内联 `window_at`。重跑 Step 1，等价性测试转 GREEN。

**Step 3 — RED（零拷贝不变量）**
写 `TEST(SniiFrqPreludeTest, WindowAtReturnsStableReferenceWithoutCopy)`：
- `EXPECT_EQ(&prelude.window_at(2), &prelude.window_at(2));`（两次调用同一地址 → 未拷贝到新对象）
- `EXPECT_EQ(&prelude.window_at(3) - &prelude.window_at(2), 1);`（相邻窗地址差 1 → 指向 `windows_` 连续 backing，证明返回的是物化向量元素而非临时副本）
此测试在 GREEN 实现下应直接通过；若误实现为返回副本/局部，地址恒等断言失败。（先写测试确立不变量，再保证实现满足。）

**Step 4 — REFACTOR（迁移热点调用点）**
逐一改写 §2 列出的调用点为 `window_at`（纯读处用引用，存副本处单次拷贝）。每改一处 build + 跑既有 `snii_query_test.cpp` 全量（phrase/term/match/all/any/prefix/wildcard/regexp）确保结果集不变。**不改既有查询测试**（验证迁移行为等价）。

**Step 5 — REFACTOR（边界/损坏输入回归）**
补 `WindowAtBoundaryAndEmpty`（N=1、N=0 经 n_windows 守卫不调用）与确认 `window()`（拷贝重载）越界仍返回 `InvalidArgument`（契约未回退）。

## 6. 功能验证

测试 target：`doris_be_test`（文件 `be/test/storage/index/snii_frq_prelude_test.cpp`，套件 `SniiFrqPreludeTest`；迁移等价性复用既有 `SniiPhraseQueryTest`/`SniiTermQueryTest` 等 `snii_query_test.cpp`）。

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| FV1 WindowAtMatchesCopyingWindow | build_frq_prelude 造 N=5、has_freq=true、has_prx=true | 各 w∈[0,5) | 对比 `window_at(w)` 与 `window(w,&c)` | 全 14 字段 `EXPECT_EQ`（含 win_base/dd_*/freq_*/prx_*/max_freq/max_norm/verify_crc） | 新路径==旧路径等价 |
| FV2 WindowAtMatchesCopyingWindowDocsOnly | N=4、has_freq=false、has_prx=false | 各 w | 同 FV1 | freq_* 默认值一致、字段全等 | tier=kDocsOnly 分支等价 |
| FV3 WindowAtReturnsStableReferenceWithoutCopy | N=5 | w=2,3 | 取 `window_at` 地址 | `&window_at(2)==&window_at(2)`；`&window_at(3)-&window_at(2)==1` | 零拷贝、指向 backing |
| FV4 WindowAtBoundarySingle | N=1 | w=0 | `window_at(0)` | 字段与 `window(0,&c)` 全等 | 单元素边界 |
| FV5 EmptyPreludeNoWindows | N=0 | — | `n_windows()` | `==0`；不调用 window_at（守卫） | 空退化 |
| FV6 CopyingWindowOutOfRangeStillErrors | N=3 | w=3 | `window(3,&c)` | 返回 `InvalidArgument`（契约未回退） | 错误路径/契约保持 |
| FV7 QueryResultEquivalence | 复用 build_reader() 造 windowed 高 df 词 | 既有 phrase/term/all 用例 | 跑迁移后查询 | 结果集 `EXPECT_EQ` 与改前一致 | 调用点迁移端到端正确 |

## 7. 性能验证（单体）— 确定性优先

| 指标 | 隔离手法 | 基线 | 断言/阈值 | 是否确定性 | 测试文件/target |
|---|---|---|---|---|---|
| 零拷贝（无新对象） | 直接对 `FrqPreludeReader` 调 `window_at`，比较返回引用地址 | `window()` 每次写入新栈对象（地址必不同） | `&window_at(w)==&window_at(w)`（同地址）；相邻 `&window_at(w+1)-&window_at(w)==1` | 是 | snii_frq_prelude_test.cpp / doris_be_test (FV3) |
| 位级等价（行为不变） | 同一 reader 下 window_at vs window 逐字段 | 旧路径副本 | 全 14 字段 `EXPECT_EQ` | 是 | (FV1/FV2) |
| 查询正确性不回退 | build_reader() 高 df 窗化词 | 改前结果集 | 结果集逐元素 `EXPECT_EQ` | 是 | snii_query_test.cpp (FV7) |
| per-window 拷贝字节/CPU | Google Benchmark `benchmark_snii_frq_prelude.hpp`（新增 + `#include` 进 benchmark_main.cpp，仅 `-DBUILD_BENCHMARK=ON` RELEASE）：对 N=4096 窗循环 window() vs window_at() | wall-clock | report-only，**不作 CI 门禁** | 否（wall-clock） | be/benchmark |

理由：本优化是去拷贝 + 内联，确定性核心证据是"访问器返回 backing 引用（地址恒等/连续）"——它在单测中确定性证明了零拷贝；逐字段等价证明行为不变；既有查询测试证明迁移不改结果。逐调用点 CPU 节省（112B/窗）相对每窗解码工作量极小，只能用 report-only 微基准观测，按 §4 规范 wall-clock 永不作门禁。

## 8. 验收标准

- `[功能]` FV1/FV2：`window_at(w)` 与 `window(w,&c)` 全 14 字段相等（`EXPECT_EQ`，含 has_freq=true/false 两 tier）。
- `[功能]` FV4/FV5/FV6：单元素/空/越界（`window()` 仍返回 `InvalidArgument`）覆盖。
- `[功能]` FV7：迁移后 phrase/term/match/all/any 查询结果集与改前位级一致（`./run-be-ut.sh --run --filter='Snii*Test.*'` 全绿）。
- `[性能-确定性]` FV3：`&window_at(2)==&window_at(2)` 且 `&window_at(3)-&window_at(2)==1`（证零拷贝、指向 `windows_` backing）。
- `[格式]` 无在盘字节变更（reader-only；不触序列化路径，既有 `FrqPreludeReader::open` 损坏/截断测试不受影响）。
- `[并发]` 无新增可变状态，N/A 显式声明；无需 TSAN 专项（不涉及锁/IO/解压）。返回 const 引用对并发共享 reader 安全（`windows_` 不可变）。

## 9. 风险与回滚

- **正确性风险（低）**：`window_at` 用 DCHECK 而非 Status 边界检查。缓解：四个热点循环均以 `w < n_windows()` 或 `windows` 列表（源自 prelude）为前置，天然在界；保留 `window()` 拷贝重载供任何需 Status 越界的调用方。验证器确认无调用方修改返回的 meta（均为只读或单次拷贝进 work 结构）。
- **线程安全风险（无）**：CONCURRENCY.md 与 F25 验证器一致确认 `windows_` 在 `open()` 后不可变、共享 reader const 只读；返回 const 引用在 reader 生命周期内有效，对并发查询安全。无锁、无 H1/H2 相关 dict-block 缓存/single-flight 牵涉。
- **格式风险（无）**：纯内存访问器，零在盘变更，无需 flag bit / 版本 bump。
- **回滚**：改动局限于 frq_prelude.h 新增一个内联方法 + 各调用点改 `window()`→`window_at()`。回滚即删除 `window_at` 并把调用点改回 `WindowMeta x; window(w,&x);`，无数据迁移、无格式回退。DEFERRED 的 scoring 三处迁移若有疑虑可独立回退而不影响已接入路径。
