## T21 — CRC32C 三路交织硬件指令

### 1. 目标与背景

**问题（finding F31，LOW，encoding-codecs）**：`be/src/storage/index/snii/core/src/encoding/crc32c.cpp:69-84` 的 `crc32c_hw()` 用单个 `_mm_crc32_u64` 累加器每轮折叠 8 字节，且本轮结果喂给下一轮（`crc = _mm_crc32_u64(crc, v)`，`:73`），形成 loop-carried dependency。SSE4.2 的 `crc32` 指令延迟 ~3 cycle、吞吐 1/cycle，串行链使其实际只跑到 ~2.7 B/cycle（~8 GB/s），而非吞吐上限 ~8+ B/cycle。

**该 CRC 在查询/打开路径被反复调用**（F31:11、验证器已逐处确认）：
- `dict_block.cpp:113` `verify_crc` 对 ~64KB block（去掉 footer）做 `crc32c(*covered)`，block open 时执行。
- `frq_pod.cpp:108` `open_region` 对 region 做 `crc32c(disk)`，**仅** `meta.verify_crc` 为真（即 pod_ref 较大项）时执行；inline 项（slim<=256B 内联）跳过（`frq_pod.cpp:105-107` 注释）。
- `prx_pod.cpp:634` `read_framed` 对每个 framed prx window（256-doc 窗口，通常几 KB）在**每次** `read_prx_window*`（phrase 位置解码）时做 `crc32c`。

**预期收益**：在足以摊销 setup/combine 的较大缓冲（dict block、大 prx/frq region）上 CRC 吞吐 ~2-3x（F31 finder/验证器一致）。**验证器纠正**：端到端只是低个位数百分比——dict block CRC 被缓存摊薄且被 zstd 解压主导（zstd ~1-2 GB/s << CRC 路径占 block-open ~15%），小项不付 frq CRC，prx 窗口里 CRC 只是 PFOR/zstd/intersection 之外的一项。故本任务为**低优先级微优化**，主交付物是“正确等价 + 优化已启用”的确定性测试，吞吐用 report-only 微基准佐证。

**核心安全结论**：结果值逐字节不变（同一 bit-reflected Castagnoli 多项式），**零在盘格式变更**，纯函数**线程安全**。

### 2. 影响的文件/函数

仅一个实现文件 + 一个头：
- `be/src/storage/index/snii/core/src/encoding/crc32c.cpp`
  - `crc32c_slice8(uint32_t, const uint8_t*, size_t)`（`:48`，软件路径，保留不动）
  - `crc32c_hw(uint32_t, const uint8_t*, size_t)`（`:69-84`，现串行硬件路径，保留为小缓冲/尾部路径，可改名 `crc32c_hw_serial`）
  - `detect_sse42()` / `kHasSse42`（`:86-92`，保留）
  - `crc32c_extend(uint32_t, Slice)`（`:97-109`，**dispatcher**，新增对大缓冲走 hw3）
- `be/src/snii/encoding/crc32c.h`（`:10-14` 公开 `crc32c_extend`/`crc32c`，签名不变；新增 `snii::detail` 测试可见的子路径声明，见 §3）

调用方（均不改签名，调用 `crc32c()`/`crc32c_extend()` 不变）：`dict_block.cpp:91/113`、`frq_pod.cpp:108`、`prx_pod.cpp:634`、`bsbf.cpp`、`tail_pointer.h`、`section_framer.cpp`、`bootstrap_header.cpp`、`per_index_meta.cpp` 等（grep 已确认全部经由 `crc32c.h` 的公开入口）。

### 3. 变更设计

**算法：标准 3-way 交织硬件 CRC32C（rocksdb/folly/Intel 同款）**

在 `crc32c.cpp` `#if SNII_CRC32C_X86` 区内新增 `crc32c_hw3()`：
1. 当 `n >= kInterleaveThreshold` 时，取每路块长 `L = (n/3) & ~size_t(7)`（8 字节对齐，保证 `_mm_crc32_u64` 路径整除），三路独立累加器：
   - `crcA = crc32c_hw_serial(crc, p,        L)`（带入参 seed）
   - `crcB = crc32c_hw_serial(0,   p+L,      L)`
   - `crcC = crc32c_hw_serial(0,   p+2L,     L)`
2. **combine（CRC 线性性）**：`crc(s, X||Y) = crc(0, Y) XOR shift(s, len(Y))`，其中 `shift(c, k bytes) = c · x^(8k) mod P`（bit-reflected 域）。故
   - `comb = crc32c_shift(crcA, L) ^ crcB;`
   - `comb = crc32c_shift(comb, L) ^ crcC;`
3. 剩余尾部 `tail = n - 3L` 字节用 `crc32c_hw_serial(comb, p+3L, tail)` 串行收尾。
4. `n < kInterleaveThreshold` 直接 `crc32c_hw_serial(crc, p, n)`。

**combine 实现 `crc32c_shift(uint32_t crc, size_t bytes)`——纯查表/矩阵，不引入 PCLMULQDQ**（避免额外 CPUID gate，遵 F31 验证器注意点(1)）：
- 用 GF(2) 32×32 矩阵（zlib `crc32_combine` 同构）：基算子 `op1` = “前进 1 个 0 字节”的矩阵（由 `kPoly` 在 static init 构建，与 `make_slice8_table` 同一处初始化）。`crc32c_shift` 用反复平方在 O(log(8·bytes)) 步内得到 `x^(8·bytes)` 算子并作用于 `crc`。每次调用最多 2 次 shift，矩阵运算成本相对 L 字节处理可忽略。基算子表常量在 namespace 匿名作用域 `const` 静态初始化，线程安全（C++11 静态初始化 + immutable 后只读）。

**阈值 `kInterleaveThreshold`**：取 `1024` 字节（遵 F31 验证器注意点(2)：小缓冲 setup+combine 占比大，inline prx/小 pod_ref region 常很小，必须保留串行路径）。该常量在 §5 无并发影响；可调，作为 `crc32c_interleave_threshold()` 暴露给测试。

**dispatcher（`crc32c_extend`）**：
```
crc = ~crc;
#if SNII_CRC32C_X86
  if (kHasSse42) { crc = crc32c_hw3(crc, p, n); return ~crc; }   // hw3 内部按阈值回落 serial
#endif
crc = crc32c_slice8(crc, p, n);
return ~crc;
```

**测试可见 seam（不污染热路径，无 thread_local 计数）**：在 `crc32c.h` 新增
```cpp
namespace snii::detail {
  uint32_t crc32c_slice8_extend(uint32_t crc, Slice data);     // 强制软件路径
  uint32_t crc32c_hw_serial_extend(uint32_t crc, Slice data);  // 强制串行硬件（无 SSE4.2 时回落 slice8）
  uint32_t crc32c_hw3_extend(uint32_t crc, Slice data);        // 强制 3-way（无 SSE4.2 时回落 slice8）
  size_t   crc32c_interleave_threshold();
  bool     crc32c_has_hw();
}
```
这些是对内部函数的薄封装（含 `~crc` 包裹），让 UT **直接逐路调用做逐字节等价对比**与“阈值>0/has_hw”断言，无需对生产热路径做任何 instrumentation。

**FORMAT-COMPATIBILITY**：reader/writer-only，零在盘变更——CRC 数值对同一多项式逐字节不变，旧索引存储的 CRC 仍校验通过（F31 验证器明确）。

**CONCURRENCY**：`crc32c*` 全为纯函数，无共享可变状态；静态查表/矩阵常量在程序启动 immutable 初始化后只读。**N/A，无共享可变状态**——与 CONCURRENCY.md 的 H1/H2 无关（不触及 shared LogicalIndexReader 状态、不持任何锁、无 IO/解压）。

### 4. 依赖
- depends_on：无。纯自包含实现文件改动。
- 不需要 T19 `resize_uninitialized`（combine 用栈上固定数组，无 resize-then-overwrite 缓冲）。
- 提供给他人：更快的 `crc32c_extend`，对所有现有调用方透明（dict_block/frq_pod/prx_pod/bsbf/tail_pointer 等自动受益）。
- shared_infra（可选）：be/benchmark Google Benchmark 骨架用于 report-only 吞吐微基准。

### 5. TDD 实施步骤（RED → GREEN → REFACTOR）

新增测试文件 `be/test/storage/index/snii_crc32c_test.cpp`（`storage/*.cpp` GLOB 自动纳入 `doris_be_test`，无需改 CMake），套件名 `SniiCrc32cTest` / `SniiCrc32cPerfTest`。

**RED-1（等价基线，先失败）**：写 `TEST(SniiCrc32cTest, Hw3MatchesSlice8AcrossSizes)`——对 size ∈ {0,1,7,8,9,15,16,255,256,1023,1024,1025,3072,4096,65536} 与多个起始 alignment 的伪随机缓冲，断言 `detail::crc32c_hw3_extend(0,s) == detail::crc32c_slice8_extend(0,s)` 且 `== crc32c(s)`。此时 `detail::*` 与 `crc32c_hw3` 尚不存在 → **编译失败（RED）**。

**GREEN-1**：在 `crc32c.cpp` 实现 `crc32c_hw3` + `crc32c_shift` + GF(2) 基算子初始化，在 `crc32c.h` 加 `snii::detail` 封装；`crc32c_extend` dispatcher 接 hw3。跑测试转 **GREEN**。

**RED-2（增量种子/拼接等价）**：写 `TEST(SniiCrc32cTest, Hw3ExtendEqualsConcatenation)`——验证 `crc32c_extend(crc32c(A), B) == crc32c(A||B)`（覆盖 seed 链 + combine 线性性），并对跨阈值长度（如 |A||B|=4097）断言。先因边界 off-by-one（如 `L` 未 8 对齐、tail 处理）可能 **FAIL**。

**GREEN-2**：修正 `L = (n/3) & ~7` 与 tail 收尾逻辑直到 PASS。

**RED-3（已知向量 + 阈值/路径启用）**：`TEST(SniiCrc32cTest, KnownVectorsAndStrategyEngaged)`——断言标准 CRC32C 测试向量（如 ASCII "123456789" → `0xE3069283`）；断言 `detail::crc32c_interleave_threshold() > 0`；当 `detail::crc32c_has_hw()` 为真时，对一个 >= threshold 的缓冲断言 `crc32c(s) == detail::crc32c_hw_serial_extend(0,s)`（证明 dispatcher 选择的 hw3 与权威串行路径一致 = 优化已启用且正确）。

**GREEN-3**：补齐已知向量常量；若 has_hw 路径有偏差则修。

**REFACTOR**：抽出 `crc32c_hw_serial`（由旧 `crc32c_hw` 改名）供 hw3 复用；矩阵工具函数 `gf2_matrix_times`/`gf2_matrix_square` 提为匿名 namespace 小函数；clang-format（`be-code-style`）。重跑全部测试保持 GREEN，断言改前后不变（纪律：改实现不改测试）。

### 6. 功能验证（gtest target：`doris_be_test`，文件 `be/test/storage/index/snii_crc32c_test.cpp`）

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| FV-1 | 伪随机缓冲，size 全集 {0,1,7,8,9,15,16,255,256,1023,1024,1025,3072,4096,65536}×多 alignment | 各缓冲 | `crc32c_hw3_extend` vs `crc32c_slice8_extend` vs `crc32c` | 三者逐字节 `EXPECT_EQ`（新路径==旧路径，正确等价） | 等价性、阈值上下边界、对齐 |
| FV-2 | 退化输入 | n=0 空 slice、n=1、n=7（<8 无整 64bit）、n=3072（恰 3×1024）| 各路径 | 全部 `EXPECT_EQ` 且 n=0 返回 `crc32c("")` 初值 | 空/极小/恰整除边界 |
| FV-3 | 拼接 A,B | `crc32c_extend(crc32c(A),B)` | 与 `crc32c(A||B)` 比较，含跨阈值长度 4097 | `EXPECT_EQ` | seed 链 + combine 线性性 |
| FV-4 | 标准向量 | "123456789"、"" 、"a"×N | `crc32c` | `EXPECT_EQ(crc32c("123456789"),0xE3069283u)` 等 | 与外部权威值一致（绝对正确性） |
| FV-5 | corrupt 检测回归 | 取一缓冲算 CRC 后翻转任意 1 bit | 比较两 CRC | `EXPECT_NE`（CRC 仍能区分单 bit 改动） | 校验功能未退化 |
| FV-6 | 路径启用 | size>=threshold 缓冲 | `crc32c` vs `crc32c_hw_serial_extend` + `threshold()>0` | `EXPECT_EQ` 且 `EXPECT_GT(threshold,0)` | 优化已接入且与权威串行一致 |
| FV-7 | 现有 reader 黑盒回归 | 复用 `snii_query_test.cpp` `build_reader()`+`MemoryFile` 写一含 dict block/prx/frq 的小索引 | 跑 `phrase_query`/term 查询 | 结果集 `EXPECT_EQ` 改前后不变（CRC 校验全程通过，无 `Status::Corruption`） | 端到端调用方（dict_block/prx_pod/frq_pod）回归 |

### 7. 性能验证（单体）

| 指标 | 隔离手法 | 基线 | 断言/阈值 | 是否确定性 | 测试文件/target |
|---|---|---|---|---|---|
| 正确等价（bit-identity） | 直接逐路调用 `detail::crc32c_hw3_extend` vs `slice8`/`hw_serial`，size/alignment 全集（FV-1/2/3） | slice8 权威值 | 三路逐字节相等（含 0/极小/65536/跨阈值） | **是（bit-identity）** | snii_crc32c_test.cpp / doris_be_test |
| 优化已启用 | `detail::crc32c_interleave_threshold()` + `crc32c_has_hw()`（FV-6） | — | `threshold>0`；has_hw 时 dispatcher 走 hw3 且==hw_serial | **是（路径/常量断言）** | 同上 |
| CRC 吞吐 (B/cycle 或 GB/s) | Google Benchmark 微基准，固定 64KB/8KB/2KB/512B 缓冲，hw3 vs hw_serial vs slice8 | hw_serial 当前实现 | 64KB/8KB 上 hw3/hw_serial ≈ 2-3x（**report-only，非门禁**） | 否（wall-clock） | be/benchmark/benchmark_snii_crc32c.hpp（`-DBUILD_BENCHMARK=ON`+RELEASE，默认关闭） |

**说明**：CRC 加速本质是指令级吞吐，无 fetch/decompress/alloc 计数可作确定性代理“更快”。CI 门禁只用前两行确定性断言（正确等价 + 优化已启用）；真实吞吐收益用 wall-clock 微基准佐证，按 §4 规范 report-only、永不作门禁。`perf_tests_deterministic=false`（见 gaps）。

### 8. 验收标准

- `[功能]` FV-1/FV-2/FV-3：`crc32c_hw3_extend` 与 `crc32c_slice8_extend`/`crc32c` 对全集 size×alignment 逐字节 `EXPECT_EQ`（手段：snii_crc32c_test.cpp，`doris_be_test`）。
- `[功能]` FV-4：`crc32c("123456789") == 0xE3069283`（外部权威向量，`EXPECT_EQ`）。
- `[功能]` FV-5：单 bit 翻转 `EXPECT_NE`，校验能力未退化。
- `[功能]` FV-7：复用 `build_reader()`+`MemoryFile` 的 phrase/term 查询结果集改前后 `EXPECT_EQ`，无 `Status::Corruption`（dict_block/prx_pod/frq_pod 调用方回归）。
- `[性能-确定性]` FV-6：`crc32c_interleave_threshold() > 0` 且 has_hw 时 dispatcher 路径 `== hw_serial`（优化已启用且正确）。
- `[性能-report-only]` 微基准 64KB hw3 相对 hw_serial ~2-3x（不作门禁，仅记录）。
- `[格式]` 旧索引（含已存储 CRC）经新 reader 校验全部通过（FV-7 覆盖）；零在盘字节变更。
- `[并发]` N/A——纯函数无共享可变状态，无锁、无 IO/解压；不触及 CONCURRENCY.md H1/H2。
- 全部 `SniiCrc32cTest.*` 绿；`be-code-style` clang-format 通过。
  运行：`./run-be-ut.sh --run --filter='SniiCrc32cTest.*'`。

### 9. 风险与回滚

- **正确性风险（主）**：combine 常量必须对应 bit-reflected Castagnoli（`kPoly=0x82F63B78`）；`L` 必须 8 字节对齐否则 `_mm_crc32_u64` 跨界。**缓解**：FV-1/2/3 在跨阈值与全 alignment 上对 slice8 做穷举等价，任何常量/对齐错误即 FAIL（F31 验证器明确要求 round-trip 等价测试）。
- **格式风险**：无——数值不变，旧 CRC 仍有效（F31 验证器确认）。
- **线程安全风险**：无——纯函数 + immutable 静态常量；不引入 thread_local 热路径计数（seam 用直调子函数实现）。不引入 PCLMULQDQ，故**不需要额外 CPUID gate**（遵 F31 验证器注意点(1)）。
- **收益不及预期风险**：端到端仅低个位数百分比、且被缓存/zstd 摊薄（F31 验证器）。本任务定位低优先级微优化，主价值是确定性正确性保障 + 在大缓冲上的明确吞吐改善，不承诺端到端大幅提升。
- **阈值风险**：阈值过低会让小缓冲付 combine 成本反而变慢。`kInterleaveThreshold=1024` 经 FV/微基准校准，且小缓冲始终回落 hw_serial。
- **回滚**：改动局限于 `crc32c.cpp` + `crc32c.h` 的 `detail` 声明。回滚即把 `crc32c_extend` dispatcher 改回直接调 `crc32c_hw_serial`（或恢复旧 `crc32c_hw`），删除 hw3/shift/矩阵代码与新测试文件，零调用方改动、零格式影响。
