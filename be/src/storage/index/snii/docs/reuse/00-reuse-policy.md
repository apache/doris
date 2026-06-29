# SNII 复用/解耦 政策（Reuse / Decouple Policy）

## 一、根本原则（Authoritative，源自作者）

1. **CLucene 完全解耦**：SNII 的索引格式、存储、查询路径中**不得出现任何 CLucene**。SNII core（`be/src/snii`）当前已是 CLucene-free（0 引用）。CLucene 仅允许残留在 **Doris 集成层**（`lucene::analysis::Analyzer` 分词器、一处 `lucene::store::Directory*` 基类签名形参、`CLuceneError` 捕获）——这是「耦合到 Doris」的副产物，不是「耦合到 CLucene 的字节路径」。

2. **优先复用 Doris 自有实现**：凡 Doris 已有等价能力，默认复用。SNII **不是**独立 / 可移植库——`be/src/storage/CMakeLists.txt` 用 `GLOB_RECURSE` 直接把 core 编进 Doris storage 库，与 doris 同一编译/链接单元。「耦合到 Doris 是被期望的，耦合到 CLucene 不是」。**仅当** Doris 实现对 SNII 明确次优时才保留 SNII 自有代码，且必须**具体论证**次优来源，不能泛泛而谈。

## 二、判定规则（Decision Rubric）

对每个组件按以下顺序判定：

### Step 1 — 是否触碰在盘字节？（on_disk gate）
- **否** → 进入 Step 2（纯内存/接口层，无格式红线）。
- **是** → 进入 Step 3（受红线约束，需字节等价证明）。

### Step 2 — 非在盘组件：API/性能/依赖三问
判定 `reuse-doris`，除非命中以下任一「次优」证据（命中则 `keep-snii-doris-suboptimal`）：
- **API 不契合**：Doris 等价物的语义/调用约定与 SNII 用法错配，迁移需在每个 use-site 重写（如 R02 doris::Slice 可变语义 vs SNII 只读解码；R09 read_at 单点 vs read_batch 合并契约）。
- **性能退化**：Doris 实现引入 SNII 热路径不可接受的开销（如 R04 解压每次走 std::mutex 上下文池，与 SNII const/无锁只读读端冲突）。
- **字节语义不符**：SNII 解码依赖 `const uint8_t*` 无符号语义，Doris 用 `char*`（有符号、移位/比较易符号扩展 bug）。
- **依赖重量**：复用会把 Doris 重头文件（profile / block_file_cache / S3 committer / pipeline exec 依赖）拽进本应轻量、CLucene-free、可独立测试的 SNII 核心。
- **特例 reuse-with-extension**（R01）：Doris 实现最优，但需小幅扩展以保语义保真（如新增 `INVERTED_INDEX_SNII_*` ErrorCode 让 kNotFound 的「越界」语义 1:1 对齐，而非误映射到「文件未找到」）。

### Step 3 — 在盘组件：字节等价闸门（On-Disk Byte-Identity Gate）⚠️ 红线
- **byte-identical（产出逐字节一致）**：**允许**判定 `reuse-doris`，但**必须**附带 cross-decode / 黄金向量字节等价测试。复用前测试绿、复用后回放历史索引仍绿，方可合入。（仅 R05 crc32c 满足且采纳复用。）
- **byte-identical 但仍 keep**：Doris 等价物虽字节一致，但其 API/依赖/语义对 SNII 次优、复用「核心」收益微小而耦合成本实在（如 R03 varint 真正可复用仅约 10 行 LEB128 循环、却要拽入 storage 重头文件且缺 zigzag；R06 写侧字节可对齐但读侧 ByteSource 游标/Status 诊断无等价物）。此时判定 `keep-snii-doris-suboptimal`，并以 cross-decode 黄金测试**防止两者各自演进导致格式静默漂移**。
- **incompatible（Doris 会改变字节）**：**一律拒绝复用、保留 SNII 代码**。Doris 是「不同编码」**不等于**「次优」——这是**格式变更**，不在实现替换范围内。（R04 zstd framing 不同、R07 PFOR vs 纯 FOR 算法与布局不同、R08 无等价物。）

## 三、在盘字节等价闸门（On-Disk Byte-Identity Gate）— 强制条款

> 任何替换在盘组件的 PR，**必须**先提供 byte-identity golden test 并使其变绿，否则不得合入。

- **正向证明**：对同一输入，SNII 实现与 Doris 实现产出**逐字节相同**的输出（编码侧），或对同一字节流产出**完全相同**的解码结果（解码侧）。
- **回放证明**：用已发布 format v2 的历史索引样本回放，Doris 实现解码无误。
- **失败即回滚**：一旦发现字节不一致，立即判定为格式变更，**KEEP fallback**——恢复 SNII 实现，header API 保持不变以零成本回退。
- **演进守护**：即使判定 KEEP，凡两侧编码声称「字节等价」的（R03/R06），也要建立 cross-decode 测试钉死，防止 SNII 与 Doris 各自升级后静默漂移。

## 四、本次审计落实情况

- **复用了什么、为何**：crc32c（R05，Castagnoli 多项式 + thirdparty 库规范值字节一致、性能更优、净删约 110 行）、Local/S3 io 后端（R10，生产已走适配器、S3 为死代码）、分词设施（R13，写读分词一致性依赖 Doris 建表属性语义）、Status（R01，纯内存、删除有损双向转换层、扩展 ErrorCode 保语义）。
- **保留了什么、为何**：在盘格式定义件（R07/R08 无字节等价路径）、字节虽可对齐但 API/依赖次优件（R03/R04/R06）、解耦缝与轻量抽象（R02/R09/R11/R12，保 core 对 Doris/CLucene 零耦合、保留 read_batch 合并契约与回调式 MemoryReporter 解耦缝）。
