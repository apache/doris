# R05-crc32c — crc32c 校验

## R05-crc32c 决策文档

### 结论与依据
**verdict = reuse-doris**。SNII 自带的 crc32c（be/src/snii/encoding/crc32c.h:10 声明 `crc32c_extend`；be/src/storage/index/snii/core/src/encoding/crc32c.cpp 实现）使用 Castagnoli 多项式（crc32c.h:9，crc32c.cpp:17 的 0x82F63B78 为 0x1EDC6F41 的位反射形式）并在入口/出口做标准 `~crc`（crc32c.cpp:100,108），与 Doris 已链接的 Google `crc32c` 第三方库（`crc32c::Crc32c`/`Extend`，规范 CRC32C，与 RocksDB/LevelDB 同源）数学上产出**同一规范值**。Doris 在 page_io、segment、wal、rowset 等核心路径已统一使用该库（be/src/storage/segment/page_io.cpp:108,181 等），SNII 复用属“实现替换”而非“格式变更”。

### Doris 等价物
- 头文件：`crc32c/crc32c.h`（/mnt/disk1/jiangkai/workspace/install/installed-master/include/crc32c/crc32c.h:50,53）
- 符号：`crc32c::Extend(crc, data, count)`、`crc32c::Crc32c(data, count)`

### 是否最优
最优。算法等价（同多项式+同标准预/后取反）；性能不弱反优（Google 库含 x86 与 ARMv8 硬件分发，SNII 在 ARM 仅软件 slice-by-8）；零新增依赖（已是 Doris thirdparty）；可净删 SNII ~110 行与 slice8 表。

### 字节兼容性结论（on-disk）
**byte-identical**。一次性 `snii::crc32c(data) == crc32c::Crc32c(data.data(), data.size())` 对任意输入成立，已发布 format v2 的所有校验字段（section_framer 的 type+len+payload crc、bootstrap_header、tail_pointer/tail_meta_region、dict_block、frq_pod/frq_prelude、prx_pod、bsbf、per_index_meta、logical_index block crc）保持完全一致。**必须以黄金向量测试 + 旧文件校验回放固化此结论后方可合入**；若任一向量不一致即视为格式变更，按 RED LINE 回滚保留 SNII 实现。

### 迁移设计
1. 保留 `be/src/snii/encoding/crc32c.h`，将其改为薄 inline 适配层：
   - `inline uint32_t crc32c_extend(uint32_t crc, Slice d){ return crc32c::Extend(crc, d.data(), d.size()); }`
   - `inline uint32_t crc32c(Slice d){ return crc32c::Crc32c(d.data(), d.size()); }`
   - 头部 `#include <crc32c/crc32c.h>`。
2. 删除 be/src/storage/index/snii/core/src/encoding/crc32c.cpp（连同 slice8 表/SSE4.2 路径/CPUID），并从对应 CMake/构建清单移除该 TU。
3. 约 28 处调用点（tail_pointer.cpp:48,90；section_framer.cpp:13,29；per_index_meta.cpp:60,86；tail_meta_region.cpp:62,65,89,144；frq_pod.cpp:90,108；bootstrap_header.cpp:34,68；dict_block.cpp:95,113；prx_pod.cpp:213,242,598,634；frq_prelude.cpp:173,200,201；logical_index_writer.cpp:506；snii_compound_writer.cpp:124；logical_index_reader.cpp:218；bsbf.cpp:176,178,193 用裸指针 Slice 同样适配）**保持签名不变、零改动**。
4. 确保 SNII core 链接 crc32c thirdparty（Doris 主体已链接，仅需让 snii 目标可见）。
5. 风险/回滚：黄金测试若发现不一致或 ARM 工具链未提供该库符号，恢复 crc32c.cpp 即可，header API 契约不变，调用点无需回改。

---

## TDD

## R05-crc32c TDD 测试计划（RED → GREEN → REFACTOR）
gtest 目标：`doris_be_test`（be/test）。新增 be/test/storage/index/snii_crc32c_equiv_test.cpp，参照既有 be/test/util/crc32c_test.cpp（其 TEST(CRC, StandardResults) 已校验 Doris 库的标准向量）。

### RED（先写、应失败或先用旧实现锚定）
1. **功能验证 functional**：对已知标准向量断言固定值，例如 RFC/RocksDB 经典向量 `crc32c("123456789")` 等，及空串、单字节、非 8 字节对齐尾部（len=1,4,7,8,9,15,16）、>1KB 随机但定长 seed 的确定性输入。断言为硬编码期望 hex（deterministic）。
2. **等价性验证 equivalence**：对同一批输入断言 `snii::crc32c(Slice(buf,n)) == crc32c::Crc32c((const uint8_t*)buf, n)`，覆盖随机长度（固定 RNG seed 保证可复现）。链式：`snii::crc32c_extend(prev, b) == crc32c::Extend(prev, b.data(), b.size())`，含 prev≠0 与分段拼接 `Extend(Extend(0,a),b)==Crc32c(a++b)`。

### GREEN（落地复用后必过）
3. **字节级黄金测试（on-disk 必备）**：用迁移前的二进制对各 format 段落（section_framer 包络、bootstrap_header、tail_pointer、tail_meta_region、dict_block、frq_pod、prx_pod、bsbf、per_index_meta、logical_index block）各采一组固定输入，落盘其 4 字节校验值为 golden（hex 常量内联在测试中）；迁移后重新计算逐字节比对 golden，断言完全相等。
4. **on-disk 回放**：取一份已发布 format v2 样本索引（或测试夹具构造的样本），用复用后的 crc32c 跑全量校验路径（各 *_decode/verify），断言无 Status::Corruption，证明既有 on-disk 校验仍通过。
5. **cross-decode**：构造“SNII 旧实现写入 crc 的字节” → 用 `crc32c::Crc32c` 校验通过；“`crc32c::Extend` 写入的 crc” → 用 SNII inline wrapper 校验通过（双向一致）。

### REFACTOR
6. 删除 crc32c.cpp 后保持上述全部断言为绿；确认 slice8 表移除不影响任何向量；在 CI 的 x86 与（若可用）ARM 两类机器各跑一遍等价/黄金测试，锁定跨架构硬件分发的字节一致性。