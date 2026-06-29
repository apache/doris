# 推荐合入顺序（Merge Sequencing）

排序原则：**先低风险非在盘复用，再在盘复用（须黄金测试变绿后才动），保留件最后（多为零改动）**。宽涟漪的基础类型（Status/Slice）尽早处理，避免后续组件反复触碰。

## 阶段 0 — 基础涟漪先行（Wide Ripples，尽早落地）

1. **R01 Status（reuse-with-extension，1400 调用点，effort=L）** — 最先做。
   - 子步骤：(a) 在 `status.h` 的 ErrorCode 增补 `INVERTED_INDEX_SNII_*` 码，使 kNotFound 的「序号/索引越界」语义与「文件未找到」分流；(b) 统一用零参工厂形态 `Status::Error<CODE,false>(msg)`（避免 `fmt::format` 解析含 `{` 的运行时消息而崩溃，并关闭热路径抓栈/LOG 刷屏）；(c) 按文件分批机械替换 `SNII_RETURN_IF_ERROR`(570)、工厂(811)、`::snii::Status` 类型(48)、转换点(28)；(d) 迁移期临时保留 `to_doris_status` 兜底，全部迁完再删，删除有损的 `to_snii_status`。
   - 之所以先行：1400 处涉及几乎所有 format/query/io 文件，先稳定错误传播层，后续每个组件迁移时不必反复改 Status。
   - **R02 Slice 本期不动**（keep）。虽是第二大涟漪（277 处），但判定保留，避免无收益的机械迁移污染 diff；其「未来标准化为 `std::span<const uint8_t>`」仅作记录，不在本期。

## 阶段 1 — 低风险非在盘复用

2. **R13 CLucene 解耦核查（reuse-doris，8 点，S）** — 维持复用 + 可选 cosmetic 清理（用 `inverted_index::AnalyzerPtr` 别名替前向声明）。零运行期风险。
3. **R10 io Local/S3（reuse-doris，2 点，M）** —
   - 先删 S3 standalone 后端（已被 CMake 排除、零调用方、纯死代码，零风险）。
   - 再迁移本地后端唯一调用点（`SpillableByteBuffer` spill scratch）到 Doris `LocalFileWriter`；保留 `local_file.{h,cpp}` 一个 commit 以便 revert。
   - 依赖 R09 接口层稳定（R09 保留现状，不阻塞）。

## 阶段 2 — 在盘复用（**必须**黄金测试先绿）

4. **R05 crc32c（reuse-doris，byte-identical，28 点，S）** — 唯一采纳的在盘复用。
   - **前置闸门**：先提交 crc32c 黄金向量测试 + 既有 on-disk 校验回放，证明 `crc32c::Crc32c(data)` 与 SNII 实现逐字节一致，**测试变绿后**再切换。
   - 落地：保留薄 inline wrapper `snii::crc32c(Slice)` 委托 `crc32c::Crc32c(...)`，28 处调用点零改动，净删 SNII 约 110 行 + slice8 表。
   - 注意 R08 SectionFramer 经 `snii::crc32c` 委托校验——R05 维持字节一致即不影响 R08。
   - 失败即回滚：恢复 `crc32c.cpp`，header API 不变。

> 注：R03 varint、R06 byte-sink 虽 byte_compat=byte-identical，但判定为 **KEEP**（次优），不进入复用切换；仅需建立 cross-decode 黄金测试防漂移（见 risk-register）。R04 zstd、R07 pfor 为 incompatible，**禁止**复用切换。

## 阶段 3 — 保留件（多为零改动，最后处理/收尾）

5. **R02、R03、R04、R06、R07、R08、R09、R11、R12** — 均 KEEP，本期不改生产代码。收尾动作仅限：
   - 为 R03/R06 建立 cross-decode 黄金测试（防 SNII↔Doris 编码静默漂移）。
   - R11 的 test-only `MeteredFileReader` 可（低优先级）折叠为基于 Doris FileCache 的真实缓存测试。
   - R12 可（低优先级）把 `MemoryReporter` 的 `ConsumeReleaseFn` 回调接到真实 Doris `MemTracker`，需以「reporter 净值归零」等价性测试守护，回调置 null 即回退。

## 关键依赖与门禁

- Status（R01）先于其它迁移：所有组件的错误返回都依赖它。
- crc32c（R05）切换门禁 = 黄金测试绿 + 历史索引回放绿。
- R08 SectionFramer 依赖 R05 字节一致性，绑定验证。
- 任何在盘组件若黄金测试红 → 立即 KEEP fallback，不合入。
