# P4-T06e — FIX-BIND-STATIC-PARTITION (P0-3) — Review Rounds

> 每轮记录 finding + verdict + 处置，防跨轮矛盾。clean-room 对抗 review（多 agent + code-first 独立判断）。
> 设计：`plan-doc/tasks/designs/P4-T06e-FIX-BIND-STATIC-PARTITION-design.md`

---

## Round 1 — workflow `wi3mnjymb`（5 lens × review→adversarial-verify，18 agents）

**裁决**：13 raw → 8 confirmed（3 major / 4 minor / 1 nit）/ 5 refuted；**mustFix=3（同一根因）**。

### 🔴 MAJOR（confirmed，同一根因）— P03-1 / P03-LENS-01 / P03-REG-1

- **根因**：projection 分支键用了 `!staticPartitionColNames.isEmpty()`（仅静态分区走 full-schema 投影）。但 `getRequirePhysicalProperties` 已改为 **full-schema 索引**——要求 child 始终 full-schema 序。**纯动态/无静态分区**的写（如 `INSERT INTO mc (part, data) SELECT ...` 重排显式列名）走 ELSE 分支（cols 序投影），child=cols 序 ≠ full-schema 序 → 分布按 full-schema 位置索引到**错列**（OOB/错 hash-sort）→ MaxCompute streaming "writer has been closed"。另：分区表显式**部分列**无静态分区写仍走 JDBC 子集投影，偏离 legacy full-schema（P03-REG-1）。
- **处置 ✅ FIXED**：分支键改为 `!table.getPartitionColumns().isEmpty()`（**分区表** → full-schema 投影，镜像 legacy `bindMaxComputeTableSink`；非分区表 JDBC/ES → 维持 cols 序投影）。这样分区连接器表 child 恒为 full-schema 序，与 full-schema 索引一致；全 case（all-static/partial-static/纯动态含重排/部分列）与 legacy 一致。`BindSink.java:941` + `PhysicalConnectorTableSink` javadoc 更新。
  - **验证**：新增 `dynamicReorderedColumnListHashesByPartitionAtFullSchemaPosition`（cols=[part,data] 重排、child=full-schema [data,part]）断言 hash key=partSlot@full-schema 位 1；mutation `getFullSchema()→cols` 令该 test + `partialStaticPartitionHashesByDynamicColumn` 双红（2 failures）。51 测试全绿、checkstyle 0、import-gate 净。

### 🟡 MINOR/NIT（confirmed，test gap）

- **P03-LENS-02**（minor）：缺纯动态「重排列名」分布 test（旧 dynamic test cols==fullSchema 退化、不能判 cols-vs-fullschema）。**✅ FIXED**：新增上述 reordered test。
- **P03-BE-2 / TA-1 / TA-3 / TA-2**（minor×3 + nit×1，同主题）：bind 期 full-schema 投影（NULL 填充 + 分区列在 full-schema 末尾）未被 connector-path 单测直接 pin——`BindConnectorSinkStaticPartitionTest` 只测列选择 helper `selectConnectorSinkBindColumns`，未驱动 `bindConnectorTableSink` 的投影。**处置 = 登记已知限制（KNOWN-LIMITATION，非静默）**：
  - fe-core **无**驱动 `bindConnectorTableSink` 的轻量 harness（`bind()` 走 `RelationUtil.getDbAndTable` 真 Env 解析；分析-INSERT 测试只覆盖经 `createTable` 注册的 OLAP 内表，PluginDriven 外表需连接器插件，注册成本高、无现成 harness）。
  - 投影由 **共享** helper `getColumnToOutput` + `getOutputProjectByCoercion(table.getFullSchema())` 完成——与 legacy `bindMaxComputeTableSink:904-906` 及 Iceberg 连接器路径**逐字一致**，且这两 helper 被既有 OLAP/Hive/Iceberg insert 分析测试充分覆盖。本 diff 的**新**行为仅是「分区表路由到该共享投影」（一行条件），已被 inspection + 分布层 full-schema 索引测试（要求 child 为 full-schema 序方能过）间接约束。
  - 列**顺序**（数据列…分区列在末尾）由 `getFullSchema()` 的契约决定（连接器 `initSchema` 末尾追加分区列，`MaxComputeConnectorMetadata` 同 legacy），非本 diff 代码决定。
  - 端到端由 p2 live 回归 `regression-test/suites/external_table_p2/maxcompute/write/test_mc_write_static_partitions.groovy` 覆盖（all-static/partial-static/纯动态/VALUES/OVERWRITE）。
  - **结论**：production code 经审阅者确认正确（"byte-for-byte same pattern as legacy/Iceberg"），此为单测覆盖缺口非行为缺陷；登记 deviations-log，留待外表分析 harness 落地后补（与 fe-core test-infra 限制耦合）。**真值闸仍是 live e2e。**

### ✅ REFUTED（5，无需处置）

- **P03-BE-1 / P03-2 / P03-REG-2**（partial-static BE 末尾擦全部分区列 → 单静态 spec 路由、丢动态列值）：审阅者证为 **legacy 既有行为**（本 diff 不改 BE、不引入），parity 保持；属既有 MaxCompute partial-static BE 限制，另案。本 fix 仅泛化 FE 使其与 legacy 一致。
- **TA-4**：dynamic test 退化——已被 P03-LENS-02 新 test 覆盖。
- **TA-5**：非空分区列 NULL-fill 安全仅靠连接器硬编码 `nullable=true`——可接受（legacy 同此假设；通用路径无非空分区列）。

### Round-1 累计结论
- 分支键 `!staticPartitionColNames.isEmpty()` → `!table.getPartitionColumns().isEmpty()`（**分区表恒 full-schema 投影 = legacy 忠实镜像**）是本轮关键修正。
- full-schema 分布索引 + full-schema child 投影**必须成对**——二者只对分区表成立；非分区(JDBC/ES) 维持 cols 序 + capability 门 GATHER。
- bind 投影单测缺口登记为 KNOWN-LIMITATION（parity + p2 live 覆盖），非静默跳过。

---

## Round 2 — workflow `wy299gtsh`（4 lens 聚焦 branch-on-partitioned 收敛，6 agents）

**裁决**：2 raw → **1 confirmed NEW major（mustFix）** / 1 refuted（"No change required"，被证为正确行为）。

### 🔴 MAJOR（confirmed NEW）— P03-R2-01：branch-on-partitioned 仍太窄 → 非分区 MaxCompute 重排/部分显式列名静默丢/错列

- **根因**：翻闸后**真实** MaxCompute catalog 是 `PluginDrivenExternalCatalog`（`CatalogFactory:105-113`），**所有** MC 写走 `bindConnectorTableSink`。**非分区** MC 表 `getPartitionColumns()` 空 → 落 cols 序 ELSE 分支。但 MC BE/JNI writer **按位置**映射 Arrow 列到 `writeSession.requiredSchema()`（完整表 schema 序，`MaxComputeJniWriter:202-208,354-356`）。故 `INSERT INTO mc_nonpart (b,a) SELECT ...`（重排）→ 值落错列（静默 corruption）；`(a) SELECT ...`（部分）→ 列数不符/未填 NULL。**legacy `bindMaxComputeTableSink:905-908` 无条件 full-schema 投影**（不论是否分区）——branch-on-partitioned 漏了非分区 MC，属翻闸回归。
- **处置 ✅ FIXED（用户既批"全 parity"方向，采审阅者 option b = capability）**：新增 SPI capability **`SINK_REQUIRE_FULL_SCHEMA_ORDER`**（连接器写按位置映射 full-schema vs JDBC 按名）；`MaxComputeDorisConnector.getCapabilities()` 声明之；`PluginDrivenExternalTable.requiresFullSchemaWriteOrder()` 读之；`bindConnectorTableSink` 分支键 `!getPartitionColumns().isEmpty()` → **`table.requiresFullSchemaWriteOrder()`**。这样 **MaxCompute 全写形（分区/非分区 × 全/重排/部分/静态/动态）恒 full-schema 投影 = legacy 逐字等价**；JDBC/ES 不声明 → 维持 cols 序（其 INSERT SQL 按名需 cols 序）。改 4 文件（SPI enum / MC 连接器 / fe-core reader / fe-core bind）+ javadoc。
  - **验证**：3 模块编译绿、checkstyle 0×3、import-gate 净、55 测试全绿。e2e gate：`test_mc_write_insert.groovy` Test 3（部分列）本就 gate 部分列 NULL 填充；**新增 Test 3b（重排显式列名 VALUES+SELECT 两形）** + `.out`——按位置投影正确则 id/name/score 各归位，cols 序投影则错列（live ODPS gate，CI 跳）。
- **distribution 一致性**：full-schema 索引 + full-schema child 对 MaxCompute 恒成立（capability→full-schema 投影）；JDBC 不声明 capability 且无分区列 → 分布走 GATHER（不索引 child）。两 capability 正交：`SINK_REQUIRE_FULL_SCHEMA_ORDER`(投影) vs `SINK_REQUIRE_PARTITION_LOCAL_SORT`(分布)。

### ✅ REFUTED（1）

- **P03-V2-N1**（分区表部分列名校验非空数据列 → 抛 "Column has no default value"）：审阅者证为**正确意图行为**（与 legacy MC parity，且翻闸前通用路径反而漏校验 = 更宽松 bug）。无需改。

### Round-2 累计结论
- **正确判别键 = capability `SINK_REQUIRE_FULL_SCHEMA_ORDER`（连接器是否按位置写 full-schema），非"分区"也非"静态分区"。** 三次迭代收敛：static → partitioned → **capability(positional-write)**。最终 = MaxCompute 与 legacy `bindMaxComputeTableSink` 全写形逐字等价。
- bind 投影单测仍 KNOWN-LIMITATION（无 harness）；非分区重排回归经 p2 `test_mc_write_insert.groovy` Test 3/3b live gate + parity 覆盖。capability 声明/reader 按既有约定不单测（既有 readers 亦仅被 mock）。

---

## Round 3 — workflow `wlwpw0b2s`（3 lens 聚焦 capability 修正收敛 + legacy 全 parity，6 agents）

**裁决**：3 raw → **mustFix=0（收敛）**；1 confirmed NEW = **nit**（前瞻 robustness，非现行缺陷）/ 2 refuted（同一 nit 的重复/被证非现行缺陷）。

### ✅ 收敛确认（legacy 全 parity）
- 三 lens 均确认：capability `SINK_REQUIRE_FULL_SCHEMA_ORDER` gated full-schema 投影令 **MaxCompute 全写形与 legacy `bindMaxComputeTableSink` 逐字等价**（no-list/full/reordered/partial/all-static/partial-static/pure-dynamic/non-partitioned）；JDBC/ES 不声明 → cols 序（其 INSERT SQL 按名需之，正确）；trino-connector `getCapabilities` 默认空集、不声明 → cols 序（若未来其按位置写须声明该 capability，机制已就位）。common 非分区全序 `INSERT...SELECT` 经 `getColumnToOutput` 全列已 mentioned→无填充、与旧 cols 序投影等价（无 common-case 回归）。

### 🟢 NIT（confirmed NEW）— P03-V3-1：跨 capability 隐式耦合（前瞻 robustness）
- **观察**：分布 full-schema 索引 gated on `SINK_REQUIRE_PARTITION_LOCAL_SORT`，而其依赖的 full-schema child 投影 gated on **另一** capability `SINK_REQUIRE_FULL_SCHEMA_ORDER`，二者无耦合校验。**现不可达**（唯一走此路径的 MaxCompute 两 capability 齐声明；Jdbc 皆不声明）。审阅者自评 "NOT a current correctness bug … latent fragility"，定级 nit。
- **处置 ✅**：在 `SINK_REQUIRE_PARTITION_LOCAL_SORT` javadoc 补硬依赖说明（"declaring this 须同时声明 `SINK_REQUIRE_FULL_SCHEMA_ORDER`，否则分布按 full-schema 位索引会错列"），与既有 "也须声明 `SUPPORTS_PARALLEL_WRITE`" 约定同款。不加运行期 assert（对假想未来连接器属过度设计；doc fail-loud 足够）。

### Round-3 累计结论
- **mustFix=0，收敛**。三轮迭代：static → partitioned → **capability(positional-write)**，终态 = MaxCompute 与 legacy `bindMaxComputeTableSink` 全写形逐字 parity；JDBC/ES cols 序 parity。
- 两写 capability 正交但有硬依赖（LOCAL_SORT ⟹ FULL_SCHEMA_ORDER），已 javadoc 登记。
- 真值闸仍为 live e2e（p2 `test_mc_write_insert` Test 3/3b + `test_mc_write_static_partitions`）。bind 投影单测缺口 = KNOWN-LIMITATION（无外表分析 harness），parity + p2 覆盖。

## ✅ 最终裁决：3 轮收敛（0 mustFix），可 commit。
