# 决策日志（ADR）

> **Append-only**：新决策置顶；旧决策永不删除（即使被推翻，也只标"已废止"而不删除）。
> 编号规则：`D-NNN` 三位数字，从 001 起单调递增，永不复用。
> 历史决策 D1-D12（master plan §5）+ U1-U6（RFC §16.2）已迁入并映射到 D-001..D-018。
> 与"偏差"的区别见 [README §3.1](./README.md)。
>
> 每条决策模板见文末 §附录。

---

## 📋 索引

> 时间倒序；带 ✅ 表示生效中，❌ 表示已废止，🟡 表示待评审

| 编号 | 别名 | 简述 | 日期 | 状态 |
|---|---|---|---|---|
| D-020 | — | 单 `hms` catalog 多格式 scan 路由 = 方案 B（`ConnectorMetadata.getScanPlanProvider(handle)` per-table default）；细化 D-005（design-only，实现批 E/P7）| 2026-06-05 | ✅ |
| D-019 | — | P3 hudi 采用 hybrid：现做 model-agnostic 连接器硬化+测试（behind gate），推迟 catalog 模型落地+cutover 到 hive/HMS migration | 2026-06-04 | ✅ |
| D-018 | U6 | `ConnectorColumnStatistics` 用 javadoc 类型映射表 + IAE 保证类型安全 | 2026-05-24 | ✅ |
| D-017 | U5 | sys-table 命名统一 `$suffix`，别名机制留待未来 | 2026-05-24 | ✅ |
| D-016 | U4 | `getCredentialsForScans` 批量化，返回 `Map<Range, Credentials>` | 2026-05-24 | ✅ |
| D-015 | U3 | `ConnectorTransaction.getTransactionId` 由连接器分配 | 2026-05-24 | ✅ |
| D-014 | U2 | 不新增 `invalidateColumnStatistics`，挂在 `invalidateTable` | 2026-05-24 | ✅ |
| D-013 | U1 | `ConnectorProcedureOps.listProcedures` 一次性返回，生命周期稳定 | 2026-05-24 | ✅ |
| D-012 | D12 | 用户安装 connector 后初版强制重启 FE | 2026-05-24 | ✅ |
| D-011 | D11 | `RemoteDorisExternalCatalog` 长期做 connector，不在本计划主线 | 2026-05-24 | ✅ |
| D-010 | D10 | `LakeSoulExternalCatalog` 在 P8 删除剩余类 | 2026-05-24 | ✅ |
| D-009 | D9 | API 版本号本计划范围内永不 +1，只新增 default 方法 | 2026-05-24 | ✅ |
| D-008 | D8 | 生产环境不允许 built-in connector，强制目录式插件 | 2026-05-24 | ✅ |
| D-007 | D7 | kafka/kinesis/odbc/doris 子目录不在本计划范围 | 2026-05-24 | ✅ |
| D-006 | D6 | Iceberg snapshot/manifest cache 放连接器内，fe-core 不感知 | 2026-05-24 | ✅ |
| D-005 | D5 | hudi/iceberg-on-HMS 用 `ConnectorTableSchema.tableFormatType` 区分 | 2026-05-24 | ✅ |
| D-004 | D4 | HMS event pipeline 放 `fe-connector-hms`，通过 `ConnectorMetaInvalidator` 回调 | 2026-05-24 | ✅ |
| D-003 | D3 | 旧 `*ExternalCatalog` 子类**全部删除**，不保留中间形态 | 2026-05-24 | ✅ |
| D-002 | D2 | `PluginDrivenScanNode` 长期保持 `extends FileQueryScanNode` | 2026-05-24 | ✅ |
| D-001 | D1 | 沿用已有 `SUPPORTS_PASSTHROUGH_QUERY`，不新增 query SPI | 2026-05-24 | ✅ |

---

## 详细记录（时间倒序）

### D-020 — 单 `hms` catalog 多格式 scan 路由 = 方案 B（per-table SPI provider）

- **日期**：2026-06-05
- **状态**：✅ 生效
- **关联**：[D-005](#d-005)（被细化）、[D-009](#d-009)（default-only 约束）、[D-019](#d-019)（hybrid）、[tasks/P3 T08](./tasks/P3-hudi-migration.md)、[designs/P3-T08-tableformat-dispatch-design.md](./tasks/designs/P3-T08-tableformat-dispatch-design.md)、[research/spi-multi-format-hms-catalog-analysis.md](./research/spi-multi-format-hms-catalog-analysis.md)
- **背景**：legacy 单 `hms` catalog 靠 `HMSExternalTable.dlaType` per-table tag + 处处 `switch(dlaType)` 同时暴露 Hive/Hudi/Iceberg。SPI 侧 `ConnectorTableSchema.tableFormatType` **产而不用**——`PluginDrivenExternalTable.initSchema:79-109` 只读 columns、`Connector.getScanPlanProvider:40-42` per-catalog 单点、`HiveScanPlanProvider` 硬编码 `tableFormatType="hive"`（research §6①②③ + 本场 firsthand 核读）。T08（批 D，design-only）须定 per-table 路由 seam；研究浮现三互斥方案（A 连接器内 router / B per-table SPI provider / C fe-core 发现期分派）。
- **决策**：M2 scan 路由采 **方案 B**——在 `ConnectorMetadata` 新增**向后兼容 default** `getScanPlanProvider(ConnectorTableHandle handle)`（默认返 null → fe-core 回落 per-catalog `Connector.getScanPlanProvider()`）；fe-core `PluginDrivenScanNode.getSplits` 优先 per-table provider、回落 per-catalog；注册 `"hms"` 的连接器 override 之、按 `handle.getTableType()` 委派 Hudi/Iceberg provider。把"per-table 选 provider"升为一等 SPI 契约。配套 **M1**（fe-core 按缓存的 `tableFormatType` 做 per-table 引擎名/身份，作 opaque 串逐字上报、热路径不读）三方案通用。**design-only，实现 = 批 E/P7**。
- **替代方案**：**A 连接器内 router**（`Connector.getScanPlanProvider()` 返回一个 `planScan` 按 `handle.getTableType()` 委派的 router）——零 SPI churn（`planScan` 已带 handle，本场核实），但路由藏进连接器、per-table 语义非一等契约；列为备选，批 E 实现期可据 iceberg 接入复杂度复核。**C fe-core 发现期分派**（fe-core 读 `tableFormatType` 建 format-specific 表对象，≈legacy DLAType→多态 DlaTable）——**否决**：fe-core 回退到 per-format 分派，违背瘦 fe-core 北极星（import-gate / D-003 / D-006）。
- **影响**：**细化 [D-005]**——D-005 的"`tableFormatType` 区分符"结论沿用；但其"fe-core dispatch 到对应 `PhysicalXxxScan`"措辞（2026-05-24，**早于 P1 scan-node 统一**为单 `PluginDrivenScanNode` + per-range format）由 per-table provider seam 取代（SPI 路径已无 per-format `PhysicalXxxScan`）。批 E/P7 据此实现 M1+M2；新 default 方法满足 [D-009]（不破签名）。Iceberg-on-hms 经 SPI 依赖 **P6** 先补 `IcebergScanPlanProvider`（M3）；hms 网关引入对 `-hudi`/`-iceberg` 模块依赖边（A/B 同担）。**本场无代码改动**。

---

### D-019 — P3 hudi 采用 hybrid 推进策略

- **日期**：2026-06-04
- **状态**：✅ 生效
- **关联**：[DV-005](./deviations-log.md)、[D-005](#d-005)、[tasks/P3](./tasks/P3-hudi-migration.md)、master plan §3.4/§3.8
- **背景**：两轮 code-grounded recon（+ 对抗验证）揭示：HMS-over-SPI 读码已存在但 dormant（gate 关、零 live caller）；scan/split plumbing 正确（单 `PluginDrivenScanNode` 混合 COW-native+MOR-JNI 非问题，与 legacy 结构等价）；真正阻塞是 catalog 模型错配（独立 `"hudi"` type vs 寄生 `"hms"` 的 `DLAType.HUDI`，fe-core 不消费 `tableFormatType`）+ 关闭的 gate；另有一批**与模型无关**的 SPI-surface 正确性缺口（`schema_id`/`history_schema_info` 缺、`column_types` 双 bug、time-travel 静默返最新、增量读无表示、partition 裁剪缺、三模块零测试）。
- **决策**：P3 走 **hybrid**。**现在做 (b)**（批 A–D，全部 behind 关闭的 gate，零 live-path 风险）：hudi 连接器 model-agnostic 正确性修复 + metadata 补全 + 测试基线 + 模型 dispatch 设计（design-only）。**推迟 (a)**（批 E，登记不编码）：fe-core 消费 `tableFormatType` 的 per-table 分流、gate flip（`SPI_READY_TYPES` 加 hms/hudi）、live cutover、删 legacy `datasource/hudi/`、完整增量/time-travel、集群/runtime 验证 —— 并入一个 properly-scoped hive/HMS migration（P7 或专门子阶段）。
- **替代方案**：(a) **hms-first 一次到位** —— 否决为 P3 首交付（把 P7 范围拉进 P3、re-route live 重度使用的 HMS 路径、零测试网，回归风险大）；(c) **直接 flip gate** —— 早已否决（模型错配下 `"hudi"` provider 不可达 + 高回归）。
- **影响**：P3（hybrid）**不交付用户可见行为变化**（hudi 仍走 legacy，gate 不翻）；产出是连接器硬化 + 测试网 + 设计。批 A–C 验证为单测/设计级，端到端/集群验证随批 E cutover。tasks/P3 据此划批。

---

### D-018 — `ConnectorColumnStatistics` 类型安全契约（原 U6）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[01-spi-extensions-rfc.md §11.2](./01-spi-extensions-rfc.md)
- **背景**：`ConnectorColumnStatistics.minValue / maxValue` 用 `Object` 装载，缺少静态类型检查可能导致 connector 间不一致。
- **决策**：在 `ConnectorColumnStatistics` javadoc 中列出 `ConnectorType` ↔ Java 装箱类型完整映射表（如 INT→Integer、TIMESTAMP→Instant、BINARY→byte[]）；连接器读取不匹配类型时**抛 `IllegalArgumentException`**，由 fe-core 转成 `UserException`。
- **替代方案**：（a）引入泛型 `ConnectorColumnStatistics<T>`——过于复杂、跨方法签名传染；（b）引入 union 类型——Java 不原生支持。
- **影响**：仅 javadoc 与运行时检查，无签名变化。

---

### D-017 — sys-table 命名统一 `$suffix`（原 U5）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[01-spi-extensions-rfc.md §10](./01-spi-extensions-rfc.md)
- **背景**：Iceberg / Paimon 各自有 sys-table（`tbl$snapshots`、`tbl$history` 等）。命名风格 `$xxx` vs `xxx@` vs `[xxx]` 跨方言不一致。
- **决策**：SPI 层固定 `$suffix` 约定。如未来出现冲突（如某 SQL dialect 把 `$` 视为变量前缀），通过 catalog property `sys_table_separator` 提供别名机制，但**不在本计划范围**。
- **影响**：所有 sys-table 实现统一遵循。

---

### D-016 — `getCredentialsForScans` 批量化（原 U4）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[01-spi-extensions-rfc.md §9](./01-spi-extensions-rfc.md)
- **背景**：原设计单 range 调一次 `getCredentialsForScan`，N 个 range 触发 N 次 STS 调用，可能撞限流。
- **决策**：签名定为 `Map<ConnectorScanRange, ConnectorCredentials> getCredentialsForScans(session, handle, List<ConnectorScanRange>)`。连接器自由决定 STS 调用粒度（1 次共享 / 按 prefix 分组 / 1:1）。fe-core 一个 scan node 一次调用。
- **替代方案**：保持单个 + 加内部缓存——把缓存策略推给每个 connector，不一致风险更高。
- **影响**：替换原 `getCredentialsForScan` 单个签名。调用位置从 `setScanParams` 移到 `createScanRangeLocations`。

---

### D-015 — `ConnectorTransaction.getTransactionId` 由连接器分配（原 U3）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[01-spi-extensions-rfc.md §7.2](./01-spi-extensions-rfc.md)
- **背景**：transaction ID 是连接器自己分配还是 fe-core 统一分配？
- **决策**：连接器分配。连接器最清楚事务 ID 与外部系统（如 HMS transaction id、Iceberg snapshot id）的对应关系。fe-core 在 `PluginDrivenTransactionManager` 用 `Map<Long, ConnectorTransaction>` 索引即可。
- **影响**：`ConnectorTransaction.getTransactionId()` 是 connector-side 字段。

---

### D-014 — 不新增 `invalidateColumnStatistics`（原 U2）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[01-spi-extensions-rfc.md §6](./01-spi-extensions-rfc.md)
- **背景**：是否给 `ConnectorMetaInvalidator` 加 `invalidateColumnStatistics(...)`？
- **决策**：暂不加。column stats 失效一并挂在 `invalidateTable` 上，避免接口表面膨胀。如后续发现频繁需要单独失效列统计，再加方法（向后兼容 default 即可）。
- **影响**：`ConnectorMetaInvalidator` 接口保持 5 个方法（catalog / database / table / partition / statistics 整张表）。

---

### D-013 — `ConnectorProcedureOps.listProcedures` 一次性返回（原 U1）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[01-spi-extensions-rfc.md §5.2](./01-spi-extensions-rfc.md)
- **背景**：connector 暴露的 procedure 列表是初始化时固定还是允许运行时变化？
- **决策**：一次性。Connector 生命周期内稳定；如外部系统的可用 procedure 集合变化，必须重新创建 catalog。
- **理由**：fe-core 可缓存该列表用于 `SHOW PROCEDURES`、autocompletion；动态变化模型复杂度不值得。
- **影响**：在 `listProcedures()` 的 javadoc 中明确写出"Lifecycle contract"。

---

### D-012 — Connector 安装初版强制重启 FE（原 D12）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)
- **背景**：装新 connector 后是否要求重启 FE？
- **决策**：初版强制重启。原因：跨连接器共享类型可能有 classloader 缓存问题，强制重启避免难复现的 corner case。后续版本可考虑热加载。
- **影响**：文档明确 + 装包流程明确。

---

### D-011 — `RemoteDorisExternalCatalog` 不在本计划主线（原 D11）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)
- **背景**：Doris-to-Doris federation 是否做成 connector？
- **决策**：长期目标做 connector，但**单独立项**，不在本计划主线（25 周计划中）。
- **影响**：`RemoteDorisExternalCatalog` 在 P8 不删除；保留独立路径。

---

### D-010 — `LakeSoulExternalCatalog` 在 P8 删除（原 D10）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)
- **背景**：`CatalogFactory` 已抛 "Lakesoul catalog is no longer supported"，但类文件仍在。
- **决策**：在 P8 收尾时删除剩余 `datasource/lakesoul/` 全部类。
- **影响**：P8 task 增加 lakesoul 清理项。

---

### D-009 — API 版本号本计划永不 +1（原 D9）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)、[01-spi-extensions-rfc.md §2.1](./01-spi-extensions-rfc.md)
- **背景**：`ConnectorProvider.apiVersion()` 何时 +1？
- **决策**：本计划范围内（25 周）保持 `apiVersion=1`，只新增 default 方法，不破坏现有签名。
- **影响**：所有 SPI 扩展必须用 default 方法。如真有不可避免的 breaking change，需走 deviation 流程并升级到 v2。

---

### D-008 — 生产强制目录式插件（原 D8）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)
- **背景**：是否允许 built-in connector（classpath 中直接打进 FE jar）？
- **决策**：否。built-in 模式只用于测试（ServiceLoader 扫 classpath）；生产部署必须从 `connector_plugin_root` 目录加载 plugin zip。
- **影响**：FE 发行包不含 connector jar；运维流程文档要明确插件部署步骤。

---

### D-007 — kafka/kinesis/odbc/doris 不在本计划范围（原 D7）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)
- **背景**：`datasource/` 下还有 kafka / kinesis / odbc / doris 子目录，是否一并迁移？
- **决策**：否。流式数据源（kafka/kinesis）与外部 catalog 模型不同；odbc 是 BE-driven；doris 是内部联邦。单独立项。
- **影响**：P8 不删除这 4 个子目录。

---

### D-006 — Iceberg snapshot/manifest cache 放连接器内（原 D6）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)、[01-spi-extensions-rfc.md §8](./01-spi-extensions-rfc.md)
- **背景**：Iceberg 的 snapshot cache 和 manifest cache 是 fe-core 通用基础设施还是连接器内部细节？
- **决策**：连接器内部细节。fe-core 不感知。连接器自己管理生命周期、淘汰策略。
- **替代方案**：放 `fe-core/datasource/metacache/` 通用框架——会增加 fe-core 对 Iceberg 概念的耦合。
- **影响**：P6 迁移时把 `cache/IcebergManifestCacheLoader` 等整体搬到 `fe-connector-iceberg`。

---

### D-005 — Hudi / Iceberg-on-HMS DLA 模型方案 A（原 D5）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §3.4](./00-master-plan.md)
- **背景**：HMS 表可能"实际是" Hudi 或 Iceberg。如何在 SPI 层建模？
- **决策**：方案 A — 用 `ConnectorTableSchema.tableFormatType` 字段（值如 `"HIVE"` / `"HUDI"` / `"ICEBERG"`），由 HMS connector 探测后填充；fe-core 据此 dispatch 到对应 `PhysicalXxxScan`。
- **替代方案**：方案 B — Hudi 作为独立 catalog type，内部委托 HMS——增加 catalog 实例数，用户混淆度高。
- **影响**：P3 hudi 和 P7 hive 迁移都依赖此模型。

---

### D-004 — HMS event pipeline 放 fe-connector-hms（原 D4）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §3.8](./00-master-plan.md)、[01-spi-extensions-rfc.md §6](./01-spi-extensions-rfc.md)
- **背景**：21 个 HMS event 类放 fe-core 还是 fe-connector-hms？
- **决策**：fe-connector-hms。通过新 SPI 接口 `ConnectorMetaInvalidator`（在 `ConnectorContext` 暴露）回调 fe-core 的 `ExternalMetaCacheMgr`。
- **替代方案**：只把"轮询 HMS 拿事件流"放 connector，"解析事件 + 分发失效"留 fe-core——分散，不利于演化。
- **影响**：P7.2 完整迁移 21 个类 + `MetastoreEventsProcessor`。`HiveConnector.create(...)` 启动 listener 线程；`close()` 停止。

---

### D-003 — 旧 `*ExternalCatalog` 子类全部删除（原 D3）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)
- **背景**：迁移过程中是保留旧 `IcebergExternalCatalog` 等类作为"中间形态"还是彻底删除？
- **决策**：全部删除。中间形态会让代码长期处于"两套并存"状态，维护负担、bug 风险都更大。
- **替代方案**：保留一段"deprecated 但可用"期——拒绝，因为旧实现实质上不会被维护。
- **影响**：P8 强制删除所有 `*ExternalCatalog` / `*ExternalDatabase` / `*ExternalTable` 类；前置工作是 P2-P7 把所有反向 `instanceof` 改为通用接口调用。

---

### D-002 — `PluginDrivenScanNode` 长期保持 extends `FileQueryScanNode`（原 D2）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)
- **背景**：`PluginDrivenScanNode` 当前继承 `FileQueryScanNode`，但 JDBC / ES 本质不是文件扫描，用 `FORMAT_JNI` 兜底。是否要重构为更彻底的多态？
- **决策**：长期保持当前继承结构。JDBC / ES 的 `FORMAT_JNI` 兜底已被 ES/JDBC 验证可行。重构成本高、收益不明确。
- **影响**：所有 plugin-driven connector 走同一 scan-node 子类，简化 dispatch 逻辑。

---

### D-001 — 沿用 `SUPPORTS_PASSTHROUGH_QUERY`（原 D1）

- **日期**：2026-05-24
- **状态**：✅ 生效
- **关联**：[00-master-plan.md §5](./00-master-plan.md)
- **背景**：是否要为 SQL 透传以外的远程 query 类型（如 `query()` TVF）新增 SPI？
- **决策**：不新增。已有 `ConnectorCapability.SUPPORTS_PASSTHROUGH_QUERY` + `ConnectorTableOps.getColumnsFromQuery` 覆盖了主要场景，沿用。
- **影响**：无新增 API。

---

## 附录：决策模板

新增决策时复制以下模板到顶部（在 §详细记录 下方），并更新 §📋 索引表。

```markdown
### D-NNN — <一句话主题>

- **日期**：YYYY-MM-DD
- **状态**：✅ 生效 / 🟡 待评审 / ❌ 已废止（被 D-MMM 取代）
- **关联**：[文档章节链接]、[相关 task ID]
- **背景**：为什么需要做这个决策？触发场景是什么？
- **决策**：具体决定是什么？
- **替代方案**：考虑过哪些其他方案？为什么没选？
- **影响**：哪些代码 / 文档 / 流程会受影响？是否需要后续 follow-up？
```
