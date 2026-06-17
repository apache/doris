# 偏差日志（DV，append-only，时间倒序）

> 编号 `DV-NNN` **仅在本子项目内有效**，与上层 `../deviations-log.md` 独立。
> 规则（沿用 ../README §4.3）：原设计在落地中发现不可行/不必要时，**先**在此顶部记录，**再**改设计文档；禁止 silently 改设计。
> 每条格式：`DV-NNN`、日期、原计划位置（设计 §x / task Pn-Tnn）、为何不可行、新方案、影响范围。

---

## DV-002 — T1 等价性从「全等」放宽为「常见静态凭据路径全等 + 文档记超集」
- **日期**：2026-06-17 ｜ **原计划位置**：设计 §5 T1 / §6.4 验收 item 4 / WORKFLOW §5.2 T1（"新 == 旧 key/value **全等**"）。
- **为何不可行（P0-T01 取证）**：fe-filesystem `toHadoopConfigurationMap()`/`toBackendProperties().toMap()` 是 paimon 现走 fe-property 路（`buildObjectStorageHadoopConfig`）的**超集**，非全等：
  - **S3**：fe-filesystem 加 assume-role 分支（`fs.s3a.assumed.role.*`）+ 无 AK 时 anonymous/default `fs.s3a.aws.credentials.provider`；fe-property base 二者皆无。
  - **OSS/COS/OBS**：配置齐时一致（jindo/cosn/obs 块都在），但 fe-filesystem `fs.s3a.endpoint`/`.region` **无条件**发（`cfg.put`）vs fe-property **懒发**（仅非空时）。
  - **BE map**：fe-filesystem `toMap()` 多 `AWS_BUCKET`/`AWS_ROOT_PATH`/`AWS_CREDENTIALS_PROVIDER_TYPE`。
  - 均为 fe-filesystem 更完整的**有意设计**，非 bug。故字面「全等」测试必红。
- **新方案（用户 2026-06-17 定 A）**：认 fe-filesystem 为**新事实源**。**T1 = 常见静态凭据路径**（S3/OSS/COS/OBS 配齐 endpoint/region/AK/SK，无 role、无 vended）下各后端 key/value **全等**（含调优默认分叉 S3=50/3000/1000 vs 其它 100/10000/10000）+ **文档明记超集差异为「有意、更完整」**。P1-T03/T04 全量切换 fe-filesystem（含 P1-T04 BE 凭据也切 `toBackendProperties().toMap()`）。
- **影响**：设计 §5 T1 / §6.4 / WORKFLOW §5.2 T1 加（DV-002 修订）脚注；risks R-001 缓解更新；P1-T03/T04 的 T1 测试钉常见路径全等 + 注释超集（对照 fe-property 现产物）。

## DV-001 — P0-1 预期「fe-filesystem-api 已够用、无需门面」被证伪：缺 raw map → List<StorageProperties> 的 bind-all 入口
- **日期**：2026-06-17 ｜ **原计划位置**：设计 §4 P0-1 / §2.1 / 决策 D-003；task P0-T01；WORKFLOW §4.1 路径白名单（"唯一 fe-core 改动 = DefaultConnectorContext"）。
- **为何不可行（取证）**：
  - fe-filesystem `org.apache.doris.filesystem.properties.StorageProperties` 是**纯接口、无静态工厂**（无 `createAll`）。绑定靠各 `FileSystemProvider.bind(Map)`。
  - 仓内**不存在**任何「raw map → `List<fe-filesystem StorageProperties>`」聚合入口：`FileSystemPluginManager.providers` 私有，唯一出口是**首个命中**的 `createFileSystem`（返回 `FileSystem`，不是 StorageProperties，且非全量）；`FileSystemFactory.getProviders()` 包级私有且仅 ServiceLoader。
  - `DefaultConnectorContext` 当前**只持有 fe-core typed map 的 supplier**（`Map<fe-core StorageProperties.Type, fe-core StorageProperties>`），不持有 raw map；fe-filesystem 是**另一族** StorageProperties。raw map 可经现有 supplier 值的 `getOrigProps()`（fe-core `ConnectionProperties` 公有 getter）取回，**无需改构造点**；但**绑定步骤**仍需新代码。
  - 结论：实现 `getStorageProperties()`（返回 fe-filesystem 类型）**至少需要在 DefaultConnectorContext 之外再加一个 additive `bindAll(...)`**（fe-core `FileSystemPluginManager` 或 fe-filesystem-spi），无法塞进 `DefaultConnectorContext` 单文件 → 白名单需最小扩张。
  - 另：F1 等价性——fe-filesystem `toHadoopConfigurationMap()` 与 paimon 现走的 fe-property `buildObjectStorageHadoopConfig` 在**静态凭据常见路径全等**（COS/OSS/OBS 的 jindo/cosn/obs 块都在）；fe-filesystem 为**超集**（S3 assume-role/anon 分支额外键 + OSS/COS/OBS endpoint/region 无条件 vs 懒发）。非阻塞，但确认 fe-filesystem 为新事实源，T1 钉常见路径全等 + 记超集差异。
- **新方案（用户 2026-06-17 定向 A，记 D-009；已回写）**：在 fe-core `FileSystemPluginManager` 加 additive `public List<StorageProperties> bindAll(Map)`（镜像 `createFileSystem` 的 provider 循环，但 `bind` 全量收集而非首个命中 `create`）；`DefaultConnectorContext.getStorageProperties()` 调它，raw map 经现有 supplier 值的 `getOrigProps()` 取（不碰构造点）。已回写：设计 §4 P0-1/P0-2、WORKFLOW §4.1 白名单（+FileSystemPluginManager）、decisions D-009、risks R-004、tasks P0-T01/P0-T02。
  - **A（荐）**：守 D-003 架构（连接器消费 fe-filesystem-api typed StorageProperties）。在 fe-core `FileSystemPluginManager` 加 additive `public List<StorageProperties> bindAll(Map)`（镜像 `createFileSystem`），`DefaultConnectorContext.getStorageProperties()` 调它（raw map 经 `getOrigProps()` 取，不碰构造点）。fe-core 改动 = DefaultConnectorContext + FileSystemPluginManager 两文件、均纯新增。
  - **B**：同架构，但 `bindAll` 放 fe-filesystem-spi 静态（ServiceLoader）→ fe-core 仅改 DefaultConnectorContext；代价=改 fe-filesystem-spi（同样白名单外）+ 仅见内置 provider（storage 足够）。
  - **C（更简、偏离 D-003）**：不下发 typed 对象；加 `ConnectorContext.getStorageHadoopConfig(): Map<String,String>`，fe-core 用现有 typed map 单点算（与 hive/iceberg 同源、零漂移），paimon 调它。改动**确可**局限 DefaultConnectorContext 单文件；但连接器**不再**依赖 fe-filesystem-api（放弃 D-003 的「fe-connector → 仅 fe-filesystem-api」目标边）。
- **影响范围**：P0-T01 结论、P0-T02 / P1-T02 / P1-T03 / P1-T04 的绑定机制与白名单；不影响 P2/P3a。
