# 设计文档：独立 `fe-property` 模块（storage 优先，strangler-fig 并存迁移）

> 日期：2026-06-15 ｜ 分支：catalog-spi-07-paimon
> 前置研究：`plan-doc/reviews/property-module-extraction-feasibility-2026-06-14.md`
> 本设计遵循"先研究、后设计文档、批准后编码"流程。**编码尚未开始，待批准。**

---

## 1. 目标（Goals）

1. 新建独立 FE 模块 **`fe-property`**，承载"数据源属性的**整理 + 校验 + 归一化**"逻辑，**产出归一化结果**（类型化对象 + 归一化 Map），供各 connector / fe-core 复用。
2. **彻底消除连接器侧的属性解析重复**：当前 `PaimonCatalogFactory` 因无法 import fe-core 的 `StorageProperties`，**手工重抄**了 `applyStorageConfig` / `applyCanonicalMinioConfig` / OSS/COS/OBS 块 / `MINIO_*_ALIASES`——这正是 `47bfe201c7c` minio bug 的来源。新模块让连接器改为直接调用，杜绝漂移。
3. **最终 jar 不含重型依赖**（hadoop/hdfs/aws/iceberg/paimon）。本设计通过"只产出 Map、不构造 live 对象"从根上让这些重依赖**根本不进入**新模块（比 `provided` 更彻底）。
4. **strangler-fig 并存**：**不动** fe-core 现有 `datasource/property`；新模块拷贝并重适配相关代码，两套并存；连接器逐个迁移到新模块；全部迁完后再删 fe-core 旧代码。

## 2. 非目标（Non-Goals）

- 不重写/不删除 fe-core 现有 `org.apache.doris.datasource.property.*`（本期零改动）。
- 首期**不**纳入 `metastore` 与 `fileformat`（仅 `storage`）。metastore 留作下一迭代（同样的纯解析范式），fileformat 不在连接器 catalog 属性范畴，暂不规划。
- 新模块**不**构造任何 live 对象：不产出 Hadoop `Configuration`、不产出 aws-sdk `AwsCredentialsProvider`、不产出 iceberg/paimon `Catalog`、不产出 thrift `TS3StorageParam` / proto `ObjectStoreInfoPB`。这些由消费方（连接器/BE/fe-core）用各自已有的重型依赖构建。
- 不改 BE。

## 3. 关键决策（已与用户确认）

| 编号 | 决策 | 选择 |
|---|---|---|
| D1 | 对外形态 | **类型化对象 + 归一化 Map**：每个数据源解析成不可变 POJO（仅 String/基本类型字段），既有 getter，又提供 `getHadoopConfigMap()` / `getBackendPropertiesMap()` |
| D2 | 首期范围 | **仅 `storage`**（S3/OSS/COS/OBS/MinIO/GCS/Ozone/HDFS/OSS-HDFS/Azure/Broker/Local/Http） |
| D3 | 绑定/解析引擎 | **复用 fe-foundation 的 `@ConnectorProperty` 引擎**（`ConnectorPropertiesUtils.bindConnectorProperties` + `ParamRules`）。现 `StorageProperties` 已在用，零新增成本 |
| D4（派生）| 包根 | **`org.apache.doris.property`**（不可用 `datasource.*`，见约束 C1） |
| D5（派生）| 依赖上限 | 仅 `fe-foundation` + commons-lang3 + guava；**不依赖** fe-common/fe-core/fe-thrift/fe-grpc/hadoop/aws（见约束 C2） |

## 4. 约束（Constraints）

- **C1（包根硬约束）**：连接器 import-gate `tools/check-connector-imports.sh` 禁止连接器 import `org.apache.doris.(catalog|common|datasource|qe|analysis|nereids|planner).*`。因此新模块**不能**放在 `org.apache.doris.datasource.property`（`datasource.*` 被禁），改用 **`org.apache.doris.property.storage`**。`foundation.*` 不在黑名单，可放心依赖。
- **C2（依赖上限）**：要让连接器可直接 import，新模块不得依赖被 gate 禁止的模块。即**不能用 `fe-common`**（无 `UserException` → 改用 `foundation.StoragePropertiesException`）。
- **C3（no split-package）**：新包根 `org.apache.doris.property.*` 与旧 `org.apache.doris.datasource.property.*` 完全不同，两 jar 不会劈分同一包，并存合法。
- **C4（连接器自带重依赖）**：连接器 plugin-zip 各自 child-first 自带 hadoop/aws/paimon。新模块产出的是 Map，连接器用自带的 hadoop/aws 把 Map 灌进 Configuration / 构造客户端。

## 5. 架构（Architecture）

### 5.1 模块依赖图（首期）

```
fe-foundation  (零重依赖：@ConnectorProperty 引擎 / ParamRules / StoragePropertiesException)
      ▲
      │ compile
   fe-property  ←── 新模块（org.apache.doris.property.storage），仅 +commons-lang3/guava
      ▲
      │ compile（连接器逐个加依赖）
fe-connector-paimon / -hive / -iceberg / …（各自带 hadoop/aws/SDK）
      …（最终）fe-core 也可依赖 fe-property，迁完后删 fe-core 旧 property
```

构建顺序：`fe/pom.xml` 的 `<modules>` 把 `fe-property` 置于 `fe-foundation` 之后、`fe-connector` 之前；`fe-property` 只依赖 `fe-foundation`，Maven 依赖图自然满足。

### 5.2 并存与迁移（strangler-fig）

```
阶段0(现状):  连接器手工重抄 applyStorageConfig/minio  ←─ 漂移源
阶段1(本设计): 新建 fe-property(storage)。两套并存，fe-core 旧 property 不动。
阶段2:        paimon 连接器改用 fe-property（删 applyStorageConfig 重抄段）→ 回归验证。
阶段3:        hive/iceberg/hudi/... 连接器逐个迁移。
阶段4:        fe-core 非 SPI 旧 catalog 迁到 SPI 后亦改用 fe-property。
阶段5:        所有使用方迁完 → 删除 fe-core org.apache.doris.datasource.property.storage。
```

## 6. API / 接口设计

### 6.1 新 `StorageProperties` 契约（`org.apache.doris.property.storage`）

保留（语义照搬旧类，仅换包/换异常）：
- `static StorageProperties create(Map<String,String> props)` —— 主存储（= 旧 `createPrimary`）
- `static List<StorageProperties> createAll(Map<String,String> props)`
- `Type getType()` / `String getStorageName()`
- `String validateAndNormalizeUri(String url)` / `String validateAndGetUri(Map loadProps)`
- `@ConnectorProperty` 字段 + getter（endpoint/region/accessKey/secretKey/sessionToken/usePathStyle/maxConnections/...，按各子类）
- `enum Type` / `FS_*_SUPPORT` 常量 / `guessIsMe(...)` 工厂探测

**替换（核心改造）——把 live 对象换成 Map：**

| 旧（fe-core，产出 live 对象/重类型） | 新（fe-property，产出 Map/纯类型） |
|---|---|
| `Configuration getHadoopStorageConfig()` | `Map<String,String> getHadoopConfigMap()`（fs.s3a.*/fs.oss.*/fs.cosn.*/fs.obs.*/fs.azure.*/dfs.* 键值；连接器自行 `conf.set`） |
| `protected void initializeHadoopStorageConfig()`（写 Configuration 字段） | `protected void buildHadoopConfigMap()`（写内部 `Map` 字段） |
| `AwsCredentialsProvider getAwsCredentialsProvider()`（aws-sdk 类型） | **移除**；暴露类型化凭据 getter + `AwsCredentialsProviderMode` 枚举，连接器用自带 aws-sdk 构造 provider |
| `S3Properties.getObjStoreInfoPB()`（proto）/`getS3TStorageParam()`（thrift） | **不移植**（fe-core 云上/存储策略专属，留在旧 property） |
| `throws UserException`（fe-common） | `throws StoragePropertiesException`（fe-foundation） |

保留并仍返回 Map（旧已是 Map，直接搬）：
- `Map<String,String> getBackendConfigProperties()` → 改名/保留为 `getBackendPropertiesMap()`（AWS_*/hadoop.* 规范键，给 BE）
- `Map<String,String> getBackendConfigProperties(Map runtime)`（叠加运行时）

### 6.2 连接器消费样例（minio 重复消失）

```java
// 旧（PaimonCatalogFactory，手工重抄 ~150 行 applyStorageConfig/applyCanonicalMinioConfig/...）
applyStorageConfig(props, conf::set);

// 新
StorageProperties sp = StorageProperties.create(props);
sp.getHadoopConfigMap().forEach(conf::set);        // fs.s3a.*/fs.oss.* 等，由 fe-property 统一推导
// 需要发给 BE 时：
Map<String,String> beProps = sp.getBackendPropertiesMap();
```

## 7. 数据流（Data Flow）

```
用户 catalog/load props (raw Map)
   │
   ▼  fe-property: StorageProperties.create(raw)
@ConnectorProperty 反射绑定 + ParamRules 校验 + guessIsMe 选型 + URI 归一化
   │
   ├── getHadoopConfigMap()      → 连接器灌进自带 Configuration（catalog FS / SDK 客户端）
   ├── getBackendPropertiesMap() → 连接器发给 BE（AWS_*/hadoop.* 规范键）
   └── 类型化 getter             → 连接器按需自取（如构造 aws provider / 取 endpoint）
```

## 8. 依赖与打包（pom）

`fe-property/pom.xml`（仿 fe-foundation/fe-catalog 头）：
- parent=`fe`（`${revision}`），packaging=jar，finalName=`doris-fe-property`，test-jar、javadoc skip、release source profile。
- **compile**：`fe-foundation`（`${project.version}`）、`commons-lang3`、`guava`、`log4j-api`。
- **可选 provided**：`hadoop-hdfs-client`（仅 `HdfsPropertiesUtils` 用 `HdfsClientConfigKeys` 的 4 个常量；二选一：①`provided`（不打包）；②直接内联这 4 个 key 字符串，连这个依赖都省掉——**推荐内联**，使新模块零 hadoop 依赖）。
- **无** hadoop-common / aws-sdk / iceberg / paimon / fe-common / fe-thrift / fe-grpc。
- `fe/pom.xml`：`<modules>` 加 `fe-property`（fe-foundation 之后）；dependencyManagement 加 `fe-property` 条目。
- 连接器 plugin-zip 的 assembly 需 include `fe-property`（纯小 jar，无重依赖可打包）。

> 由于新模块本就不引入重型依赖，用户"重依赖不进 jar"的约束被**结构性满足**，无需依赖 `provided` 排除技巧。

## 9. 与现有 `ConnectorContext` 桥的关系

fe-core 现已在 `ConnectorContext` 暴露 `getBackendStorageProperties()` / `normalizeStorageUri()` / `vendStorageCredentials()`——让连接器"回调引擎做归一化"以绕开 import-gate。`fe-property` 让连接器**直接做**归一化（无需回调引擎），未来可逐步替代这些桥方法（含 vended：把 per-table token map 直接喂 `StorageProperties.create`）。**本期不动这些桥**，仅新增 fe-property 并迁移 `applyStorageConfig` 重抄段；桥方法待全面迁移后再评估下线。

## 10. 边界与坑（Edge Cases）

1. **`S3URI`**：旧 `S3PropertyUtils` 依赖 fe-core 的 `common.util.S3URI`。新模块**拷贝** `S3URI` 进 `org.apache.doris.property.storage`（纯 java，仅把 UserException 换 StoragePropertiesException）。
2. **HdfsClientConfigKeys**：见 §8，推荐内联 4 个 key 常量，避免 hadoop 依赖。
3. **Azure OAuth**：旧 `AzureProperties` OAuth 分支构造 `Configuration`；新版产出 `fs.azure.account.oauth.*` Map。
4. **鉴权（kerberos）**：旧 `HdfsProperties` 构造 `HadoopAuthenticator`（fe-common）。新版**只解析**鉴权属性（auth type / principal / keytab / 归一化 hadoop.* Map），**不构造** authenticator——authenticator 是运行时对象，由连接器/引擎（`ConnectorContext.executeAuthenticated`）负责。借此甩掉 fe-common 依赖。
5. **`guessIsMe` 选型顺序**：MinIO 让位 Azure/COS/OSS/S3 的探测顺序需原样保留（否则误判后端）。
6. **`HttpProperties` 误引**：旧版误 import `org.apache.hudi...MapUtils`；新版改 commons-collections4，避免拖 Hudi。
7. **凭据 provider 构造下移**：`AwsCredentialsProviderFactory`（aws-sdk）不进 fe-property；连接器需要 provider 时自建（多数场景连接器只需把 Map 灌 Configuration / 发 BE，并不需要 provider 对象）。

## 11. 测试与回滚

- **平价测试（关键）**：对每个 storage 子类，用同一组 raw props 跑"新 `getHadoopConfigMap()`/`getBackendPropertiesMap()`" vs "旧 `getHadoopStorageConfig()` 转成 Map / `getBackendConfigProperties()`"，断言**键值完全一致**（含 minio/oss/cos/obs/azure/hdfs）。这是防漂移的核心闸。
- **单元测试**：选型 `guessIsMe` 顺序、URI 归一化、ParamRules 校验、各 alias 解析、minio.* 专属前缀检测。
- **连接器迁移验证**：paimon 改用 fe-property 后，跑 `external_table_p0/paimon` 回归（minio/oss/s3 catalog 读）。
- **回滚**：strangler-fig 天然可回滚——任一连接器迁移出问题，单独回退该连接器到旧 `applyStorageConfig`，fe-property 与旧 property 并存互不影响。

## 12. 风险与备选

- **风险1：平价漂移**。新 Map 推导若与旧 Configuration 落键有差→连接器/BE 行为变。缓解=§11 平价测试逐键断言；首迁 paimon 用真实 minio/oss 回归。
- **风险2：代码拷贝双维护**。并存期 storage 逻辑两份。缓解=并存窗口尽量短、优先迁 paimon 验证范式、平价测试钉死一致；新代码为权威，旧代码进入"只读冻结"。
- **风险3：plugin-zip 漏打 fe-property**。缓解=迁移连接器时同步改 assembly，加一条 smoke（plugin 加载 `org.apache.doris.property.storage.StorageProperties` 不 ClassNotFound）。
- **备选A（被否）**：原地 lift-and-shift 整个 datasource.property → 循环依赖 + vendored glue + 重型 SDK，已证不可行（见前置研究报告）。
- **备选B**：把新代码直接塞进 fe-foundation。否决——会破坏 fe-foundation 零依赖基座（即使首期 storage 很轻，后续 metastore 会重）。保持 fe-foundation 为纯基座，fe-property 为其上一层。

## 13. 有序 TODO 列表

**M1 — 模块骨架**
1. 新建 `fe/fe-property/pom.xml`（仿 fe-foundation；compile=fe-foundation+commons-lang3+guava+log4j-api）。
2. `fe/pom.xml` `<modules>` 加 `fe-property`（fe-foundation 后）；dependencyManagement 加条目。
3. 建包 `org.apache.doris.property.storage`（+ `storage` 内 util）。

**M2 — 拷贝并重适配 storage（核心）**
4. 拷贝 `ConnectionProperties`（基类，去掉 `loadConfigFromFile` 对 fe-common 的依赖或改 foundation 等价）→ 新包。
5. 拷贝 `StorageProperties` 抽象类：把 `getHadoopStorageConfig():Configuration` 改 `getHadoopConfigMap():Map`、`initializeHadoopStorageConfig()` 改 `buildHadoopConfigMap()`、`UserException`→`StoragePropertiesException`、`getBackendConfigProperties`→`getBackendPropertiesMap`。
6. 拷贝 `AbstractS3CompatibleProperties` + `ObjectStorageProperties` + `Abstract*`：移除 `getAwsCredentialsProvider()`（aws-sdk），改产出凭据 getter；S3 系 fs.s3a.* 改写进 Map。
7. 拷贝 13 个具体类（S3/OSS/COS/OBS/MinIO/GCS/Ozone/Hdfs/OSSHdfs/Azure/Broker/Local/Http）：逐个把 `conf.set(...)` 改为 `map.put(...)`；保留 `guessIsMe` 顺序；修 HttpProperties 的 hudi 误引。
8. 拷贝 util：`S3PropertyUtils`、`HdfsPropertiesUtils`（内联 `HdfsClientConfigKeys` 4 常量）、`AzurePropertyUtils`、**`S3URI`**（新拷贝）、`exception/AzureAuthType`。
9. 移除 `S3Properties` 的 proto/thrift 方法（不移植）。

**M3 — 测试**
10. 平价测试：每个子类新 vs 旧逐键断言（依赖 fe-core 旧类做基准，可放 fe-property 的 test 或一个临时对照 test）。
11. 单元测试：guessIsMe 顺序、URI 归一化、minio.* 检测、ParamRules、alias。
12. checkstyle 0 告警。

**M4 — 首个连接器迁移（paimon，验证范式）**
13. `fe-connector-paimon` 加 `fe-property` 依赖；plugin-zip assembly include fe-property。
14. `PaimonCatalogFactory`：删 `applyStorageConfig`/`applyCanonicalMinioConfig`/OSS/COS/OBS 块/`MINIO_*_ALIASES`，改为 `StorageProperties.create(props).getHadoopConfigMap().forEach(conf::set)`。
15. 跑 import-gate（`tools/check-connector-imports.sh`）确认无违规；跑 paimon 回归（minio/oss/s3）。

**M5 — 文档与收尾**
16. 更新 plan-doc：迁移路线、并存说明、下线计划（metastore 下一迭代、桥方法评估）。
17. 记录"旧 datasource.property.storage 冻结、以 fe-property 为权威"。

## 14. 验收标准（强约束，可独立 loop 验证）

- `mvn -pl fe/fe-property -am package` 成功；`unzip -l doris-fe-property.jar` **不含** `org/apache/hadoop/**`、`software/amazon/**`、`org/apache/iceberg/**`、`org/apache/paimon/**`。
- 平价测试全绿（新 Map ≡ 旧落键）。
- `tools/check-connector-imports.sh` 通过；paimon 连接器删除 `applyStorageConfig` 后编译通过。
- paimon minio/oss/s3 catalog 回归通过（证明重复消除且无行为回归）。
- fe-core 旧 `datasource/property` 零改动（并存）。
