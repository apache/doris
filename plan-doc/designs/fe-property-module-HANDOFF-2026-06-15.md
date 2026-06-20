# fe-property 模块 —— 开发 HANDOFF（storage 首期）

> 日期：2026-06-15 ｜ 分支：catalog-spi-07-paimon ｜ 设计：`plan-doc/designs/fe-property-module-design-2026-06-15.md`
> 状态：**M1+M2+M3+M4 完成（uncommitted）**；M5 = 本文。

## M4 完成（paimon 连接器迁移，strangler-fig 阶段2）—— 2026-06-15

**做法=Hybrid（用户决策）**：连接器把 canonical 对象存储别名→`fs.s3a.*/fs.oss.*/fs.cosn.*/fs.obs.*` 翻译**委托给 fe-property**，保留连接器特有的 `paimon.*` 重写 + 原始 `fs./dfs./hadoop.` 透传。
- `fe-connector-paimon/pom.xml`：加 `fe-property` compile 依赖（plugin-zip assembly 未排除 → 随 fe-foundation 一起 child-first 打包）。
- `PaimonCatalogFactory.applyStorageConfig`：5 个 `applyCanonical*` 调用 → `StorageProperties.buildObjectStorageHadoopConfig(props).forEach(setter)`；删除 6 个 canonical 方法 + 全部别名数组/默认值/impl 常量 + 4 个仅 canonical 用的 helper（`firstNonBlankOrDefault/anyKeyStartsWith/containsToken/isClassAvailable`）。**文件 988→626 行（−362）**。保留 `firstNonBlank/nullToEmpty/USER_STORAGE_PREFIXES/FS_S3A_PREFIX`（透传/DLF/HMS 仍用）。
- 新增 fe-property 入口 `StorageProperties.buildObjectStorageHadoopConfig(Map)`：只对象存储（跳过 HDFS/broker/local/http→避开 createAll 的默认 HDFS+checkHaConfig 抛错），无匹配返回空 map（不抛）。

**验收**：`PaimonCatalogFactoryTest` **60/60 绿**、fe-property `StoragePropertiesTest` 5/5 绿、`tools/check-connector-imports.sh` PASS、checkstyle 0、`mvn -pl :fe-connector-paimon -am package -Dassembly.skipAssembly=true` EXIT 0。（连接器 HiveConf 须 `package` 阶段=hive-shade；故用 `package -Dassembly.skipAssembly=true` 跑 UT。）

**平价对齐：起步 57/60，3 处 divergence 按用户决策（保运行时行为=调 fe-property）已修：**
1. `fs.s3a.session.token` 漏 → fe-property `S3Properties` sessionToken 别名补 `AWS_TOKEN`（连接器有、legacy 无）。
2. `minio.endpoint required` 抛 → 删 `MinioProperties.setEndpointIfPossible` 抛（lenient）。
3. ak-without-sk 抛 → 删 `AbstractS3CompatibleProperties` 的 region/endpoint 必填抛 + ak/sk 一致性抛，并把 `fs.s3a.endpoint[.region]` 改**有值才发**（match 连接器宽松；conditional emit）。

**1 处 test 改（唯一反“保行为不变”的项，已在测试注释说明）**：`buildHadoopConfigurationEmitsCosRegionUnconditionally` 断言 `fs.cosn.bucket.region` 由 `""`→`ap-beijing`。原因=fe-property(=legacy) **从 `cos.<region>.myqcloud.com` 端点派生区域**（更正确），连接器旧 re-port 留空是简化；迁移后该 cosn catalog 得到正确区域（良性/更优）。

**M4 偏离记录（fail-loud，影响 fe-property 全体消费方）**：为对齐连接器宽松行为，fe-property 的 S3 兼容校验已**放宽**=不再因 region/endpoint 空而抛、不再强制 ak/sk 同设、endpoint/region 空则省略对应 fs.s3a key。这使 fe-property 比 legacy 宽松；未来 fe-core 迁入时若需严格校验需另行评估。

**仍未做**：① 引擎启动同步 `PropertyConfigLoader.hadoopConfigDir`/`AzureProperties.azureBlobHostSuffixes` 两静态（默认值对多数部署 OK）；② docker e2e（`enablePaimonTest=true`）验 minio/oss/s3/cos/obs/dlf paimon catalog 真实读 + plugin-zip 实际 bundle 了 fe-property/fe-foundation（本地仅 UT+assembly 配置推断）；③ 其它连接器（hive/iceberg/hudi）后续同法迁移。

## 已迁移连接器审计：es / jdbc / trino / maxcompute —— 是否有同类 storage-property 重复？ —— 2026-06-15

**结论：四个都没有 storage-property 重复逻辑，无需迁移到 fe-property。**

证据（`fe/fe-connector/fe-connector-{es,jdbc,trino,maxcompute}/src/main` 全量扫描）：

| 连接器 | 主类数 | `fs.s3a`/`fs.oss`/`fs.cosn`/`fs.obs`/`S3AFileSystem` | `StorageProperties`/`hadoop.conf.Configuration`/`applyCanonical*` | 引擎 storage 桥（`getBackendStorageProperties`/`normalizeStorageUri`/`vendStorageCredentials`） | 对象存储凭据别名（`minio.`/`s3.access`/`oss.access`/`AWS_ACCESS`） | 结论 |
|---|---|---|---|---|---|---|
| **es** | 20 | 无 | 无 | 无 | 无 | ES REST 直连（hosts/user/password/ssl/keyword_sniff/mapping），不读对象存储数据文件 → 无重复 |
| **jdbc** | 26 | 无 | 无 | 无 | 无 | JDBC 直连（jdbc_url/driver_url/driver_class/user/password/connection_pool_*），不读对象存储 → 无重复 |
| **trino** | 13 | 无 | 无 | 无 | 无 | Trino 元连接器，存储交由 Trino 自身连接器；唯一 hadoop 命中=注释 → 无重复 |
| **maxcompute** | 15 | 无 | 无 | 无 | 无 | ODPS 直连。`mc.access_key`/`mc.endpoint`/`mc.region`/`mc.session_token` 是 **ODPS SDK 凭据**（com.aliyun.odps），**非对象存储**；fe-property 不覆盖 ODPS。`bucket` 命中=MaxCompute tunnel 分片号，非存储桶 → 无重复 |

**为什么**：这 4 个都是"直连活系统"连接器（ES / 关系库 / Trino / ODPS），数据不来自 S3/OSS 上的 parquet/orc 文件，所以从不构建 `fs.s3a.*` Hadoop storage config——这正是 paimon `applyStorageConfig` 那类重复的来源。`*ConnectorProperties` 都是纯连接器域常量持有类。

**真正会有该重复的是"读对象存储数据文件的湖仓连接器"**：paimon（已迁，本次）、以及 **hive / iceberg / hudi**（不在本次 4 个名单内，是后续 M-next 的候选；它们若有 `applyStorageConfig`-类手抄段，同法 Hybrid 迁移）。

---

## （以下为 M1-M3 原始 HANDOFF）

## 1. 已完成（验证态）

- **M1 模块骨架**：新建 `fe/fe-property`（artifactId `fe-property`，包 `org.apache.doris.property[.storage|.common]`）；注册进 `fe/pom.xml` `<modules>`（fe-foundation 之后）+ dependencyManagement。
- **M2 拷贝并重适配 storage**：24 个源文件搬入并改造（见 §3 改造清单）。
- **M3 测试**：`StoragePropertiesTest` 5 用例（MinIO→fs.s3a.* 映射、MinIO→AWS_* 映射、S3 选型+URI 归一化、guessIsMe 顺序、HTTP 空配置）。

**验收证据：**
- `mvn -pl fe-property -am compile`：成功；**checkstyle 0**。
- `mvn -pl fe-property -am package`：`doris-fe-property.jar`（96KB，26 class）；`unzip -l` **不含** `org/apache/hadoop`、`software/amazon`、`com/amazonaws`、`org/apache/iceberg`、`org/apache/paimon`、`org/apache/hudi` —— **重依赖不进 jar ✓**。
- `mvn -pl fe-property -am test`：`Tests run: 5, Failures: 0, Errors: 0, Skipped: 0`（surefire 报告确认真跑，已 `-Dmaven.build.cache.enabled=false`）。
- fe-core 旧 `datasource/property` **零改动**（两套并存）。

## 2. 对外 API（连接器消费契约）

```java
StorageProperties sp = StorageProperties.createPrimary(rawProps);   // 或 createAll(...)
sp.getType();                                  // Type 枚举
Map<String,String> fsConf = sp.getHadoopConfigMap();        // fs.s3a.*/fs.cosn.*/fs.azure.*/dfs.* —— 连接器灌进自己的 Configuration
Map<String,String> beProps = sp.getBackendConfigProperties();  // AWS_*/hadoop.* —— 发 BE
sp.validateAndNormalizeUri("s3a://b/k");       // -> "s3://b/k"
// + 各子类类型化 getter（getEndpoint/getRegion/getAccessKey/...）
```
依赖：**仅 fe-foundation**（@ConnectorProperty 引擎/ParamRules/StoragePropertiesException）+ commons-lang3 + commons-collections4 + guava + log4j-api + **hadoop-common(provided)**。

## 3. 改造清单（相对 fe-core 旧 storage）

1. 包 `org.apache.doris.datasource.property.*` → `org.apache.doris.property.*`（连接器 import-gate 禁 `datasource.*`）。
2. **配置产物 Configuration → Map**：`Configuration hadoopStorageConfig` → `Map<String,String> hadoopConfigMap` + `getHadoopConfigMap()`；所有 `conf.set` → `map.put`；FS impl 全是字符串字面量（无 hadoop 类型）。null map = 该后端无 hadoop 配置（如 HTTP），保留旧 skip 语义。
3. `UserException`(fe-common) → `StoragePropertiesException`(fe-foundation，unchecked)；S3URI 的 `new UserException(throwable)` → `(msg, cause)`。
4. **S3Properties 剥离 fe-core 云/存储策略机器**：删 `getObjStoreInfoPB`(proto)、`getS3TStorageParam`(thrift)、`getCredProviderTypePB/getTCredProviderType`、`requiredS3Properties/checkRequiredProperty/requiredS3PingProperties/convertToStdProperties/optionalS3Property`(DdlException)、`getAwsCredentialsProvider V1/V2`(aws-sdk)、`Env` 内类 + 云常量。连接器自行用自带 SDK 构造凭据/PB。
5. `getAwsCredentialsProvider`(aws-sdk) 从 AbstractS3Compatible + OSS/COS/OBS/GCS 移除；保留类型化凭据 getter + `AwsCredentialsProviderMode` 枚举（纯枚举）。
6. **HdfsProperties/HdfsCompatibleProperties 去鉴权对象**：删 `HadoopAuthenticator` 构造 + `hadoopAuthenticator` 字段/getter（鉴权是连接器/引擎职责，走 `ConnectorContext.executeAuthenticated`）；HDFS 仍解析 kerberos 属性进 map。
7. **fe-common Config 解耦**：`Config.hadoop_config_dir` → `PropertyConfigLoader.hadoopConfigDir`（可设静态，默认 `$DORIS_HOME/plugins/hadoop_conf/`）；`Config.azure_blob_host_suffixes` → `AzureProperties.azureBlobHostSuffixes`（可设静态，内联默认 8 项）。
8. **loadConfigFromFile 保留并迁移**：新建 `PropertyConfigLoader`（hadoop `Configuration` 解析 XML，**hadoop-common=provided**）；`ConnectionProperties.loadConfigFromFile` 改用它，仍返回 Map。
9. `HdfsClientConfigKeys`（hadoop-hdfs-client）4 常量内联进 HdfsPropertiesUtils。
10. `S3URI` 拷入 `org.apache.doris.property.storage`。
11. `HttpProperties` 误引 `org.apache.hudi...MapUtils` → `commons-collections4.MapUtils`（`isNullOrEmpty`→`isEmpty`）。

## 4. 偏离设计/需注意（fail-loud）

- **hadoop-common 是 provided（非"零 hadoop"）**：设计文档曾期望零 hadoop；因首期纳入 `loadConfigFromFile`（须 hadoop 解析 XML 配置文件）而保留为 provided。**仍满足硬约束**（不进 jar，已验证）。唯一 hadoop 用处=`PropertyConfigLoader`/`loadConfigFromFile`；其余全 hadoop-free。若要彻底零 hadoop，可把 loadConfigFromFile 改注入式 loader（引擎提供）。
- **S3 v2 凭据版本开关未移植**：`S3Properties.initializeHadoopStorageConfig` 只发默认(v1)assumed-role 配置（provider 类用 FQN 字符串硬编码）。`Config.aws_credentials_provider_version=v2` 的分支属 fe-core Config 行为，连接器场景不适用。
- **引擎需在启动时同步两个可设静态**（否则用默认值）：`PropertyConfigLoader.hadoopConfigDir = Config.hadoop_config_dir`、`AzureProperties.azureBlobHostSuffixes = Config.azure_blob_host_suffixes`。首期未接（默认值对绝大多数部署正确）。
- **S3Properties 剥离的云/存储策略方法**：仅 fe-core legacy 调用，连接器不需要；保留在 fe-core 旧类（并存）。

## 5. 下一步（M4：paimon 连接器迁移，strangler-fig 阶段2）

1. `fe-connector-paimon` pom 加 `fe-property` 依赖；plugin-zip assembly include `fe-property`（纯小 jar）。
2. `PaimonCatalogFactory`：删 `applyStorageConfig`/`applyCanonicalMinioConfig`/OSS/COS/OBS 块/`MINIO_*_ALIASES` 重抄段，改 `StorageProperties.create(props).getHadoopConfigMap().forEach(conf::set)`。
3. `tools/check-connector-imports.sh` 通过（fe-property 在允许包根）。
4. paimon minio/oss/s3 回归（`external_table_p0/paimon`）证明重复消除 + 无行为回归。
5. 引擎启动期同步 §4 的两个静态。
6.（可选）更全的逐后端平价 sweep：新 `getHadoopConfigMap()`/`getBackendConfigProperties()` vs fe-core 旧 `getHadoopStorageConfig()`/`getBackendConfigProperties()` 逐键断言。

## 6. 文件清单（新增）

```
fe/fe-property/pom.xml
fe/fe-property/src/main/java/org/apache/doris/property/
    ConnectionProperties.java          PropertyConfigLoader.java
    common/AwsCredentialsProviderMode.java
    storage/  (StorageProperties, AbstractS3CompatibleProperties, ObjectStorageProperties,
               S3/OSS/COS/OBS/Minio/GCS/Ozone/Hdfs/HdfsCompatible/OSSHdfs/Azure/Broker/Local/Http Properties,
               S3PropertyUtils, HdfsPropertiesUtils, AzurePropertyUtils, S3URI, exception/AzureAuthType)
fe/fe-property/src/test/java/org/apache/doris/property/storage/StoragePropertiesTest.java
fe/pom.xml  (modules + dependencyManagement: +fe-property)
```
