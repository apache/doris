# ✅ Task List — 清理 fe-core `datasource/property/{common,metastore}`

> **本任务的唯一进度清单**。完成一项即把 `[ ]` 勾成 `[x]`（随 commit 更新）。
> **「怎么做」看 [`design.md`](./design.md)，「下一步做什么」看 [`HANDOFF.md`](./HANDOFF.md)，
> 「还没定的事」看 [`open-decisions.md`](./open-decisions.md)。**
> **⚠️ 行号信 HEAD 不信文档**（基线 = 2026-07-28 / `3468d905eb3`）。
> 状态：⬜ 未开始 ｜ 🚧 进行中 ｜ ✅ 完成 ｜ ⛔ blocked。编号永不复用。

---

## 🎯 总判据（唯一的「做完了没」标准）

```bash
R=/mnt/disk1/yy/git/wt-catalog-spi

# ① fe-core 不再有 metastore 属性代码（基线 5 文件 → 0）
ls $R/fe/fe-core/src/main/java/org/apache/doris/datasource/property/metastore/ 2>/dev/null | wc -l   # → 0
test -f $R/fe/fe-core/src/main/java/org/apache/doris/datasource/property/ConnectionProperties.java   # → 不存在

# ② 全仓无残留引用（含 groovy / 注释 / 文档）
grep -rIn 'MetastoreProperties\|MetastorePropertiesFactory\|AbstractMetastorePropertiesFactory\|TrinoConnectorPropertiesFactory\|ConnectionProperties\|checkMetaStoreAndStorageProperties\|getMetastoreProperties' \
     $R/fe $R/regression-test $R/tools $R/gensrc --exclude-dir=target        # → 空

# ③ common/ 只剩活代码（OD-2 已拍板 A ⇒ 本条是判据）
#    ⚠️ 唯一允许的命中：iceberg 测试里的 createV2Unpartitioned（表格式 v2 同名，误报）
grep -rn 'createV2\|createDefaultV2\|getAwsCredentialsProvider()' $R/fe --exclude-dir=target

# ④ 编译 + 门禁全绿（每步都要，不只最后一次）
mvn -f $R/fe/pom.xml -T 1C clean test-compile -Dcheckstyle.skip=true
mvn -f $R/fe/pom.xml -pl fe-core checkstyle:check
```

**⚠️ 五条纪律**（本仓库已知踩坑，见 `design.md` §5）：
1. 删除类改动**不能只信增量编译** → 每步先 `rm -rf fe-core/target/{classes,test-classes}`。
2. 全反应堆**必须含测试源**，**禁 `-Dmaven.test.skip=true`**；且必须 `-Dcheckstyle.skip=true`
   （否则 checkstyle 扫 generated-sources 退化成平方级，构建卡死）。
3. checkstyle 的 `UnusedImports` 是**阻塞门禁**，改为**只对改动模块**单独跑 `checkstyle:check`。
4. 🆕 **`-pl` 必须配 `-am`**（否则兄弟模块的 `${revision}` 解析不了 → 假错），且 surefire 2.22.2
   认的是 **`-DfailIfNoTests=false`**（不是 `-DfailIfNoSpecifiedTests`）。2026-07-28 两条都实测踩过。
5. 🆕 **但 `-am test` 对「依赖链经过 shade 模块」的连接器跑不通** —— 例如
   `-pl fe-connector/fe-connector-iceberg -am test` 会在 `fe-connector-hms` 炸
   「package org.apache.hadoop.hive.metastore.api does not exist」，因为 shaded jar 只在 `package`
   阶段产出，`test` 够不着。**这是既有怪癖，已 stash 到干净 HEAD 复现确认，不是你改坏的。**
   对这类模块用全反应堆 `test-compile` 覆盖；`fe-connector-api` 不在该链上，`-am test` 正常。

---

## 阶段 0 — 调研（✅ 已完成）

- [x] **FPC-00** 事实基线：8 路并行侦察（172 条 finding）+ 3 路独立设计（minimal / architectural / risk）
      + 6 项对抗验证（**2 项被推翻**）→ [`design.md`](./design.md)
      - 🔴 **被推翻的初判必读**：`common/` **不是** `fe-filesystem-s3-base` 的可替换双胞胎
        （两条活的行为差异，`design.md` §3.3）。这是本轮最容易重犯的错。
- [x] **FPC-01a** 承重结论逐条复核（本文作者亲验，非仅采信 agent）：
      `metastore/` 唯一 import 者 · 两道门皆不可达 · `ConnectionProperties` 在 `StorageAdapter:856`
      仅是 javadoc · `getAwsCredentialsProvider()` 零调用者 · fe-core pom 无 s3-base 依赖 ·
      **四个连接器构造函数均不碰 storage ⇒ null-supplier 窗口今天不可达**

---

## 阶段 1 — 🔴 前置拍板（**不拍板不许开工 FPC-03**）

- [x] **FPC-01** ✅ **OD-1 = A（抛异常），用户 2026-07-28 已拍板**
      （问题：删掉 metastore 后，`resolveDerivedStorageDefaults()` 的 null-supplier 分支要
      **fail-loud（`throw`）** 还是 **fail-silent（`return emptyMap()`）**？详见
      [`open-decisions.md`](./open-decisions.md) **OD-1**）
      - **落地为 A**：`throw new IllegalStateException(...)`，随 FPC-03 一起提交。
      - 配守卫测试 `CatalogPropertyPluginStorageDerivationTest.unwiredSupplierFailsLoudInsteadOfDerivingNothing`，
        **已做变异验证**：把 `throw` 改成 `return emptyMap()` → 该用例变红（其余三例不受影响），改回 → 复绿。
      - （先行按推荐值落地，随后获用户明确确认，实现无需改动。）
      - ⚠️ 调研报告原提的缓解方案「把 supplier 安装语句提前」**经复核修不干净**
        （lambda 读到的 `connector` 字段仍是旧值/null），已在 OD-1 中列为**不推荐**，未采纳。

---

## 阶段 2 — 删死代码（独立，可先做，也可整项丢弃）

- [x] **FPC-02** ✅ 删 AWS provider 的死构造臂（**实删 159 行，零行为变更**）
      > ✅ **OD-2 拍板 = A（直接删）**，用户 2026-07-28 明确，**推翻了我的推荐 B**。
      > 已知并接受的代价：`upstream-apache/master` @ `2faf819fa89` 上这段**是活的**
      > （`connectivity/AbstractS3CompatibleConnectivityTester.java:71`、
      > `property/common/IcebergAwsClientCredentialsProperties.java:84`——本分支已把这两个消费者
      > 连同整个 `datasource/connectivity/` 包删光），而 `StorageAdapter.java` 两边都在、走三方合并
      > ⇒ 上游改动该区域时 rebase 会出 modify/delete 冲突，**届时保留删除**。
      - **文件**：
        - `fe/fe-core/src/main/java/org/apache/doris/datasource/storage/StorageAdapter.java`
        - `fe/fe-core/src/main/java/org/apache/doris/datasource/property/common/AwsCredentialsProviderFactory.java`
        - （仅注释）`fe/fe-connector/fe-connector-iceberg/.../AwsCredentialsProviderModes.java:36-37`、
          `IcebergConnector.java:1113-1114`
      - **删**：
        - `StorageAdapter`：`getAwsCredentialsProvider()`（`:383-416` 含 javadoc）、
          `staticAwsCredentialsProvider(...)`（`:418-429`）、`s3AwsCredentialsProvider(...)`（`:431-458`）
        - `AwsCredentialsProviderFactory`：`createV2(mode,boolean)`（`:46-68`）、
          `createDefaultV2(boolean)`（`:80-99`）、**单参** `getV2ClassName(mode)`（`:136-162`）
      - **import 精确修剪**（⚠️ 三份候选设计里有两份这里是错的）：
        - `StorageAdapter` 删 8 个：`AnonymousCredentialsProvider` `AwsBasicCredentials`
          `AwsCredentialsProvider` `AwsSessionCredentials`（`:38-41`）、`StaticCredentialsProvider`（`:43`）、
          `Region`（`:44`）、`StsClient`（`:45`）、`StsAssumeRoleCredentialsProvider`（`:46`）
        - `AwsCredentialsProviderFactory` 删 **2 个**：`:26` `AwsCredentialsProvider` **和**
          `:27` `AwsCredentialsProviderChain`
          （⚠️ 某份设计写的 "StaticCredentialsProvider 族" **在本文件里不存在**，它在 `StorageAdapter.java:43`）
      - **✋ 必须保留**：`StorageAdapter.getAwsCredentialsProviderMode()`（`:379-381`）+ `s3CredentialsMode` 字段
        （被 `AzureGuessRoutingParityTest` 钉住，且喂 `:614` 这个**活的** BE 值）；
        `StorageAdapter` 的 `InstanceProfileCredentialsProvider` import（`:42`，`:731` 在用）；
        `AwsCredentialsProviderFactory.getV2ClassName(mode, boolean)`（`:101-134`）+ 两个 env 探针（`:70-78`）
      - **验收**：
        ```bash
        R=/mnt/disk1/yy/git/wt-catalog-spi
        grep -rIn 'getAwsCredentialsProvider()\|createV2\|createDefaultV2' $R/fe $R/regression-test --exclude-dir=target
        rm -rf $R/fe/fe-core/target/classes $R/fe/fe-core/target/test-classes
        mvn -f $R/fe/pom.xml -T 1C clean test-compile -Dcheckstyle.skip=true
        mvn -f $R/fe/pom.xml -pl fe-core -am test -Dcheckstyle.skip=true -DfailIfNoTests=false \
            -Dtest='AzureGuessRoutingParityTest,S3ThriftAdapterParityTest,CloudObjectStoreAdapterParityTest,LocationPathTest,DefaultConnectorContextBackendStoragePropsTest,DefaultConnectorContextNormalizeUriTest'
        mvn -f $R/fe/pom.xml -pl fe-core checkstyle:check   # 阻塞项：证明 import 修剪精确
        ```
      - **实测校正**（未照抄清单，逐符号验证过孤儿）：`StorageAdapter` 确为 8 个孤儿 import，
        `InstanceProfileCredentialsProvider` 有 1 处正文使用故保留、`Config` 尚有 12 处使用；
        `AwsCredentialsProviderFactory` 确为 2 个孤儿（`AwsCredentialsProvider` + `AwsCredentialsProviderChain`）。
        另清掉 3 处点名已删方法的连接器注释（含 `AwsCredentialsProviderModesTest` 的类 javadoc）。

---

## 阶段 3 — 主删除（**依赖 FPC-01 拍板**）

- [x] **FPC-03** ✅ 退役整个 `metastore/` 集群 + 孤儿 `ConnectionProperties`
      （**删 5 文件 473 行 + 从 `CatalogProperty` 挖掉 ~45 行；零 pom / 零连接器 / 零 fe-filesystem 改动**）
      - **`CatalogProperty.java` 改动**：
        1. `resolveDerivedStorageDefaults()`（`:264-272`）→ 只走 supplier；
           null 分支按 **FPC-01 的拍板结果**写（`throw` 或 `return Collections.emptyMap()`）
        2. 更新其 javadoc（`:257-263` 里 `{@link}` 了 `MetastoreProperties`）和 `:274-278` 的 javadoc
        3. 删字段 `metastoreProperties`（`:94`）+ `resetAllCaches()` 里的 `this.metastoreProperties = null;`（`:189`）
        4. 删 `checkMetaStoreAndStorageProperties(Class)`（`:307-321`）和 `getMetastoreProperties()`（`:323-345`）
        5. **import 精确修剪**：`:20` `UserException`（只被 `:312`/`:336` 用）、`:21` `MetastoreProperties`、
           **`:25` `Preconditions`（只被 `:316-317` 用 —— ⚠️ 两份设计漏了这个）**、
           `:29` `ExceptionUtils`（只被 `:314`/`:339` 用）、`:30-31` `LogManager`/`Logger`，
           以及 **`:48` 的 `LOG` 字段**（唯一使用点是 `:337`）。
           **✋ 保留** `MapUtils`（`:250` 在用）和 `Collections`。
      - **`git rm`**：
        - `property/metastore/MetastoreProperties.java`
        - `property/metastore/MetastorePropertiesFactory.java`
        - `property/metastore/AbstractMetastorePropertiesFactory.java`
        - `property/metastore/TrinoConnectorPropertiesFactory.java`
        - `property/ConnectionProperties.java`（删 metastore 后成孤儿，`design.md` §2.5）
      - **注释清理**（无编译影响，但守 Rule 12「不留悬空引用」）：
        - `StorageAdapter.java:856` —— 那是 **javadoc 散文**，改写掉即可，**不是**编译依赖
        - `fe-connector-paimon/.../TcclPinningConnectorContext.java:49`
        - `fe-core/src/test/.../CatalogPropertyPluginStorageDerivationTest.java:33-34,:53-54`
      - **⚠️ `CatalogPropertyPluginStorageDerivationTest` 不许删也不许合并**：三个用例都装了 supplier
        （`:55`/`:74`/`:83`），删除后**照常通过**；但它注释里写的变异（「把
        `resolveDerivedStorageDefaults` 改回走 `getMetastoreProperties()`」）**变得无法表达** ⇒
        按 Rule 9 把变异描述**改钉到一个仍然存在的变异上**。它是插件派生路径的唯一守卫。
      - **验收**：
        ```bash
        R=/mnt/disk1/yy/git/wt-catalog-spi
        grep -rIn 'MetastoreProperties\|MetastorePropertiesFactory\|AbstractMetastorePropertiesFactory\|TrinoConnectorPropertiesFactory\|ConnectionProperties\|checkMetaStoreAndStorageProperties\|getMetastoreProperties' \
             $R/fe $R/regression-test $R/tools $R/gensrc --exclude-dir=target
        rm -rf $R/fe/fe-core/target/classes $R/fe/fe-core/target/test-classes
        mvn -f $R/fe/pom.xml -T 1C clean test-compile -Dcheckstyle.skip=true
        mvn -f $R/fe/pom.xml -pl fe-core -am test -Dcheckstyle.skip=true -DfailIfNoTests=false \
            -Dtest='CatalogPropertyPluginStorageDerivationTest,CatalogPropertyEffectiveRawStoragePropsTest,HmsGsonCompatReplayTest,IcebergGsonCompatReplayTest,PaimonGsonCompatReplayTest,PluginDrivenExternalCatalog*Test'
        # 🔴 录制基线必须显式跑（全反应堆 test-compile 不跑 surefire —— 本分支已知盲区）
        mvn -f $R/fe/pom.xml -pl fe-connector/fe-connector-api -am test -Dcheckstyle.skip=true -DfailIfNoTests=false
        mvn -f $R/fe/pom.xml -pl fe-core checkstyle:check
        ```
        **预期不需要刷 `connector-metadata-methods.txt`**（fe-connector-api 不依赖 fe-core，
        72 行基线只用 `connector.api.*`/`java.*` 类型）—— **一旦红了就停手，别顺手刷基线。**

---

## 阶段 4 — 清扫已死的 fe-core storage 门

- [x] **FPC-04** ✅ **已落地**（**纯删除 135 行，零新增**）
      > 📌 **落地前重新 grep 的结果修正了本文档两处**（详见 `progress.md` 2026-07-28（五））：
      > ① 原文写「✋ 不要碰 `ExternalCatalog.buildHadoopConfiguration(Map)` —— 其调用者未曾枚举」。
      >    **枚举了：它也是死的。** 全仓 16 处 `buildHadoopConfiguration` 命中**全是连接器侧同名但不同类**的
      >    `IcebergCatalogFactory.` / `PaimonCatalogFactory.` 方法，`ExternalCatalog` 上那个零调用。
      > ② 原文**漏了** `ExternalCatalog.ifNotSetFallbackToSimpleAuth()`（`public`，全仓仅 2 处使用
      >    且都在将死方法内 ⇒ 连带死亡），以及 `cachedConf`/`confLock` 在**方法外**还有两处使用。

      - **`ExternalCatalog.java`（−70 行）**
        - 删方法：`getHadoopProperties()` · `getConfiguration()`（`@Deprecated` 原注释就写着
          "will be removed when connector SPI extraction is complete"）· `buildConf()` ·
          `buildHadoopConfiguration(Map)` · `ifNotSetFallbackToSimpleAuth()`
        - 删字段：`cachedConf` · `confLock`
        - **两处方法外使用一并清掉**（易漏）：`resetToUninitialized()` 里的
          `synchronized (this.confLock) { this.cachedConf = null; }` 块；反序列化后处理里的
          `this.confLock = new byte[0];`
        - 删孤儿 import：`Configuration` · `HdfsConfiguration`
      - **`CatalogProperty.java`（−65 行）**
        - 删方法：`getHadoopProperties()` · `getBackendStorageProperties()` · `getOrderedStorageAdapters()`
        - 删字段：`hadoopProperties` · `backendStorageProperties`
          ⇒ `resetAllCaches()` 瘦到只剩一行 `this.storageBindings = null;`
        - 删孤儿 import：`Configuration`
      - **🎯 真正的收益不在行数**：做完后 `initStorageAdapters()` 的入口只剩
        `getStorageAdaptersMap()` 与 `getEffectiveRawStorageProperties()`，且都来自
        `PluginDrivenExternalCatalog:207-208` ⇒ **「fe-core 存储只有一个入口」从「靠人工审计」
        变成「由构造保证」**，正好给 FPC-03 那个 fail-loud 兜底上双保险。
      - **验收**：
        ```bash
        R=/mnt/disk1/yy/git/wt-catalog-spi
        grep -rIn "ifNotSetFallbackToSimpleAuth\|getOrderedStorageAdapters\|\.buildHadoopConfiguration(\|catalogProperty\.getHadoopProperties\|catalogProperty\.getBackendStorageProperties" \
             $R/fe $R/regression-test --include=*.java --include=*.groovy --exclude-dir=target \
             | grep -v "IcebergCatalogFactory\.\|PaimonCatalogFactory\."      # → 空
        rm -rf $R/fe/fe-core/target/classes $R/fe/fe-core/target/test-classes
        mvn -f $R/fe/pom.xml -T 1C clean test-compile -Dcheckstyle.skip=true
        mvn -f $R/fe/pom.xml -pl fe-core checkstyle:check
        # 🔴 动的是每个 catalog 都继承的基类 ⇒ 窄 -Dtest 列表不够，必须整套 + --fail-at-end
        mvn -f $R/fe/pom.xml -pl fe-core -am test -Dcheckstyle.skip=true -DfailIfNoTests=false --fail-at-end
        ```
      - **状态**：残留 grep = 0 ✅ · 全反应堆 `test-compile` = BUILD SUCCESS ✅ ·
        `checkstyle:check` = 0 violations ✅
      - ⚠️ **完整 fe-core 套件未跑完**：跑到 **3h29m / 1232 个测试类**时由用户指示**主动终止**
        （耗时问题另见 [`../fe-core-ut-runtime-problem.md`](../fe-core-ut-runtime-problem.md)）。
        终止时**仅 1 个测试类失败**：`http.ForwardToMasterTest.testAddBeDropBe`
        （`ClassCastException: JSONObject cannot be cast to JSONArray`）。
        **已用 stash 归因**：`git stash push -u -- fe/` 回到干净 HEAD 跑同一测试 → **一模一样地失败**
        ⇒ **既有失败，与本改动无关**。其余 1231 个测试类全绿。

---

## 📋 单列后续（**不并入本任务**，各自开 ticket）

- **SEP-1** `StorageAdapter.checkAzureOauth2OnlyForIcebergRest()`（`:822-843`）在 storage 路径读
  metastore 命名空间键（`type` / `iceberg.catalog.type`）—— 真的 ARCH-GOAL 违规，但带着上游 #66004
  刻意的大小写敏感怪癖，需单独一刀 + e2e。
- **SEP-2** architectural 视角的完整 S3–S5 轨道（把 `S3CredentialsProviderType` 上提 `fe-filesystem-api`
  → 调和 `hadoopClassName` → 删 `common/`）。**架构上是对的终局**，但需用户对
  `Config.aws_credentials_provider_version` v1 分支（`Config.java:3740`）+「发哪条 DEFAULT 链」拍板，
  且会改线上串。见 `design.md` §7-2。
- **SEP-3** `constants/BaseProperties.getCloudCredential(...)` 零调用者，唯一作用是当 `AIProperties` 的空父类。
- **SEP-4** `fe-connector-metastore-api/pom.xml:64` 注释「This module is compiled into fe-core」是
  **过时的假话**（无任何 pom 声明它；它打进各插件 zip）——顺手改掉。
