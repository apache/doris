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

# ③ common/ 只剩活代码 —— ⚠️ 仅当 OD-2 拍板为 A（做 FPC-02）时才是判据；
#    OD-2 现推荐 B（不做），此时本条**不适用**，`common/` 保留死构造臂是有意为之
grep -rn 'createV2\|createDefaultV2\|getAwsCredentialsProvider()' $R/fe --exclude-dir=target

# ④ 编译 + 门禁全绿（每步都要，不只最后一次）
mvn -f $R/fe/pom.xml -T 1C clean test-compile -Dcheckstyle.skip=true
mvn -f $R/fe/pom.xml -pl fe-core checkstyle:check
```

**⚠️ 三条纪律**（本仓库已知踩坑，见 `design.md` §5）：
1. 删除类改动**不能只信增量编译** → 每步先 `rm -rf fe-core/target/{classes,test-classes}`。
2. 全反应堆**必须含测试源**，**禁 `-Dmaven.test.skip=true`**；且必须 `-Dcheckstyle.skip=true`
   （否则 checkstyle 扫 generated-sources 退化成平方级，构建卡死）。
3. checkstyle 的 `UnusedImports` 是**阻塞门禁**，改为**只对改动模块**单独跑 `checkstyle:check`。
4. 🆕 **`-pl` 必须配 `-am`**（否则兄弟模块的 `${revision}` 解析不了 → 假错），且 surefire 2.22.2
   认的是 **`-DfailIfNoTests=false`**（不是 `-DfailIfNoSpecifiedTests`）。2026-07-28 两条都实测踩过。

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

- [x] **FPC-01** ⏳ **OD-1：按推荐值 A 执行，待用户追认**
      （问题：删掉 metastore 后，`resolveDerivedStorageDefaults()` 的 null-supplier 分支要
      **fail-loud（`throw`）** 还是 **fail-silent（`return emptyMap()`）**？详见
      [`open-decisions.md`](./open-decisions.md) **OD-1**）
      - **落地为 A**：`throw new IllegalStateException(...)`，随 FPC-03 一起提交。
      - 配守卫测试 `CatalogPropertyPluginStorageDerivationTest.unwiredSupplierFailsLoudInsteadOfDerivingNothing`，
        **已做变异验证**：把 `throw` 改成 `return emptyMap()` → 该用例变红（其余三例不受影响），改回 → 复绿。
      - **要翻成 B 只需改一行 + 删该用例**（OD-1 里写了具体位置）。
      - ⚠️ 调研报告原提的缓解方案「把 supplier 安装语句提前」**经复核修不干净**
        （lambda 读到的 `connector` 字段仍是旧值/null），已在 OD-1 中列为**不推荐**，未采纳。

---

## 阶段 2 — 删死代码（独立，可先做，也可整项丢弃）

- [ ] **FPC-02** ⛔ **BLOCKED on OD-2 —— 现推荐「不做」** 删 AWS provider 的死构造臂（**~146 行，零行为变更**）
      > 🔴 **2026-07-28 查上游后推荐值翻转**：`upstream-apache/master` @ `2faf819fa89` **有两个活调用者**
      > （`connectivity/AbstractS3CompatibleConnectivityTester.java:71`、
      > `property/common/IcebergAwsClientCredentialsProperties.java:84`），只是本分支已把这两个消费者
      > 连同整个 `datasource/connectivity/` 包删光了 ⇒ **上游活、本分支死**。
      > 而 `StorageAdapter.java` **两边都在**、会走 rebase 三方合并：删掉方法等于把上游对该区域的
      > 每次改动都变成人工冲突，换来的只是 146 行本就不执行的代码。**详见 [`open-decisions.md`](./open-decisions.md) OD-2。**
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
      - 🟢 **可整项丢弃**：不影响 FPC-03。~~落地前 grep 一次上游 master~~ **已 grep，见上方红框**。

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

## 阶段 4 — 可选清扫（**另起提交，落地前重新 grep**）

- [ ] **FPC-04** ⬜ 清扫 fe-core 已死的 storage 门
      - **仅当执行时重新 grep 确认零调用者**才做：
        `ExternalCatalog.getHadoopProperties()`、`ExternalCatalog.getConfiguration()`（已标 `@Deprecated`）
        + `buildConf()` 及其缓存字段、`CatalogProperty.getBackendStorageProperties()`、
        `CatalogProperty.getOrderedStorageAdapters()`
      - **✋ 不要碰** `ExternalCatalog.buildHadoopConfiguration(Map)` —— 它的调用者**没有枚举过**
      - **收益**：做完后 `PluginDrivenExternalCatalog.java:207-208` 成为 `initStorageAdapters()` 的
        **唯一入口（由构造保证，而非靠人工审计）**
      - **验收**：逐符号零调用者 grep → **完整** `mvn -pl fe-core test -Dcheckstyle.skip=true --fail-at-end`
        （⚠️ 它动的是每个 catalog 都继承的基类，**窄 `-Dtest` 列表不够**）→ `checkstyle:check`
      - 🟢 刻意与 FPC-03 分开，好让 FPC-03 保持**可单独回滚**

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
