# Problem

CI 996541（`Doris_External_Regression`, commit `fa2fcf4b246`）13 个失败中，根因 D 对应 1 个用例：

- **用例**：`external_table_p0.tvf.test_catalogs_tvf`
- **现象**：`regression-test/suites/external_table_p0/tvf/test_catalogs_tvf.groovy:82-93` 的 `CREATE CATALOG catalog_tvf_test_dlf` 抛 `DdlException`，suite 直接中断。

失败链（全部按 HEAD 核实，路径已修正 issue 文档笔误）：

| 步骤 | 位置（HEAD 实测） |
|---|---|
| 用例属性 | `test_catalogs_tvf.groovy:85` `"hive.metastore.type" = "dlf"` |
| 校验入口 | `fe/fe-connector/**fe-connector-hive**/src/main/java/org/apache/doris/connector/hive/HiveConnectorProvider.java:47` `validateProperties` |
| 拒绝点 | 同文件 `:52-55` → `HmsClientConfig.removedMetastoreTypeError(properties)` 非 null → `throw new IllegalArgumentException(...)` |
| 拒绝表 | `fe-connector-hms/.../HmsClientConfig.java:51` `REMOVED_METASTORE_TYPES`，`:55-56` `glue` / `dlf` 两项；消息生成在 `:72-83` |
| 报错文案 | `hive.metastore.type = dlf is no longer supported: Alibaba Cloud DLF 1.0 as an HMS thrift metastore has been removed. Supported types: hms.` |

> ⚠️ **修正 issue 文档**：§D 写的是 `HiveConnectorProvider.validateProperties:52` 但未给模块名，实际在 **`fe-connector-hive`** 而非 `fe-connector-hms`（`fe-connector-hms` 下无此文件，已 `find` 确认）。`HmsClientConfig` 才在 `fe-connector-hms`。行号 `:52` 与 HEAD 一致。

**产品行为完全符合设计，不修产品代码。** commit `6c9b491dbcf`「删除 thrift 一代的阿里云 DLF 1.0 metastore 支持」是用户拍板「不修直接删」的结果。

# Root Cause

**陈旧用例（stale test case），不是产品 bug。**

`6c9b491dbcf` 删 DLF 1.0 时**一个 regression-test 文件都没筛**——已实测确认：

```
$ git show --stat 6c9b491dbcf -- regression-test/
（输出为空）
```

它只删了 fe-core / connector 侧的 JUnit（含新增 `HmsClientConfigRemovedTypeTest`，+82/-82），漏掉了所有以 DLF 为载具的 e2e 用例。`test_catalogs_tvf` 是本 p0 流水线里唯一被波及的一个。

**该用例真正测的是 `catalogs()` TVF**（明文属性透传 / 敏感属性掩码 `*XXX` / GRANT-REVOKE 权限过滤），DLF 只是载具。

# Design

用户签字：**方案 b —— 整段注释（保留注释，不删除）**。

## 关键核实 1：注释范围必须扩大到 :80-145，不止 CREATE + test_10~test_14

任务简报给的范围是「CREATE CATALOG 那一段 + qt_test_10~qt_test_14」。**这个范围不够，按它施工 suite 仍然挂。**

`test_15`~`test_24` 与 `:123` / `:135` 的 GRANT/REVOKE **也全部挂在 `catalog_tvf_test_dlf` 上**（HEAD 实测）：

| 行 | 内容 | 若只注释 CREATE+10~14 会怎样 |
|---|---|---|
| `:119` | `order_qt_test_15 ... CatalogName = "catalog_tvf_test_dlf"` | .out 期望**空** → catalog 不存在也返回空 → **恒真通过**（违反 Rule 9） |
| `:120` | `order_qt_test_16 ... from catalogs()` | .out 期望 `internal internal NULL NULL` → 仍通过 |
| `:123` | `GRANT SELECT_PRIV on \`catalog_tvf_test_dlf\`.\`\`.\`\`` | 见「未证实」节：Doris GRANT 按 pattern 授权、`TablePattern.analyze()`（`TablePattern.java:105-118`）与 `GrantTablePrivilegeCommand.validate()`（`:88-113`）**均无 catalog 存在性检查** ⇒ 大概率**不报错** |
| `:129-132` | `order_qt_test_17`~`test_20` | .out 期望**非空行**（`*XXX` / `123456789` / `hms`）→ 实际空 → **确定性 FAIL** |
| `:135` | `REVOKE ...` | 同 GRANT，大概率不报错 |
| `:141-144` | `order_qt_test_21`~`test_24` | .out 期望**空** → **恒真通过** |

⇒ **`test_17` 一定挂**，窄范围方案无效。这是本设计相对简报的**唯一实质性范围扩大**，是技术必需而非偏好。

## 关键核实 2：.out 删块**安全**——框架是「按 tag 前向扫描并吞掉不匹配块」

这是简报点名「最容易翻车」的地方，已读框架源码查实：

`regression-test/framework/src/main/groovy/org/apache/doris/regression/util/OutputUtils.groovy:357-369`：

```groovy
boolean hasNextTagBlock(String tag) {
    while (hasNext()) {
        if (Objects.equals(tag, cache.tag)) {
            return true
        }
        // drain out
        def it = next()
        while (it.hasNext()) { it.next() }
    }
    return false
}
```

调用方 `Suite.groovy:1764-1766`：

```groovy
if (!context.getOutputIterator().hasNextTagBlock(tag)) {
    throw new IllegalStateException("Missing output block for tag '${tag}': ${context.outputFile.getAbsolutePath()}")
}
```

语义结论（三条，决定本设计安全性）：

1. **按 tag 匹配，不按序号/位置匹配** ⇒ 删中间 5（或 15）个块**不会让后续块错位**。
2. **前向单向游标，不匹配的块被 drain 掉** ⇒ **多余的孤儿 .out 块被静默跳过，不报错**（无「未消费块」校验）。
3. **只有反向会炸**：qt 还在跑但 .out 块没了 → 扫到 EOF → `hasNextTagBlock` 返 false → `Missing output block for tag 'xxx'`。

⇒ 只要 **注释掉的 qt 与删掉的 .out 块严格一一对应**，就安全。本设计两侧同时处理 `test_10`~`test_24`，满足。

（`order_qt_*` 走同一路径：`Suite.groovy:1924-1925` `order_qt_` → `quickTest(tag, sql, true)`。）

（另注：`.out` 里 `-- !create --` 出现两次（`:2` 与 `:8`），前向扫描天然处理，本改动不触及。）

## 关键核实 3：注释语法 —— 用 `//` 逐行，**不用** `/* */`

Rule 11 conformance，实测 `external_table_p0` 既有先例：

- `external_table_p0/hive/test_external_catalog_hive.groovy:102-105`：`// TODO(kaka11chen): Need to upload table to oss, comment it temporarily.` + 逐行 `// qt_xxx """..."""`
- 同文件 `:147-150`、`external_table_p0/hive/test_hive_statistic.groovy:40-42` 同款。
- `/* */` 在 suites 里主要用于**贴 DDL/schema 说明文本**（如 `iceberg/test_iceberg_partition_evolution.groovy:95`、`iceberg_complex_type.groovy:85`），**不用于停用断言**。

⇒ **停用断言的约定是 `//` 逐行。**

技术上 `/* */` 在本段也可行（段内无 `*/`；`"""` 与 `${user}` 在块注释里被词法层吞掉，不会插值），但：
- Groovy 块注释**不嵌套**，未来往段内加带 `*/` 的内容会静默截断；
- `//` 逐行 diff 友好、恢复时 `sed 's|^    // ||'` 可机械还原。

⇒ 选 `//`。

## 关键核实 4：`type`/`hms` 等属性并非全死

只有 **`hive.metastore.type=dlf`** 与 **`dlf.proxy.mode`** 是死路由 key。`.out` 断言的 `dlf.catalog.id` / `dlf.secret_key` / `dlf.access_key` / `dlf.uid` / `type` **每一个都还活着**——掩码被刻意设计为**比 feature 活得更久**（见下节）。所以这**不是**删死代码，是**暂停覆盖**，恢复条件明确。

## 设计决策：注释边界取 `:80`~`:145`（连续一整段）

注释掉 `:80` `drop catalog if exists` ~ `:145` 闭合 `}`，即：drop / CREATE / test_10~14 / user 创建与授权 / test_15~16 / GRANT / test_17~20 / REVOKE / test_21~24。

**为什么 `:80` 的 `drop catalog if exists` 一并注释**（简报要我判断）：`IF EXISTS` 本身无害，但 CREATE 从 `6c9b491dbcf` 起**永远失败** ⇒ 该 catalog 在任何集群上都不可能存在 ⇒ 留着是纯死语句，且与恢复时的段落完整性冲突。**注释掉，与 CREATE 同进退。**

**为什么 `test_16`（`:120`）也注释**（这是唯一有争议的一条，需用户拍板 —— 见结尾）：`test_16` 断言「低权用户 `select * from catalogs()` 只看到 `internal`」。它**字面上不依赖 DLF**，但：
- 此刻 `catalog_test_hive00` / `catalog_test_es00` 已在 `:64` / `:70` 被 drop，**集群上只剩 `internal` 一个 catalog** ⇒ 「过滤掉无权 catalog」这个**意图无法被证伪**（有没有过滤逻辑，结果都是 `internal`）⇒ **恒真，违反 Rule 9**。
- 保留它就得连带保留 `:102-117` 整套 user/GRANT 脚手架，为一条已失去鉴别力的断言留一堆碎片 ⇒ 违反 Rule 2/3。

⇒ **整段注释到 `:145`**，`.out` 相应删 `test_10`~`test_24` **全部 15 个块**。

保留部分（`:19-78`）不受任何影响：`catalogs()` 的列数/内容/count/order/建删 catalog 后的可见性/TVF 参数异常，全部照跑。

# Implementation Plan

## 改动 1：`regression-test/suites/external_table_p0/tvf/test_catalogs_tvf.groovy`

**替换 `:80-145`**（`:79` 是空行、`:146-149` 是尾部空行 + `}`，均不动）。

替换**前**（HEAD `:80-145`，节选首尾以定位）：

```groovy
    sql """ drop catalog if exists catalog_tvf_test_dlf """ 

    sql """ 
        CREATE CATALOG catalog_tvf_test_dlf PROPERTIES (
        "type"="hms",
        ...
    connect(user, "${pwd}", context.config.jdbcUrl) {
        sql """ switch internal """

        order_qt_test_21 """ ... """ 
        order_qt_test_22 """ ... """ 
        order_qt_test_23 """ ... """ 
        order_qt_test_24 """ ... """ 
    }
```

替换**后**（确切全文，`:80` 起）：

```groovy
    // ---------------------------------------------------------------------------
    // DLF 1.0 coverage below is TEMPORARILY DISABLED -- DO NOT DELETE.
    //
    // Alibaba Cloud DLF 1.0 as an HMS thrift metastore ("hive.metastore.type" = "dlf")
    // was removed in commit 6c9b491dbcf, so the CREATE CATALOG below is now rejected at
    // HiveConnectorProvider.validateProperties with:
    //   hive.metastore.type = dlf is no longer supported: Alibaba Cloud DLF 1.0 as an
    //   HMS thrift metastore has been removed. Supported types: hms.
    // Every assertion from test_10 to test_24 hangs off that catalog (the GRANT/REVOKE
    // filtering cases included), so the whole segment is disabled as one block instead
    // of being partially patched. The -- !test_10 -- .. -- !test_24 -- blocks were
    // removed from the .out file together with it.
    //
    // When DLF is supported again, restore this whole segment verbatim and regenerate
    // the .out blocks. What it covers and must cover again:
    //   * plain property passthrough via catalogs(): dlf.catalog.id, dlf.uid, type
    //   * sensitive property masking to *XXX: dlf.secret_key, dlf.access_key
    //   * catalogs() row filtering by catalog-level privilege: GRANT -> REVOKE
    // ---------------------------------------------------------------------------
    logger.info("Skipping DLF 1.0 catalogs() coverage (test_10 ~ test_24): "
            + "hive.metastore.type = dlf was removed in commit 6c9b491dbcf. "
            + "DLF is planned to be supported again; restore the whole commented-out "
            + "segment below, and its .out blocks, at that point.")

    // sql """ drop catalog if exists catalog_tvf_test_dlf """
    //
    // sql """
    //     CREATE CATALOG catalog_tvf_test_dlf PROPERTIES (
    //     "type"="hms",
    //     "hive.metastore.type" = "dlf",
    //     "dlf.proxy.mode" = "DLF_ONLY",
    //     "dlf.endpoint" = "dlf-vpc.cn-beijing.aliyuncs.com",
    //     "dlf.region" = "cn-beijing",
    //     "dlf.uid" = "123456789",
    //     "dlf.catalog.id" = "987654321",
    //     "dlf.access_key" = "AAAAAAAAAAAAAAAAAAAAAA",
    //     "dlf.secret_key" = "BBBBBBBBBBBBBBBBBBBBBB"
    //     );"""
    //
    // order_qt_test_10 """ select  CatalogName,CatalogType,Property,Value from catalogs() where CatalogName = "catalog_tvf_test_dlf" and   Property= "dlf.catalog.id" """
    // order_qt_test_11 """ select  CatalogName,CatalogType,Property,Value from catalogs() where CatalogName = "catalog_tvf_test_dlf" and   Property= "dlf.secret_key" """
    // order_qt_test_12 """ select  CatalogName,CatalogType,Property,Value from catalogs() where CatalogName = "catalog_tvf_test_dlf" and   Property= "dlf.access_key" """
    // order_qt_test_13 """ select  CatalogName,CatalogType,Property,Value from catalogs() where CatalogName = "catalog_tvf_test_dlf" and   Property= "dlf.uid" """
    // order_qt_test_14 """ select  CatalogName,CatalogType,Property,Value from catalogs() where CatalogName = "catalog_tvf_test_dlf" and   Property= "type" """
    //
    //
    // def user = 'catalog_user_test'
    // def pwd = 'C123_567p'
    // try_sql("DROP USER ${user}")
    //
    // sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    // sql """GRANT SELECT_PRIV on `internal`.``.`` to '${user}'"""
    // //cloud-mode
    // if (isCloudMode()) {
    //     def clusters = sql " SHOW CLUSTERS; "
    //     assertTrue(!clusters.isEmpty())
    //     def validCluster = clusters[0][0]
    //     sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user}""";
    // }
    //
    //
    // connect(user, "${pwd}", context.config.jdbcUrl) {
    //     sql """ switch internal """
    //     order_qt_test_15 """ select  CatalogName,CatalogType,Property,Value from catalogs() where CatalogName = "catalog_tvf_test_dlf" and   Property= "type" """
    //     order_qt_test_16 """ select  CatalogName,CatalogType,Property,Value from catalogs()  """
    // }
    //
    // sql """GRANT SELECT_PRIV on `catalog_tvf_test_dlf`.``.`` to '${user}'"""
    //
    //
    // connect(user, "${pwd}", context.config.jdbcUrl) {
    //     sql """ switch internal """
    //
    //     order_qt_test_17 """ select  CatalogName,CatalogType,Property,Value from catalogs() where CatalogName = "catalog_tvf_test_dlf" and   Property= "dlf.secret_key" """
    //     order_qt_test_18 """ select  CatalogName,CatalogType,Property,Value from catalogs() where CatalogName = "catalog_tvf_test_dlf" and   Property= "dlf.access_key" """
    //     order_qt_test_19 """ select  CatalogName,CatalogType,Property,Value from catalogs() where CatalogName = "catalog_tvf_test_dlf" and   Property= "dlf.uid" """
    //     order_qt_test_20 """ select  CatalogName,CatalogType,Property,Value from catalogs() where CatalogName = "catalog_tvf_test_dlf" and   Property= "type" """
    // }
    //
    // sql """REVOKE SELECT_PRIV on `catalog_tvf_test_dlf`.``.`` FROM '${user}'"""
    //
    //
    // connect(user, "${pwd}", context.config.jdbcUrl) {
    //     sql """ switch internal """
    //
    //     order_qt_test_21 """ select  CatalogName,CatalogType,Property,Value from catalogs() where CatalogName = "catalog_tvf_test_dlf" and   Property= "dlf.secret_key" """
    //     order_qt_test_22 """ select  CatalogName,CatalogType,Property,Value from catalogs() where CatalogName = "catalog_tvf_test_dlf" and   Property= "dlf.access_key" """
    //     order_qt_test_23 """ select  CatalogName,CatalogType,Property,Value from catalogs() where CatalogName = "catalog_tvf_test_dlf" and   Property= "dlf.uid" """
    //     order_qt_test_24 """ select  CatalogName,CatalogType,Property,Value from catalogs() where CatalogName = "catalog_tvf_test_dlf" and   Property= "type" """
    // }
```

施工要点：
- 注释体**逐行照抄原文**，仅去掉行尾多余空格并加 `    // ` 前缀 —— 恢复时逐行去前缀即得原文。
- `:102` 的 `def user` 被注释后，`user` / `pwd` 在 suite 里**无其他引用**（已 grep 全文确认：`user` 仅在 `:102-144` 段内出现）⇒ 无编译/运行期未定义符号。
- 段内原有的 `//cloud-mode` 行注释被二次注释成 `// //cloud-mode` —— 有意为之，保证机械恢复。
- Groovy 里 `//` 后的 `"""` 与 `${user}` 不参与词法/插值，**无坑**。

## 改动 2：`regression-test/data/external_table_p0/tvf/test_catalogs_tvf.out`

**删除 `:11-50`**（`-- !test_10 --` 块起，到文件末尾 `-- !test_24 --` 块止，共 15 个块）。

删除后文件**确切全文**（10 行，末尾为 `es\n\n`，与 `OutputBlocksWriter` 的「块尾留一空行」格式一致，已用 `od -c` 核对原文件同款）：

```
-- This file is automatically generated. You should know what you did if you want to edit this
-- !create --
catalog_test_es00	es	type	es
catalog_test_hive00	hms	type	hms

-- !delete --

-- !create --
catalog_test_es00	es	type	es

```

（列分隔符为 **TAB**，不是空格。）

## 不改动

- **零产品代码改动。** 不碰 `HmsClientConfig` / `HiveConnectorProvider` / `DatasourcePrintableMap` / `CatalogMgr`。
- 不碰 `:19-78`（`catalogs()` 核心覆盖）与 `:146-149`。
- 不碰任何 pom / 框架代码。

# Risk Analysis

## 会不会打挂现有单测？

**不会。改动 0 行 Java。** 已核实与本主题相邻的两个单测类，均**不读 regression-test 文件**：

| 单测类 | 方法 | 与本改动关系 |
|---|---|---|
| `fe/fe-connector/fe-connector-hms/src/test/java/org/apache/doris/connector/hms/HmsClientConfigRemovedTypeTest.java` | `removedTypesAreRejectedAndNameWhatWasRemoved`（`:65`）、`rejectionIsCaseInsensitive`（`:74`）、`survivingTypeIsNotRejected`（`:82`）、`messageAdvertisesOnlyTheSurvivingType`（`:92`） | 只测**拒绝**逻辑，纯 `HmsClientConfig` 静态方法，不受影响 |
| `fe/fe-core/src/test/java/org/apache/doris/common/util/DatasourcePrintableMapTest.java` | `testSensitiveKeysContainAliyunDLFProperties`（`:30`）、`testConstructorWithHidePassword`（`:89`）、`testHidePasswordWithSensitiveKeys`（`:159`）、`testCaseInsensitiveSensitiveKeys`（`:184`）、`testPasswordMaskConstant`（`:271`） | 只测 `SENSITIVE_KEY` 集合与 `DatasourcePrintableMap.toString()` 渲染，不受影响 |

## 会不会影响那 550 个通过的用例？

**不会。破坏面 = 2 个文件，都属本 suite 私有。**

实测：`grep -rn "catalog_tvf_test_dlf" regression-test/` 命中 **27 处，全部集中在 2 个文件**：
- `regression-test/suites/external_table_p0/tvf/test_catalogs_tvf.groovy`
- `regression-test/data/external_table_p0/tvf/test_catalogs_tvf.out`

无其他 suite 引用该 catalog、该 user（`catalog_user_test`）或该 .out。

## 删 .out 块会不会让后续 qt 错位/全挂？

**不会** —— 见 Design「关键核实 2」：`OutputUtils.groovy:357-369` 是**按 tag 前向扫描**，非按序号。且本设计里 `test_10`~`test_24` 的 **qt 侧与 .out 侧同时消失**，删除后文件中**不存在任何 qt 引用不到的 tag，也不存在任何 tag 找不到块的 qt**（残余 3 个块 `create` / `delete` / `create` ↔ 残余 3 个 qt `qt_create:62` / `qt_delete:66` / `qt_create:68`，一一对应且顺序一致）。

## 残留副作用

- **`catalog_user_test` 不再被创建** ⇒ 该 suite 原本 `try_sql("DROP USER ...")` + `CREATE USER` 的用户泄漏（suite 结束从不 drop）反而消失，属正向。
- **恒真断言被移除**（`test_15` / `test_21`~`test_24` 期望空、注释后不再跑）⇒ 符合 Rule 9，不是覆盖损失（它们在 DLF 存在时才有鉴别力）。

## 🔴 覆盖缺口（用户已知情并接受，此处存档 + 补充调查结论）

整段注释**丢掉 `dlf.secret_key` / `dlf.access_key` 的 `*XXX` 打码 e2e 回归**。而打码正是 `6c9b491dbcf` 里标为「🔴 安全：脱敏不能随功能一起删」的重点保留项，被**刻意设计为比 feature 活得更久**：

`fe/fe-core/src/main/java/org/apache/doris/common/util/DatasourcePrintableMap.java:58-69`（HEAD 实测原文）：
> *"Masking must outlive the feature: a DLF catalog created before the removal still replays from the image (rejection deliberately fires at CREATE and at client creation, never during replay, so FE can still start), so it remains listable and SHOW CREATE CATALOG still prints its stored properties."*

两个 key 的打码来源已核实（**不同来源，不可互相代表**）：
- `dlf.secret_key` → `DatasourcePrintableMap.java:66` **显式**加入 `SENSITIVE_KEY`；**另经** `OSSProperties.java:62-69`（`sensitive = true`，names 含 `"dlf.secret_key"`）经 `:87` `getSensitiveKeys(OSSProperties.class)` 二次进入。
- `dlf.access_key` → **只经** `OSSProperties.java:55-60`（`sensitive = true`，names 含 `"dlf.access_key"`）；`DatasourcePrintableMap` 的显式 dlf 四件套（`:66-69`）**不含它**。

**调查结论：单测只覆盖了「拒绝」和「`SENSITIVE_KEY` 集合 + `DatasourcePrintableMap` 渲染」，没有覆盖 `catalogs()` TVF 实际吐 `*XXX` 的那条路径。** 具体：

| 层 | 是否有单测 | 证据 |
|---|---|---|
| DLF 类型被拒 | ✅ | `HmsClientConfigRemovedTypeTest`（4 个方法） |
| `SENSITIVE_KEY` 含 `dlf.secret_key` | ✅ | `DatasourcePrintableMapTest:35` |
| `SENSITIVE_KEY` 含 **`dlf.access_key`** | ❌ | grep `dlf.access_key` 在该测试类**零命中**（只有 `dlf.secret_key` / `dlf.catalog.accessKeySecret` / `dlf.session_token` / `dlf.catalog.sessionToken`） |
| `DatasourcePrintableMap` 把 `dlf.secret_key` 渲染成 `*XXX` | ✅ | `testConstructorWithHidePassword:100`、`testHidePasswordWithSensitiveKeys:174` |
| **`CatalogMgr.getCatalogPropertiesWithPrintable` 把敏感 key 换成 `*XXX`** | ❌ **零覆盖** | `grep -rn "getCatalogPropertiesWithPrintable" --include=*.java fe/ \| grep /test/` → **无命中**。该方法在 `CatalogMgr.java:491-506`，`:481` 被 `catalogs()` TVF 行构造调用 —— 正是 `test_11`/`test_12`/`test_17`/`test_18`/`test_21`/`test_22` 唯一覆盖它的地方 |

⇒ **注释掉本段后，`catalogs()` 的打码行为在整个仓库里没有任何自动化守门。** 建议按下方 Unit Tests 节补一个廉价单测把缺口补上（**本轮是否做，需用户拍板** —— 严格按「只注释」施工的话不含它）。

**恢复条件**：DLF 重新支持时，整段恢复 + 重新生成 `test_10`~`test_24` 的 .out 块。

# Test Plan

## Unit Tests

**本设计本体（改动 1 + 改动 2）不新增、不修改任何单测** —— 因为它改动 0 行产品代码，加任何单测都不会因本改动而红/绿，属于 Rule 9 意义上的无意义测试。

**但覆盖缺口的补偿性单测建议如下（需用户拍板是否纳入本轮）**：

- **类**：`fe/fe-core/src/test/java/org/apache/doris/datasource/CatalogMgrPrintablePropertiesTest.java`（新建；`CatalogMgr` 现有测试无此文件）
- **方法 1**：`sensitiveCatalogPropertiesAreMaskedInPrintableView()`
  - **断言意图**：构造一个 `CatalogIf` 桩，其 `getProperties()` 返回 `{"type":"hms", "dlf.uid":"123456789", "dlf.catalog.id":"987654321", "dlf.access_key":"AAAA...", "dlf.secret_key":"BBBB..."}`，断言 `CatalogMgr.getCatalogPropertiesWithPrintable(catalog)` 里 `dlf.secret_key` **和** `dlf.access_key` 均 == `DatasourcePrintableMap.PASSWORD_MASK`（`"*XXX"`），而 `dlf.uid` / `dlf.catalog.id` / `type` **保持明文原值**。
  - **能挂性（Rule 9）**：把 `DatasourcePrintableMap.java:66` 的 `dlf.secret_key` 删掉 → 红；把 `OSSProperties.java:57` 的 `sensitive = true` 去掉 → `dlf.access_key` 变明文 → 红；把 `CatalogMgr.java:499-503` 的 mask 分支去掉 → 红；把明文断言反过来（若有人为省事把所有 key 都 mask）→ 红。**四个方向都能杀。**
  - **为什么必要**：这是 `catalogs()` 打码的唯一守门；`DatasourcePrintableMapTest` 只测集合与 `toString`，测不到 `CatalogMgr` 这层的 `SENSITIVE_KEY.contains(key)` 分支。
- **方法 2**：`dlfAccessKeyIsSensitive()`（或直接加进 `DatasourcePrintableMapTest`，与 `:30` 的 `testSensitiveKeysContainAliyunDLFProperties` 并列）
  - **断言意图**：`Assertions.assertTrue(DatasourcePrintableMap.SENSITIVE_KEY.contains("dlf.access_key"))`，并加注释说明它**只**来自 `OSSProperties` 的 alias（非 `DatasourcePrintableMap` 显式列表），所以一旦有人清理 OSS 属性的 dlf alias 就会**静默解除打码**。
  - **能挂性**：删 `OSSProperties.java:57` 的 `sensitive = true` 或从 `names` 里去掉 `"dlf.access_key"` → 红。

> 严格按用户签字（「只注释 + 只删 .out 块」）的话，本轮**不**做上述单测，改列入「仍未证实 / 后续」。

## E2E Tests

**不新增 E2E。** 理由：

1. 本设计**没有引入任何新行为**，只是停用已失效的旧覆盖 —— 新 e2e 无对象可测。
2. 「DLF 被拒绝」这一行为**不该**加 e2e：单测层已有守门（`HmsClientConfigRemovedTypeTest` 的 4 个方法，`6c9b491dbcf` 中 +82/-82），加一条 e2e 只为断言一句报错文案，成本高、收益为零，且违反 Rule 2。
3. `catalogs()` 的其余 e2e 覆盖（`test_catalogs_tvf.groovy:19-78`）**原样保留**，不需要替代品。

**验证方式**（施工后，由跑 CI 的一方执行；本设计禁编译/禁 maven，未在本轮实测）：
- `external_table_p0.tvf.test_catalogs_tvf` 从 FAIL → PASS，且日志中出现新加的 `logger.info` 行 `Skipping DLF 1.0 catalogs() coverage (test_10 ~ test_24) ...`。
- 其余 550 个通过用例不受影响（破坏面已证为 2 个本 suite 私有文件）。

## 后续（本轮不做，issue 文档 §D 已列，已按 HEAD 核实存在）

同一漏网（`6c9b491dbcf` 零 regression-test 改动）在**其他流水线**会同样挂。本 p0 流水线不跑，故本轮不处理：

| 文件 | 行 | 类型 |
|---|---|---|
| `external_table_p2/hive/test_cloud_accessible_oss.groovy` | 65 | dlf |
| `external_table_p2/refactor_catalog_param/hive_on_hms_and_dlf.groovy` | 634 | dlf |
| `aws_iam_role_p0/test_catalog_with_role.groovy` | 86 | glue |
| `aws_iam_role_p0/test_catalog_instance_profile.groovy` | 70, 79, 89 | glue |
| `external_table_p2/iceberg/test_iceberg_dlf_catalog.groovy` | 36 | dlf |
| `external_table_p2/refactor_catalog_param/iceberg_on_hms_and_filesystem_and_dlf.groovy` | 747, 753 | dlf |
| `external_table_p2/paimon/test_paimon_dlf_catalog{,_miss_dlf_param,_new_param}.groovy` | 37/38/39 | dlf（注意 `_new_param` 名字虽新，用的仍是 **DLF 1.0** `paimon.catalog.type=dlf`，非保留的 2.0 REST 路径） |

死代码（定义未引用，不触发）：`iceberg_on_hms_and_filesystem_and_dlf.groovy:721` `hive_dlf_type_properties`；`iceberg_and_hive_on_glue.groovy:369` `hms_glue_catalog_base_properties`。
注意 `iceberg_and_hive_on_glue.groovy:367` 的 `iceberg.catalog.type='glue'` **仍受支持**（只删了 glue 的 HMS-thrift metastore），别误删。

**需 owner 拍板**：p2 的 DLF 用例是**直接删除**（DLF 就是它们的主题，符合 `6c9b491dbcf` 的策略）还是**照本设计整段注释**。issue 文档建议删除；但既然本 p0 用例已签字「注释保留、后续恢复」，两处策略不一致会埋坑 —— 建议统一。

# 仍未证实

1. **`GRANT`/`REVOKE` 作用于不存在的 catalog 是否报错** —— 未证实。已读 `TablePattern.java:105-118`（`analyze()` 只做名字规范化，无存在性检查）与 `GrantTablePrivilegeCommand.java:88-113`（`validate()` 只查 Ranger / 角色名 / 权限类型 / 授权者自身权限，无 catalog 查表），**倾向于不报错**，但**未实跑验证**（本轮禁编译/禁 maven）。
   → **不影响本设计**：`:123` / `:135` 已整段注释，此结论只用于论证「窄范围方案不可行」，而该论证的**决定性证据是 `test_17`~`test_20` 期望非空行必然 FAIL**（.out `:31-41` 实测），不依赖 GRANT 是否报错。
2. **本改动未经实跑验证** —— 设计为纯静态产出（另一 session 正在同一工作树跑 maven，禁踩踏）。「suite 转 PASS」是推理结论，非实测。
3. **`catalogs()` 打码的单测缺口是否补** —— 待用户拍板（见 Test Plan / Unit Tests）。缺口本身**已证实**：`grep -rn "getCatalogPropertiesWithPrintable" --include=*.java fe/ | grep /test/` 零命中，`grep "dlf.access_key" DatasourcePrintableMapTest.java` 零命中。
4. **注释范围扩大到 `:145` 超出用户原签字范围**（原签字为「CREATE + test_10~test_14」）—— 技术必需性**已证实**（`test_17`~`test_20` 必挂），但**范围本身需用户确认**。其中 `test_16` 是否保留是唯一有判断空间的一条（本设计判其恒真、主张注释，见 Design）。
5. **继承 issue 文档 §D 的既有未证实项**：无（§D 的 open question #1「`dlf.uid` / `dlf.catalog.id` 会不会作为未知 key 被拒」已被该文档用本次运行日志证伪 —— 抛点在 `checkProperties`，说明连接器构造 + `preCreateValidation` + `initAccessController` 已带全套 `dlf.*` 属性通过）。
6. **`6c9b491dbcf` 是否还漏了 p0 流水线里的其他 DLF/Glue 用例** —— 未穷举。本轮只按 CI 996541 的实际失败清单（13 个）核实，`test_catalogs_tvf` 是其中唯一的根因 D 用例；但「本流水线其他用例是否恰好没碰 DLF」未做全量 grep 证明。
