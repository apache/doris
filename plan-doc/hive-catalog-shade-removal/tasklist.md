# ✅ Task List — 剔除 `hive-catalog-shade` 冗余依赖

> **本任务的唯一进度清单**。完成一项即把 `[ ]` 勾成 `[x]`（随 commit 更新）。
> **「怎么做」看 [`design.md`](./design.md)，别在这里展开。「下一步做什么」看 [`HANDOFF.md`](./HANDOFF.md)。**
> **Task ID 永不复用**；删除的 task 标 `[deleted YYYY-MM-DD]` 保留占位行。
> **⚠️ 行号信 HEAD 不信文档**（基线 = 2026-07-14 / `669602f079d`）。

**总判据（唯一的「做完了没」标准）**：
```bash
grep -rlE '^import (org\.apache\.hadoop\.hive\.|shade\.doris\.hive\.|com\.aliyun\.datalake\.)' \
     fe/fe-core/src/main/java | wc -l     # 基线 26  →  目标 0
grep -rn 'hadoop.hive' fe/fe-common/src/main/java   # 基线 1 处 → 目标 空
```

---

## 阶段 0 — 离线代偿验证 + 决策拍板（⛔ 阻塞阶段 2–6）

> **🚩 用户定调（2026-07-14）：Glue-on-HMS 与 paimon-on-DLF 无法 e2e 验证 → 一律按「必须保留、必须继续能用」设计。**
> ⇒ **禁止**「反正可能坏了就删功能」；**禁止**无论证的静默换实现；**e2e 不可得 ⇒ 用 HCS-01 离线代偿**。
> ⇒ **阶段 6 是单向门**：任一客户端搬不动就停在降级档，**不许为了删而删**（`design.md §4.1`）。

- [x] **HCS-00** 事实基线（10-agent 侦察 + 3-agent 对抗验证 + 主 session 亲验）→ `design.md`
- [ ] **HCS-01** ⭐ **离线 classloader 复现测试（不需要真集群，e2e 的代偿；本任务最高优先级）**
      写一个单测：用**真实 plugin-zip 的 `lib/*.jar`** 组一个 `ChildFirstClassLoader`（parent = app loader，
      parent-first 前缀照抄 `ConnectorPluginManager:64`），然后断言：
      ```java
      Class<?> childIMSC = Class.forName("org.apache.hadoop.hive.metastore.IMetaStoreClient", false, child);
      Class<?> glue      = Class.forName("com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient", false, child);
      childIMSC.isAssignableFrom(glue);   // ← 这就是 RetryingMetaStoreClient.getProxy 内部 asSubclass 的判据
      ```
      对 **Glue-on-HMS**（hms/hive/iceberg/hudi 插件）与 **paimon-on-DLF**（`ProxyMetaStoreClient`）各做一遍。
      - **机制推演预测它今天是 false（= 已经坏了）**，见 `design.md §3.2b`。**先跑，用事实说话。**
      - ⚠️ 无论结果如何，**这个测试本身就是交付物**（降级档 1）：它把一个「谁也说不清」的问题变成 CI 红绿灯，
        并成为阶段 2/3 搬迁后的**验收门禁**。
      - 参考事实：hudi/iceberg 的 zip 里**实查到** `lib/hive-catalog-shade-3.1.1.jar`（127 MB）；
        `ThriftHmsClient.java:644` 把 TCCL 钉到插件 loader，`:910` 才调 `getProxy`。
- [ ] **HCS-02** 决策 **D-3** 小调研：`RangerHiveAuditHandler` 用了 hive-exec `HiveOperationType` 的**什么**？
      出 1 页方案（自建枚举 / 下沉插件 / 保留）。**1 个 subagent 够**。
- [ ] **HCS-03** 用户签字 **D-1**（Glue 归属，推荐 A=搬进 `fe-connector-hms`）+ D-3 结论。
      （**D-2 不在这里签** —— 它有真实技术未知数，先做 HCS-30a spike。）
      签完写进 `../decisions-log.md`（新 D-NNN）+ `progress.md`。

---

## 阶段 1 — 独立冗余清理（🟢 不依赖任何决策，**可立即开工**）

> 这些与 shade **正交**，但 HCS-11 是「`fe/lib` 真正 hive-free」的前提（风险 R-4）。
> **每一条都要先验反射用途**（`grep` 类名字符串 + `Class.forName`），别只看 import。

- [ ] **HCS-10** 逐条验证 fe-core 源码**零引用**的依赖（agent 报告称零引用，**须自验**）：
      `hudi-hadoop-mr` · `iceberg-aws` · `s3-tables-catalog-for-iceberg` · `avro` · `kryo-shaded`。
      产出：一张「可删 / 须留 / 需单独分析」表 → `progress.md`。
- [ ] **HCS-11** 删 `hudi-hadoop-mr`（`fe/fe-core/pom.xml:600-604`）。**它是 `hive-storage-api:2.8.1` 进 `fe/lib`
      的唯一来源**（风险 R-4）。⚠️ `hudi-common` **要留**（`HttpProperties` / `StatisticsCache` 各一处 import）。
- [ ] **HCS-12** 删 HCS-10 判定为「可删」的其余项（与主线 HANDOFF「单列后续 ②：iceberg AWS 属性簇 + maven 依赖
      pom 裁剪」合流；**注意别和主线 session 抢同一批文件**，memory `concurrent-sessions-shared-worktree-hazard`）。
- [ ] **HCS-13** 守门：fe-core `test-compile` 绿 + checkstyle 0 + `unzip -l fe/fe-core/target/doris-fe-lib.zip | grep hive`
      记录删前/删后差异。**独立 commit**。

---

## 阶段 2 — Glue 客户端归位（依赖 D-1）

- [ ] **HCS-20** 把 `fe/fe-core/src/main/java/com/amazonaws/glue/**`（**38** 个 `.java`）搬进 `fe-connector-hms`。
      搬完它随 hive/iceberg/hudi 的 plugin-zip 走 **child-first**，`ThriftHmsClient.java:920-922` 的按名加载在插件内自满足。
      - ⚠️ **shade jar 里 `AWSCatalogMetastoreClient` 有 0 个条目** —— 唯一定义就是这 38 个文件，删了没人兜底。
      - ⚠️ 搬迁后 checkstyle 会扫它们（连接器模块规则可能与 fe-core 不同）。
- [ ] **HCS-21** fe-core 删除 glue 树；确认 `doris-fe.jar` 里不再有 `com/amazonaws/glue/**`。
- [ ] **HCS-22** 验证：hms/iceberg/hudi 三个 plugin-zip 里都能解析到 `AWSCatalogMetastoreClient`（`unzip -l` 实查），
      且 **HCS-01 的离线 classloader 测试转绿**（child 的 `IMetaStoreClient.isAssignableFrom(Glue客户端)` = true）。
      —— **e2e 不可得，这就是唯一的守门。** **独立 commit**。

---

## 阶段 3 — DLF 客户端归位（⚠️ **最高风险 / 真实技术未知数 → 先 spike**）

- [ ] **HCS-30a** ⚠️ **SPIKE（先做，结论决定 D-2）**：paimon 到底能不能安全自带 DLF 客户端？三个待答问题：
      1. fe-core vendored 的 `ProxyMetaStoreClient`（**2193 行**）还 import 了
         `com.aliyun.datalake.metastore.common.*` / `.hive.common.utils.*` —— 这 **1963 个 `com/aliyun` 类
         只在 shade jar 里**。搬客户端 = 必须同时搬 **SDK 闭包**。这闭包从哪来？
         （`com.aliyun.datalake:metastore-client-hive3:0.2.14` 独立可加吗？`~/.m2/.../com/aliyun/datalake` 已存在，先查）
      2. **版本口径**：阿里云 artifact 是 `metastore-client-hive**3**`（hive 3 API），paimon 私有 shade 是
         hive **2.3.7**（jar 内 `IMetaStoreClient.class` 日期 2020-04-07）。
         📎 线索：类的包名却叫 `...metastore.**hive2**.ProxyMetaStoreClient` → **确认它实现的是哪个版本的
         `IMetaStoreClient` 接口**（`javap` 看 supertype）。
      3. fe-core vendored 那份 与 shade 里那份，**差异是什么**？（R-2：今天生效的是 fe-core 那份）
      **产出**：A/B/C 三条路线的可行性判定 → `progress.md` + 交用户签 D-2。
      - **A** `metastore-client-hive3` 加进 `fe-connector-paimon-hive-shade` artifactSet，随 paimon 前缀 relocate（最干净，**前提是 hive API 对得上**）
      - **B** 把 fe-core 那 2193 行搬进 paimon 插件并改写 import 前缀（工作量大）
      - **C** paimon 也依赖 `hive-catalog-shade`（P5 明确拒绝过；且会与 paimon 私有 shade 的 `IMetaStoreClient` 撞成两份）**← 不推荐**
      - **⛑ D（降级）** spike 判定不可安全搬迁 → **停在 `design.md §4.1` 的档 2**，`ProxyMetaStoreClient` + shade 依赖**保留**，本任务收在「fe-core 大幅瘦身但 shade 不删」。**这是允许的结局，不许硬上。**
- [ ] **HCS-30b** 按 spike 结论实施（= P5-B7 自标 blocker；`PaimonConnector.java:398` 注释
      *"host-provided via hive-catalog-shade at cutover, not bundled"*）。
- [ ] **HCS-31** fe-core 删 `com/aliyun/datalake/metastore/hive2/ProxyMetaStoreClient.java`。
      ⚠️ **风险 R-2**：`start_fe.sh:355` 钉 `doris-fe.jar` 最前 → **今天生效的是 fe-core 这份**，不是 shade 里那份。
      **用户无法 e2e ⇒ 禁止无论证的静默换实现**：删前必须 diff 两份、把差异写进 `progress.md`；
      **等价性论证不出来就搬源码（保语义），不要换 jar。**
- [ ] **HCS-32** 确认 **iceberg 不用动**（`DLFClientPool.java:20` 直接 import，iceberg plugin-zip 自带 shade → child-first 自满足）。
- [ ] **HCS-33** **HCS-01 的测试转绿**：paimon 插件 loader 下 `ProxyMetaStoreClient` 能被解析为 child 的
      `IMetaStoreClient`。**独立 commit**。

---

## 阶段 4 — HiveConf 属性解析下沉（fe-core + fe-common 去 hive 化）

> 这正是 memory `catalog-spi-no-property-parsing-in-fecore`（**fe-core 不持有任何属性解析**）的未完成尾巴。
> **⚠️ 受【铁律 B】约束**：不得为了「删得动」把 HiveConf 逻辑就近挪进 fe-core util —— 遇到就**停手**交 review。

- [ ] **HCS-40** 下沉 5 个属性类（`fe/fe-core/.../datasource/property/metastore/`）：
      `HMSBaseProperties` · `AbstractHiveProperties` · `AliyunDLFBaseProperties` ·
      `HiveAliyunDLFMetaStoreProperties` · `HiveGlueMetaStoreProperties`。
      ⚠️ R-6：`AliyunDLFBaseProperties.java:71` 用 `DataLakeConfig.CATALOG_PROXY_MODE` 当**注解常量**（javac 内联）
      → 纯字节码扫描会误报「未使用」，**一律以 `grep import` + 真编译为准**。
- [ ] **HCS-41** `DefaultConnectorContext.java:208` 的 `loadHiveConfResources` 去 HiveConf 化。
- [ ] **HCS-42** **fe-common 去 hive 化**（⚠️ **必须与 fe-core 同批或更早**，否则「编译绿、运行炸」）：
      删 `CatalogConfigFileUtils.java:23,95` 的 `HiveConf` import + `loadHiveConfFromHiveConfDir` 方法。
      判据：`grep -rn 'hadoop.hive' fe/fe-common/src` → **空**。
- [ ] **HCS-43** 4 个测试删/改：`HMSIntegrationTest` · `HMSPropertiesTest` · `HMSGlueMetaStorePropertiesTest` ·
      `HMSAliyunDLFMetaStorePropertiesTest`。**独立 commit**。

---

## 阶段 5 — Ranger hive 审计去 hive-exec（依赖 D-3）

- [ ] **HCS-50** 按 HCS-02 的结论处理 `RangerHiveAuditHandler.java:22` 的 `HiveOperationType`（hive-exec）。
      ⚠️ `RangerHiveAccessControllerFactory` 是 **service-loader 注册的活类**
      （`fe-core/src/main/resources/META-INF/services/...AccessControllerFactory`）→ 配了
      `access_controller.class` 的用户会在**运行时**炸，单测抓不到。**独立 commit**。

---

## 阶段 6 — pom 终局清理（⛔ **单向门**：只有阶段 2–5 全绿 + 总判据 = 0 才能动）

> **⚠️ 用户无法 e2e ⇒ 归不了 0 就停在 `design.md §4.1` 的降级档，不许为了删而删。**

- [ ] **HCS-60** 删 `fe/fe-core/pom.xml:437-440`（`hive-catalog-shade` 本体）
- [ ] **HCS-61** 删 `fe/fe-core/pom.xml:217-223`（`commons-lang` 2.6 runtime）
      —— 已验证 fe-core 源码 0 处用 lang2、Ranger 2.8.0 也 0 处（⚠️ 风险 R-7：Ranger 降版会复活它）
- [ ] **HCS-62** 删 `fe/fe-core/pom.xml:926-967`（`bundle-fastutil-into-doris-fe` shade execution）
      —— ⚠️ **`fastutil-core` 依赖本身要留**（`:766-774`，`TabletInvertedIndex` 等 3 文件在用）；
      按风险 **R-5** 在该依赖上**留一条注释**记录「为什么这里曾经需要 shade 覆盖」，否则将来 bump 会复现原 bug
- [ ] **HCS-63** 删 `fe/fe-core/pom.xml:776-782`（`<repositories>` 的 `central`，注释写着 `for hive-catalog-shade`）
- [ ] **HCS-64** 删 `fe/fe-common/pom.xml:87-91`（`provided` shade）
- [ ] **HCS-65** **不许动**的复核：`fe/pom.xml:801-805` 版本钉 ✋ · `bin/start_fe.sh:355` 钉序 ✋ ·
      `fastutil-core` 依赖 ✋ · BE `java-udf`/`avro-scanner` pom ✋（独立供应链，动它炸过 JNI scanner）·
      `doris-shade` 仓库 ✋。**独立 commit**。

---

## 阶段 7 — 守门 + e2e

- [ ] **HCS-70** 静态守门：
      - 总判据两条 grep → 0 / 空
      - `mvn -o -f <abs>/fe/pom.xml -pl fe-core -am test-compile` BUILD SUCCESS（**漏 `-am` → 假错**）
      - fe-common + 全连接器 `test-compile` 绿 · checkstyle 0 · import-gate exit 0
      - `cd fe && mvn -o dependency:tree -Dverbose -Dincludes=org.apache.doris:hive-catalog-shade -pl fe-core -am`
        → **fe-core 段为空**
      - `unzip -l fe/fe-core/target/doris-fe-lib.zip | grep hive` → 只剩 `hive-storage-api`（若 HCS-11 已做则**全无**）
- [ ] **HCS-71** **验收（⚠️ 用户无法 e2e Glue/DLF → 分两栏）**：
      - **离线必过**（本任务的实际门禁）：
        - **HCS-01 的 classloader 测试全绿** —— Glue-on-HMS + paimon-on-DLF 客户端在各自插件 loader 下
          可解析为 child 的 `IMetaStoreClient`
        - 各 plugin-zip `unzip -l` 实查客户端类就位
      - **可跑的 e2e**：① 普通 HMS catalog 读写（回归基线）；② Ranger hive 鉴权（若 HCS-50 动了它）
      - **⛔ 无法验证、须在 PR 描述中显式声明为「未 e2e 覆盖」的**：**Glue-on-HMS** · **DLF（hive/iceberg/paimon）**
        —— 靠 HCS-01 的离线测试 + 代码审查兜底。**Fail loud：不要在 PR 里假装它们验过了。**
- [ ] **HCS-72** 收尾：`progress.md` 结项 + `../decisions-log.md` 补 D-NNN + PR（base = `branch-catalog-spi`，squash）

---

## 📌 Commit / 分支纪律（沿用主线）

- 工作分支 `catalog-spi-11-hive`（或按用户指示另开）；PR base = `branch-catalog-spi`，**squash**。
- **每个阶段 = 独立 commit**；文档（本空间 4 个文件）与 code **分开 commit**。
- **⚠️ path-whitelist `git add`，严禁 `git add -A`** —— 工作树有大量历史遗留 scratch（`.audit-scratch/` /
  `conf.cmy/` / `META-INF/` / `*.bak` / `failed-cases.out` / `.claude/` …），**非本线程产物，勿混入**。
- commit message：`[refactor|fix|doc](catalog) …` + `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`
