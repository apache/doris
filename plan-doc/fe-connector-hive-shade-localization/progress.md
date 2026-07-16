# 进度记录 — `fe/fe-connector` 剥离 `hive-catalog-shade`

> **append-only**，只追加不覆盖。每条：日期 · 谁/什么 session · 结论 · 证据(file:line/命令) · 坑。
> 状态清单看 `tasklist.md`；下一步看 `HANDOFF.md`。

---

## 2026-07-16 · 建档 session（调研，**代码零改动**）

### 起源
用户先做了一轮 `hive-catalog-shade` 调研（为什么包里有 iceberg/paimon 的 hive-metastore、fe-connector 下是否还需要、StarRocks 怎么不用 shade），据此提出**中期迁移**：把 iceberg 照 paimon 模式迁自带精简局部 shade。追问「只迁 iceberg 是不是整个 fe/fe-connector 就不依赖 hive-catalog-shade 了」→ 核实后**答案是否**，遂建本任务空间。

### 依赖图核实（本 session 实测，2026-07-16 `catalog-spi-11-hive`）
- **`fe/fe-connector/` 真实(非注释)直接消费者只有 2 个**：`fe-connector-hms/pom.xml:43`（**无 scope = compile = 传递**）、`fe-connector-iceberg/pom.xml:93`。
  - 验证法：`perl -0777 -pe 's/<!--.*?-->//gs' <pom> | grep -c '<artifactId>hive-catalog-shade</artifactId>'`（剥注释再数）。
- `fe-connector-hive` / `fe-connector-hudi` / `fe-connector-iceberg` **都 depend `fe-connector-hms`** → 经它传递拿 shade。
- **repo 全局真实消费者 = 4 + 版本钉**：`fe-connector-hms`、`fe-connector-iceberg`、`be-java-extensions/avro-scanner`、`be-java-extensions/java-udf`、`fe/pom.xml`。**与兄弟任务 `hive-catalog-shade-removal/` HANDOFF 的声明一致。** 后三者不在本任务范围。
- ⇒ **结论：承重墙是 `fe-connector-hms`。只迁 iceberg 摘不掉。**

### 关键难点（已识别，未解）
- `fe-connector-hms` 有 vendored 补丁 `src/main/java/org/apache/hadoop/hive/metastore/HiveMetaStoreClient.java`，`import org.apache.doris.datasource.hive.HiveVersionUtil`（**版本感知**），当前靠 jar 排序 overlay 掉 shade 里未打补丁的同名类。重定位后可能命名空间撕裂——**paimon 无此问题**（客户端全来自 SDK jar）。列为 **FCL-02 头号交付物 / D4**。

### shade 机制与体积（本 session 实证，供设计参考）
- 重定位：`doris-shade/hive-catalog-shade/pom.xml:627` `org.apache.thrift`→`shade.doris.hive.org.apache.thrift`。Doris 内部 thrift 0.16.0（`fe/pom.xml:295`），Hive 3.1.3 客户端桩按 ~0.9.x，二进制不兼容（`TFramedTransport` 换包 `.transport.layered`、`TBase` 契约漂移）。
- **实测部署 jar** `output/fe/plugins/connector/iceberg/lib/hive-catalog-shade-3.1.1.jar` = **122 MB 压缩 / 300 MB 解压 / 70,030 文件**。占用排行（解压）：paimon-bundle 内部再打包依赖 72.8MB(25%)、完整 hadoop 58.7MB(20%)、fastutil 6.5.x 36.8MB(13%)、多平台原生库 18.6MB(6%)、iceberg 11MB(4%)、datanucleus+derby ~17MB、DLF 7.7MB……**而真正为它存在的 `org.apache.hive` 客户端仅 4MB(1.4%)、重定位 thrift 仅 0.5MB(0.2%)**。⇒ 迁精简 shade 预估目标 **15–25 MB**。
- 对照 StarRocks：不自维护 shade，用 `io.trino.hive:hive-apache:3.1.2-22`（33MB），单一 libthrift 0.23.0；生成桩只用 thrift「生成代码契约」（跨版本二进制稳定，`readStructBegin` 等 0.14+ 上移到 `TReadProtocol/TWriteProtocol` 接口保签名——本 session 用 classload+link 实测通过）；唯一换包的 `TFramedTransport` 落在手写 transport 层、默认死代码。**「整体换 hive-apache」是可选的更激进终局，本任务不含**（只做「精简局部 shade」中期方案）。

### 建档产出
- 新任务空间 `plan-doc/fe-connector-hive-shade-localization/`：README + design（D1–D6 + 风险 R1–R6）+ tasklist（FCL-01~50，Phase 0–5）+ HANDOFF + 本文件。
- **下一步**：Phase 0 从 FCL-02 起（见 HANDOFF）。

### 坑/提醒（留给下一个 session）
- 剥 XML 注释再判依赖，否则被大量注释里的 `hive-catalog-shade` 字样骗（本 session 第一次 grep 就中招）。
- 兄弟任务 `hive-catalog-shade-removal/` 已把 shade 从 fe-core/fe-common 摘掉（阶段 1–5），**别重做**；它明确保留 hms/iceberg 两个 fe-connector 消费者——那正是本任务的对象。

---

## 2026-07-16 · Phase 0 完成（侦察+设计定案+用户签字，**代码零改动**；分支 `catalog-spi-hive-shade-12`）

### 头号未知 D4 定案（`javap` 字节码实证）
- `javap -v -p fe-connector-hms/target/classes/.../HiveMetaStoreClient.class`：对 thrift 全部 **48** 处引用（7 base + 5 protocol + 36 transport）**全部是重定位名** `shade/doris/hive/org/apache/thrift/*`，**零 raw `org.apache.thrift`、零按名反射**。⇒ 补丁客户端**留在 fe-connector-hms、不搬进 shade、不改写**，只需精简 shade 复现同名 thrift。设计原担心的"命名空间撕裂"不成立。
- 全仓库仅剩**一份** `HiveMetaStoreClient.java`（在 fe-connector-hms）；文件头"be-java-ext/fe-core 有副本"注释**已过期**（那些副本已随迁移删除）。fe-core 已零 hive-catalog-shade 依赖（兄弟任务确认）。

### 精确 bundle 清单（核查 agent + `jar tf` 交叉验证）
- **装**：`hive-standalone-metastore:3.1.3`（**非** stub `hive-metastore:3.1.3`）、`libthrift:0.9.3`+`libfb303:0.9.3`（重定位）、`hive-common:3.1.3`、`hive-storage-api:2.7.0`、`hive-serde:3.1.3`（iceberg）、`iceberg-hive-metastore:1.10.1`（iceberg）。体积 122MB→**~13-15MB**。清单入 design.md §3。
- **hive-exec 不进插件**：`hive.ql.*` 全是字符串常量（格式类名写进 HMS SD），零 import；host `hive-exec:core`(e7eae85) 是 CREATE FUNCTION 独立事。
- **DLF 已死代码**：iceberg dlf flavor 已移除+守卫测试拦截；iceberg pom:138-145 注释过期待订正。standalone 制品存在（`com.aliyun.datalake:metastore-client-hive3:0.2.14` 在 ~/.m2）但用不到。

### 用户签字（D1/D2/D3）
- **D1=A 共享一个** `fe-connector-hms-hive-shade`；**D2=3.1.3**；**D3=复用 `shade.doris.hive.org.apache.thrift`**（零源码改动）。红队 GO + 两条件：① iceberg 摘全局 shade 与接精简 shade **原子同 commit**（消除过渡期两份 HiveCatalog 并存不确定，实测 37821B vs 37853B）；② Phase 1/4 跑类加载冒烟（每插件 `metastore.api.Table`/`TException` 各一份）+ HMS e2e + Kerberos/filter-hook 路径。

### 证据/命令
- `javap -v -p .../HiveMetaStoreClient.class | grep thrift`；`jar tf hive-catalog-shade-3.1.1.jar`（roots: paimon 18912 / hadoop 12474 / fastutil 10653 / iceberg 2972 / aliyun-datalake 2266 / hive-ql 6115 / shade-thrift 225）；验证 workflow `.claude/wf-fcl-phase0-verify.js`（4 agent，349k tok）。

### 下一步
- **Phase 1**：建 `fe-connector-hms-hive-shade` 模块（镜像 paimon-hive-shade），wire fe-connector-hms，gate 连 hive+hudi build+UT。见 HANDOFF。

### 坑/提醒
- 精简 shade 必装 `hive-**standalone**-metastore`（`hive-metastore:3.1.3` 是 13 类空壳，装错=整个 metastore api NoClassDefFound）。
- libthrift 必须**内联钉 0.9.3**（managed 默认 0.16.0 是 host doris-gen 路径，别串）。

---

## 2026-07-16 · Phase 1+2+3 完成（建精简 shade 模块 + 切换 hms/iceberg + 静态&打包闸门，分支 `catalog-spi-hive-shade-12`）

### 本轮做了什么
把 `fe/fe-connector/` 对 122MB 胖 shade 的依赖，换成一个自建的 **15MB 精简 shade 模块** `fe-connector-hms-hive-shade`（只装 Hive 元数据**客户端**闭包）。

- **新建** `fe/fe-connector/fe-connector-hms-hive-shade/pom.xml`：bundle `hive-standalone-metastore:3.1.3`(元数据 api+客户端) + `hive-common:3.1.3`(HiveConf) + `hive-serde:3.1.3` + `hive-storage-api:2.7.0` + `iceberg-hive-metastore:1.10.1`(HiveCatalog) + `libthrift/libfb303:0.9.3` + `jackson-mapper/core-asl:1.9.2`（HMS 事件解析用的老 Jackson）+ `commons-lang:2.6`（HiveConf 用）。重定位 `org.apache.thrift`→`shade.doris.hive.org.apache.thrift`（**沿用**旧前缀，零源码改动）+ 防御性重定位 fastutil。
- **共享库改依赖** `fe-connector-hms/pom.xml`：删胖 shade、加精简 shade（compile 传递 → hive/hudi/iceberg 经它拿到）。补丁客户端 `HiveMetaStoreClient.java` **原地不动**（字节码已全是重定位名）。
- **iceberg** `fe-connector-iceberg/pom.xml`：删掉它自挂的那条胖 shade 直接依赖（改经 hms 传递拿精简 shade），并补 `iceberg-bundled-guava`(compile，供 vendored DeleteFileIndex 编译)、订正过期注释。
- 与 hms 切换**放在同一次原子提交**（用户拍板），消除 iceberg 过渡期两份 HiveCatalog 并存的红队隐患。

### 关键决策落地方式（与 tasklist 里旧措辞的差异，以此处为准）
- 重定位前缀用 **`shade.doris.hive.org.apache.thrift`**（非 tasklist FCL-10 旧写的 `org.apache.doris.hms.shaded.thrift`）—— 对齐 design §4 已签字的 D3。
- 版本**在 shade 模块内联钉**（未动 fe/pom.xml dependencyManagement），libthrift 0.9.3 直接声明胜出 managed 0.16.0。
- artifactSet 用 **白名单 `<includes>`**（不是黑名单 excludes）：胖 shade 是"厨房水槽"式全打包（122MB 大量 hadoop-yarn/curator/jersey/jetty/kerby/sqlserver junk 由 hive-shims-0.23 拖入），精简版只白名单 hive 客户端闭包，其余运行时由各插件自带的 hadoop-common 闭包/宿主 parent-first 提供。

### 闸门证据（本轮实测）
- **UT 全绿**：hms/hive/hudi/iceberg build+UT（`-am`，build-cache off，跑到 package）全 SUCCESS，197 个测试类，0 fail/error，checkstyle 0。
- **两处运行时缺类由闸门抓出并补齐**（白名单方法的预期迭代）：① `HmsEventParser` 静态 `JSONMessageDeserializer` 需老 Jackson `org.codehaus.jackson.map.ObjectMapper`（且 fe/pom.xml 把 jackson-mapper-asl 钉成 test scope，须显式 `<scope>compile</scope>` 覆盖才会被 shade）；② iceberg vendored `DeleteFileIndex` 编译需 `iceberg-bundled-guava`（iceberg-core 只 runtime 带，编译期缺）。
- **静态闸门 FCL-30**：全部 19 个 fe-connector 模块 `dependency:tree -Dincludes=hive-catalog-shade` 全空。
- **打包闸门 FCL-31**：hive/hudi/iceberg 三个插件 zip 内——无胖 shade jar、精简 shade jar 各 1 份、无原包 libthrift/fe-thrift；`metastore.api.Table` / `HiveConf` / `iceberg.hive.HiveCatalog` / 重定位 `TException` / 老 Jackson `ObjectMapper` **各仅 1 份**；`HiveMetaStoreClient` 2 份（补丁 `fe-connector-hms-*.jar` + 精简 shade 未补丁，前者字典序在前→补丁生效，与胖 shade 时代同机制）。插件 zip 体积 hive 53M / iceberg 94M / hudi 200M（各比胖 shade 时代少约 107M）。
- **多 agent 对抗 review（clean-room）**：4 lens × 逐条 adversarial 复核，**零 confirmed/blocker**。要点：精简 shade 里 metastore + iceberg.hive 字节码与胖 shade **md5 逐类相同**；重定位完整（loaded path 上零原包 thrift）；`hive-storage-api 2.7.0` 是 hive-3.1.3 **原生**版本（胖 shade 的 2.8.1 是被 orc/hive-exec 上抬的、精简版已排除二者），非回退；`serde2.dynamic_type` 两个类残留原包 thrift 引用是**死路径+与胖 shade 逐字节相同**，非本次引入。

### 下一步
- **Phase 4 e2e（唯一真闸门）**：docker 异构 HMS 跑 hive 读写 / iceberg-on-HMS INSERT/DELETE/MERGE(断言与独立 iceberg 目录同结果) / hudi-on-HMS 读；TCCL 不回归（filter hook / string dict filter / kerberos）；FE 启动+缓存冒烟；顺带专项验证 storage-api 2.7.0 的 write/ACID 路径。
- Phase 5：结项 + PR（引用 tracking issue `apache/doris#65185`）。

### 坑/提醒（留给下一个 session）
- 白名单 shade 的运行时缺类只有跑到才暴露：**hms UT 抓 Jackson、iceberg 编译抓 bundled-guava** 都是这轮现补的；e2e 可能再暴露 kerberos/filter-hook 路径的缺类，按同法（查缺 → 加 include/显式 dep）补即可，别退回黑名单。
- `-pl` 选精简 shade 模块要用 **`:fe-connector-hms-hive-shade`**（冒号 artifactId 选择器），裸名 maven 找不到。
- 后台跑 maven **别** `nohup ... &` 套在 `run_in_background` 里（会脱离 harness 跟踪，误报"完成"）；本轮踩过，改用 `tail --pid` 阻塞等真结果。
