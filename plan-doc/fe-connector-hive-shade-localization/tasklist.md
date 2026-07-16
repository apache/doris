# Task list — `fe/fe-connector` 剥离 `hive-catalog-shade`

> 唯一进度清单。每完成一项随 commit 勾 `[x]` 并在 `progress.md` 追加一行。
> 设计/为什么看 [`design.md`](./design.md)（决策 D1–D6）；下一步看 [`HANDOFF.md`](./HANDOFF.md)。
> 基线 2026-07-16 `catalog-spi-11-hive`。**行号信 HEAD 不信文档**。

**总成功判据**：对 `fe/fe-connector/` 每个 pom，`dependency:tree -Dincludes=org.apache.doris:hive-catalog-shade` 全空。
（`fe/pom.xml`、avro-scanner、java-udf 仍命中 = 预期，不算破。）

---

## Phase 0 — 侦察 + 设计定案（**动码前必须做完 + 用户签字 D1/D2**）

- [x] **FCL-01** 枚举完成：补丁客户端 + `ThriftHmsClient` 用重定位 thrift（`shade.doris.hive.*`）；`HiveConnectorTransaction`/`*SchemaUtils` 用 host raw thrift 0.16.0（**不受迁移影响**）。清单入 progress。
- [x] **FCL-02** 🔴 **定案**：`javap` 出补丁客户端字节码对 thrift 48 处引用**全是重定位名**、零 raw、零按名反射 → **D4=KEEP_IN_HMS**（不搬不改写）。
- [x] **FCL-03** 版本确认：`hive.version=3.1.3`（fe/pom.xml:341）；补丁客户端头"Copied From release-3.1.3" → **D2=3.1.3**。
- [x] **FCL-04** **D1=A 共享 + D3=复用 `shade.doris.hive.org.apache.thrift`**（非原设计的新前缀），中文向用户解释并**已签字**。
- [x] **FCL-05** 确认：iceberg 需 `iceberg-hive-metastore`（HiveCatalog）**与** hms 客户端共用同一重定位 thrift（支撑 D1-A）；DLF 已死代码不计。
- [x] **FCL-06** design.md §3 补精确 bundle 清单、§4 决策定案留痕。

**Phase 0 出口**：✅ design.md 定案 + 用户签 D1/D2/D3 + FCL-02 定 D4=KEEP_IN_HMS。**红队 GO**（两条件：iceberg 原子切换 + Phase1/4 类加载冒烟&e2e）。

---

## Phase 1 — `fe-connector-hms` 局部 shade（承重墙）

- [x] **FCL-10** 新建模块 `fe/fe-connector/fe-connector-hms-hive-shade/pom.xml`（镜像 paimon-hive-shade）：bundle `hive-metastore`(D2 版本) + `hive-serde` + `hive-common` + `libthrift`；重定位 `org.apache.thrift`→`org.apache.doris.hms.shaded.thrift`；防御性重定位 `it.unimi.dsi.fastutil`；artifactSet 排除 hadoop/guava/protobuf/slf4j/log4j/commons-*/gson/jackson/caffeine；META-INF SF/DSA/RSA/maven 过滤。（D1-A 则此处也 bundle `iceberg-hive-metastore`——见 FCL-20。）
- [x] **FCL-11** `fe/fe-connector/pom.xml` `<modules>` 注册**在 `fe-connector-hms` 之前**；`fe/pom.xml` dependencyManagement 补 `libthrift`(0.9.x?按 D2 版本对应) + `hive-metastore`(D2) 版本钉（或 shade 模块内联 pin）。
- [x] **FCL-12** 🔴 落地 D4：按 FCL-02 结论处理 vendored `HiveMetaStoreClient.java`（纳入 shade 编译 / 保留 / 改写），确保它与重定位后的基类命名空间一致。
- [x] **FCL-13** 改 `fe-connector-hms/pom.xml`：删 `hive-catalog-shade` 依赖，加 `fe-connector-hms-hive-shade`。核对 raw hive-metastore/thrift 若有残留改 `<optional>`/exclude；plugin-zip 保留 `libthrift`/`fe-thrift` exclude。
- [x] **FCL-14** Gate：`fe-connector-hms` UT（`-am`，`-Dmaven.build.cache.enabled=false`，跑到 `package`）；连带 `fe-connector-hive` + `fe-connector-hudi` build + UT（它们经 hms 传递）。全绿 + checkstyle 0。
- [x] **FCL-15** commit（英文）+ 更新 HANDOFF + progress 追加。

---

## Phase 2 — `fe-connector-iceberg` 迁移

- [x] **FCL-20** iceberg-hive-metastore 落位：D1-A 则已在 FCL-10 的共享 shade 里（本步仅核对）；D1-B 则单独 shade 且与 hms 共用同一重定位 thrift 命名空间。
- [x] **FCL-21** 改 `fe-connector-iceberg/pom.xml`：删 `hive-catalog-shade`（第 93 行那条）直接依赖，改依赖共享/iceberg shade。核对 `fe-connector-hms`（它已带客户端）+ 新 shade 无重复类；更新 plugin-zip exclude。
- [x] **FCL-22** Gate：`fe-connector-iceberg` UT（含 `assembleHiveConf` parity 测试等，`-am`，build-cache off，跑到 `package`）；checkstyle 0。
- [x] **FCL-23** commit（英文）+ 更新 HANDOFF + progress 追加。

---

## Phase 3 — 证明 `fe/fe-connector` 已脱钩（静态闸门）

- [x] **FCL-30** 对 `fe/fe-connector/` **每个** pom 跑 `dependency:tree -Dincludes=org.apache.doris:hive-catalog-shade` → **全空**（这是总判据）。命中的只应剩 avro-scanner/java-udf/fe-pom（不在范围）。
- [x] **FCL-31** `unzip -l` 三个插件 zip（hive/iceberg/hudi）：断言① 无 `hive-catalog-shade-*.jar`；② 无原包 `org/apache/thrift/`（除 host provide）；③ 有 `org/apache/doris/hms/shaded/thrift/`；④ 无重复 `hive-metastore`/`libthrift`/`HiveConf`；⑤ 记录 zip 体积删前/删后差。
- [x] **FCL-32** 确认 `fe/pom.xml` 的 hive-catalog-shade 版本钉**仍在**（avro-scanner/java-udf 还要），并加注释说明为何保留。

---

## Phase 4 — e2e（异构 HMS，唯一真闸门；memory `hms-iceberg-delegation-needs-e2e`）

- [ ] **FCL-40** docker 外表 HMS 套件：① 普通 hive catalog 读+写；② **iceberg-on-HMS** INSERT/DELETE/MERGE/read，断言与独立 iceberg 目录同表同结果；③ hudi-on-HMS 读。
- [ ] **FCL-41** 🔴 TCCL 不回归（D5/R2）：`test_string_dict_filter` 类用例 + MetaStoreFilterHook 路径 + kerberos HMS（若环境有）；FE 启动 + 缓存（MetaCache/StatisticsCache/FileSystemCache）冒烟。
- [ ] **FCL-42** progress 追加 e2e 结果（含 CI 编号）。

---

## Phase 5 — 收尾

- [ ] **FCL-50** `progress.md` 结项段；`../decisions-log.md` 补 `D-NNN`（本任务的 D1–D6 定案）；PR（base `branch-catalog-spi`，squash，英文）。引用 tracking issue `apache/doris#65185`。

---

## 🧰 构建/验证坑（兄弟任务实测，直接复用，别再踩）

1. **maven build cache 会静默跳过 surefire**（`Skipping plugin execution (cached): surefire:test`，BUILD SUCCESS 但 0 测试）→ **必须 `-Dmaven.build.cache.enabled=false`**，并数 `grep -c "^\[INFO\] Running org.apache.doris"`。
2. **`-am` 必填**：漏了报 `org.apache.doris:fe:pom:${revision}` 无法解析 = **没编译，不是代码错**。
3. **依赖 shade 模块的模块必须跑到 `package`**：`test-compile`/`test` 会假报 `package org.apache.hadoop.hive.conf does not exist`（不是代码错）；`install` 不行（`fe-type` quirk）。
4. **`surefire:test` 独立 goal 解析不了 `${revision}`** → 走 `test` 生命周期 + `-am`；无匹配测试加 `-DfailIfNoTests=false`。
5. **连接器模块路径嵌套**：`-pl fe/fe-connector/fe-connector-XXX` 用相对 reactor 名 `-pl fe-connector-XXX`（在 fe/pom.xml reactor 内）；maven 必须**绝对 `-f`**：`mvn -o -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml ...`。
6. **`hive-serde` 闭包首次需联网**（`javax.servlet:servlet-api:2.4` 不在本地仓），`-o` 会失败——首次去 `-o`。
7. **`mvn ... | tail` 后 `$?` 是 tail 的**：重定向到文件再读 `BUILD SUCCESS/FAILURE` 行。
8. **变异验证要红在断言上**（不是 checkstyle/编译），变异代码也须合 checkstyle。
9. **`git add` 用 path-whitelist，严禁 `git add -A`**（工作树有大量非本线程 scratch，含 `regression-conf.groovy` 本就脏）。commit 后看 `git show --stat` 文件数。
10. **动码前先探并发**（`git log`/`status` + `pgrep maven` 看 `etime` 别误判僵尸 + 近 90s mtime）。
