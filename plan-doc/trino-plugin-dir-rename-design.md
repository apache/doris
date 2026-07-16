# Trino 子插件目录改名设计（`plugins/connectors/` → `plugins/trino_plugins/`）

日期：2026-07-15
状态：**已实现并提交**（`3aebe84ec85`）。验证与欠账见 §8。

## 1. 问题

`output/fe/plugins/` 下并排躺着两个只差一个 `s` 的目录，含义完全不同：

| 目录 | 内容 | 谁产生 | 发布状态 |
|---|---|---|---|
| `plugins/connector/`（单数） | Doris 连接器插件（es/jdbc/hive/iceberg/paimon/...），`build.sh:1069-1083` 解压各模块 zip | 构建部署 | **本特性分支新增，未发布** |
| `plugins/connectors/`（复数） | **Trino 自己的插件**（"插件的插件"），喂给 Trino 的 `ServerPluginsProvider` | 用户手动投放，`build.sh:1045` 只 `mkdir` 空目录 | **2.1.8 起已发布，有真实用户** |

这不是审美问题，是真实成本：`plan-doc/00-connector-migration-master-plan.md:57` 把新框架的部署路径写成了 `plugins/connectors/<name>/`（复数），**跟实现的单数对不上** —— 文档作者当场就踩了这个坑。运维脚本或用户敲错一个字母不会报错，只会静默拿到错误行为。

trino-connector 迁入 `fe/fe-connector` 框架后，两个目录的语义分层更刺眼：`plugins/connector/trino/` 是 Doris 的 trino 连接器插件，`plugins/connectors/` 是那个插件要加载的 Trino 子插件。

## 2. 关键事实（侦察结论）

1. **`plugins/connectors/` 是 Trino 的 `installedPluginsDir`。** `TrinoConnectorPluginLoader.java:89-96` 与 `TrinoBootstrap.java:127-133` 把它交给 `ServerPluginsProviderConfig.setInstalledPluginsDir()`，其下每个子目录是一个 Trino 插件，各带自己的 jar。

2. **FE / BE 两份，各读各的，且 BE 侧未迁移。**
   - FE：`Config.trino_connector_plugin_dir`（`Config.java:2873`）→ `DefaultConnectorContext.java:563` 放进 environment → 插件内 `TrinoBootstrap.resolvePluginDir()` 解析。**已插件化。**
   - BE：`be/src/common/config.cpp:1543` 独立的同名 config → `format_v2/jni/trino_connector_jni_reader.cpp:134`（及 V1 的 `format/table/trino_connector_jni_reader.cpp:112`）经 JNI 调 `TrinoConnectorPluginLoader.setPluginsDir()`。**仍是老的 be-java-extensions，本轮迁移没碰过。**

3. **该目录已搬家过一次，且留下了标准手法。** 2.1.8 把默认值从 `DORIS_HOME/connectors` 改为 `DORIS_HOME/plugins/connectors`，FE（`TrinoBootstrap.java:356-366`）与 BE（`TrinoConnectorPluginLoader.java:124-137`）各留了"老目录存在且非空则用老目录"的兜底。

4. **`plugins/filesystem/` 与 `plugins/connector/` 是刻意的平行结构。** `build.sh:1051` 与 `1069` 同构：单数目录名 + `<name>/` 子目录 + 解压 zip。

5. **`plugins/jdbc_drivers/` 是既有先例**：用户投放、服务单一连接器的第三方产物，挂在 `plugins/` 顶层。

## 3. 约束

> **已被 §12 推翻（2026-07-16 用户拍板）**：不再考虑任何兼容，FE/BE 兜底全删。C2、C3 不受影响。

**C1（用户拍板）：必须兼容。** 老部署 `plugins/connectors/` 里的 Trino 插件升级后必须继续可用。失效的表现是 `LOG.warn` 吞掉 + catalog 建得出来但查不了，排查代价高。

**C2：`connectors` 这个名字永久烧毁，谁都不能占。** 因为 C1 要求 fallback 永远去读 `plugins/connectors/` 找遗留 Trino 插件；若新框架占用该名，老部署里这个目录装的是 Doris 连接器插件 → fallback 判"非空" → 把 Doris 连接器插件喂给 Trino 的 `ServerPluginsProvider` → 灾难。

**C3：不能嵌套进 `plugins/connector/trino/`。** 两个理由：(a) BE 侧根本没有 `plugins/connector/` 这棵树，照此设计 FE 装 X、BE 装 Y，安装文档从"两边同路径"退化为"两边不同路径"，比现状更糟；(b) 用户数据混入 build 管理的目录，部署脚本一旦 `rm -rf` 重解压即丢失。

## 4. 目标布局

```
plugins/
├── filesystem/<name>/     构建部署 · build.sh:1051
├── connector/<name>/      构建部署 · build.sh:1069        ← 不动，平行结构保住
├── trino_plugins/         用户投放 · Trino 自己的插件      ← 新家
├── jdbc_drivers/          用户投放 · JDBC 驱动
├── java_extensions/       用户投放 · 自定义 jar
├── hadoop_conf/           用户投放
└── connectors/            仅遗留读取 · build.sh 不再创建   ← 退役
```

**命名规约：构建部署的插件树用单数名 + `<name>/` 子目录；用户投放区用描述性复合名。** `trino_plugins` 紧邻 `jdbc_drivers`，同类同名法。

`build.sh` 不再 `mkdir` `connectors/` 是设计的一部分：**全新安装从此不存在该目录**，fallback 的"存在且非空"自然永不触发。

## 5. 解析链

> **已被 §12 推翻**：第 3、4 步（legacy 目录探测）已删除，解析链只剩「属性优先，否则逐字使用配置值」两步。§5.1 的同步点与 §5.2 的 memoize 随之整体消失。下文保留原始记录。

FE 与 BE 各自独立解析，但用同一套优先级。以 `TrinoBootstrap.resolvePluginDir()` 为准：

```
1. catalog 属性 trino.plugin.dir 非空             → 用它                              （不变）
2. trino_connector_plugin_dir ≠ 默认值            → 用它（语义 = 用户显式设过）        （不变）
3. DORIS_HOME/connectors 存在且非空                → 用它（pre-2.1.8 遗留）             （不变）
4. DORIS_HOME/plugins/connectors 存在且非空        → 用它（2.1.8~当前 遗留）            （新增）
5. 否则                                            → DORIS_HOME/plugins/trino_plugins  （新默认）
```

第 3 步排在第 4 步前，是**刻意保持现有行为不变**：今天的代码就是"老的非空就用老的"，本设计只在链尾插一环，不动既有次序。BE 的 `TrinoConnectorPluginLoader.checkAndReturnPluginDir()` 镜像同一条链。

### 5.1 三处一体的常量（隐蔽同步点）

> **已被 §11 修正**：实为**四处**（漏数了 `TrinoConnectorPluginLoader`）。Java 的三份现已收敛为 `TrinoPluginDirs.DEFAULT_PLUGIN_SUBDIR` 单一真相，只剩 `config.cpp` 一份靠注释同步。下表保留原始记录。

第 2 步靠"配置值 ≠ 默认值"判断用户是否显式设过。这要求以下三处字面量**必须同步**，一旦漂移就会把"用户没设过"误判成"设过"：

| 位置 | 形态 |
|---|---|
| `fe/fe-common/.../Config.java:2873` | `EnvUtils.getDorisHome() + "/plugins/trino_plugins"` |
| `be/src/common/config.cpp:1543` | `"${DORIS_HOME}/plugins/trino_plugins"` |
| `fe/fe-connector/fe-connector-trino/.../TrinoBootstrap.java:349` | 硬编码字面量（插件跨 classloader 读不到 `Config` 类，只能靠字面量对齐） |

实现时须在三处各加注释交叉指认。

### 5.2 与 fail-loud 单例的交互（本设计新增的修复）

`e17c8601201` 之后 `TrinoBootstrap` 是进程级单例，`getInstance()` 遇到不同 plugin dir 抛 `IllegalStateException`。而 §5 的第 3/4 步是**探测文件系统**决定的，即解析结果依赖探测时刻的磁盘状态：

> catalog A 于 T1 建立 → `plugins/connectors/` 当时为空 → 解析到 `plugins/trino_plugins/` → 单例锁定。
> 用户于 T2 照旧文档往 `plugins/connectors/` 丢了插件。
> catalog B 于 T3 建立 → 解析到 `plugins/connectors/` → `getInstance()` 不一致 → 抛异常，catalog B 不可用。

这是今天就存在的隐患（两级 fallback 时窗口小），加到三级会放大。

**决策：把 fallback 探测结果按 `doris_home` memoize 一次，进程内所有 catalog 复用。**

理由不是防御性编程，而是**它更诚实**：Trino 的插件只在 `TrinoBootstrap` 构造时加载一次，运行中往目录里丢新插件在 FE 重启前根本不生效。既然如此，解析时机就该与加载时机对齐；每个 catalog 重新探一次磁盘，制造的是"探测结果会变但加载结果不会变"的假象。memoize 后进程内必然一致，上述异常场景从"可能发生"变为结构上不可能。

实现用 `ConcurrentHashMap<String, String>` 以 `dorisHome` 为 key（而非裸 static 字段）：生产环境 `doris_home` 恒定 → 只探一次；单测各用各的临时目录 → 天然隔离，无测试污染。

§5 的第 1、2 步是入参的纯函数，不 memoize。

## 6. 改动清单

| 文件 | 改动 |
|---|---|
| `fe/fe-common/src/main/java/org/apache/doris/common/Config.java:2873` | 默认值 → `/plugins/trino_plugins` |
| `be/src/common/config.cpp:1543` | 默认值 → `${DORIS_HOME}/plugins/trino_plugins` |
| `fe/fe-connector/fe-connector-trino/.../TrinoBootstrap.java` | `resolvePluginDir()`：新默认值 + 插入第 4 步 + memoize |
| `fe/be-java-extensions/trino-connector-scanner/.../TrinoConnectorPluginLoader.java` | `pluginsDir` 默认值 + `checkAndReturnPluginDir()` 镜像同链 |
| `build.sh:1045` | `fe/plugins/connectors/` → `fe/plugins/trino_plugins/` |
| `build.sh:1271` | `be/plugins/connectors/` → `be/plugins/trino_plugins/` |
| `fe/fe-connector/fe-connector-trino/src/test/.../TrinoBootstrapTest.java` | 更新既有 3 个 case + 补新链的 case |
| `plan-doc/00-connector-migration-master-plan.md:57` | 修文档漂移：`plugins/connectors/<name>/` → `plugins/connector/<name>/` |

**不改**：`regression-test/.../Suite.groovy:1304-1306`。回归环境在 fe.conf/be.conf 里显式设 `trino_connector_plugin_dir=/tmp/trino_connector/connectors`，走第 2 步"显式设过"直接返回，不受默认值变更影响，行为零变化。

## 7. 兼容矩阵

> **已被 §12 推翻**：兼容行为已删除，真实的升级影响见 §12.5（含破坏性变更说明）。下表保留原始记录。

| 部署形态 | `connectors/` | `plugins/connectors/` | 解析结果 | 结论 |
|---|---|---|---|---|
| 全新安装 | 不存在 | 不存在（build.sh 不再建） | `plugins/trino_plugins/` | 新布局 |
| 2.1.8~当前升级上来 | 不存在 | 非空 | `plugins/connectors/`（第 4 步） | **零感知，继续可用** |
| pre-2.1.8 从未迁移 | 非空 | 空/不存在 | `connectors/`（第 3 步） | 行为不变 |
| fe.conf 显式设了值 | — | — | 配置值（第 2 步） | 行为不变（含回归环境） |
| 显式设成旧默认字面量 | — | — | 该字面量（第 2 步，因 ≠ 新默认） | 正确 |

## 8. 验证结果与欠账（2026-07-15）

**已验**：

- `TrinoBootstrapTest` **9 个 case 全绿、0 skip**，覆盖 §7 兼容矩阵五行 + §5.1 常量守门。
- **两条关键 case 各做了变异测试**（证明它们咬得住，而非只是碰巧通过）：
  - 摘掉 `computeIfAbsent` → `legacyProbeIsMemoizedSoEveryCatalogInAProcessAgrees` 如期失败（第一次解析得 `plugins/trino_plugins`、第二次得 `plugins/connectors`，正是 §5.2 会触发 `IllegalStateException` 的分歧），其余 8 条不受影响。
  - 把 `Config.java` 默认值改成 `/plugins/drifted_name` → `feConfigDefaultMatchesThePluginsHardcodedDefault` 如期失败，其余 8 条不受影响。
- `build.sh` 过 `bash -n`；`be-java-extensions/trino-connector-scanner` 编译 + checkstyle 通过。

**§10 第 3 条的落地结论**：`fe-connector-trino` 的 pom **确有** compile-scope 的 fe-common 依赖（为 `TrinoColumnMetadata` 引入），故按设计走"有则加断言测试"分支：`DEFAULT_PLUGIN_SUBDIR` 改为 package-private + `@VisibleForTesting`，测试双向断言它与 `Config.trino_connector_plugin_dir` 一致（改任一侧都会失败）。BE 的 `config.cpp` 是 Java 测试够不到的第三份副本，仍只靠三处交叉注释。

> **已被 §11 推翻**：该断言测试要 import `org.apache.doris.common.Config`，撞上 connector 导入门禁而挂 CI（build 997269）。现改为常量去重，漂移不再可能，断言测试已删。

**欠账**：

1. **BE 未跑全量构建**。`config.cpp:1543` 仅改字符串字面量（既有 `DEFINE_String` 内），风险接近零，但严格说未经编译验证。
2. **无 e2e**。三级 fallback 只有单测覆盖。真集群回归要跑到它需要构造"老部署遗留目录非空"的形态，而回归环境走的是显式配置（§6"不改"一节），天然绕开 fallback —— 即**现有回归跑绿不构成 fallback 的证据**。
3. **需 release note**。默认值变更对用户可见：老部署靠 fallback 零感知，但新装的用户投放点从 `plugins/connectors/` 变为 `plugins/trino_plugins/`，文档（含 doris-website 的 trino-connector 安装说明）须同步。

## 9. 实现期的并发阻塞（已解除，留档）

设计批准时探测到 `sh build.sh --be`（PID 1515583）正在跑，§6 的四个源文件全在其必经之路上 —— 含**正在被执行的 `build.sh` 本体**（`sh` 按字节偏移边读边执行，而 1271 行的 `mkdir` 位于 ninja 编完才走到的 BE 打包段，改动会令其从错位偏移读入新字节）。故当轮只写设计、代码一行未动，挂 Monitor 等该构建于 16:56:36 退出后才施改。留档理由：这是共享工作树上的复发型风险，下次改 `build.sh` 或 `be/src/` 前同样要先探。

## 10. 验收标准（原始定义，逐条结果见 §8）

1. `TrinoBootstrapTest` 覆盖 §7 兼容矩阵全部 5 行，且每个 case 断言的是"为什么这样解析"而非仅"解析成了什么"。
2. memoize 后，同一 `doris_home` 下两次 `resolvePluginDir()` 在中途改变磁盘状态时返回同一结果（§5.2 的回归锁）。
3. §5.1 三处常量一致。实现时先确认 `fe-connector-trino` 的测试 classpath 上是否有 fe-common：**有**则加一个断言 `TrinoBootstrap` 的默认值字面量与 `Config.trino_connector_plugin_dir` 一致的测试；**没有**（该模块以零 fe-core 依赖为目标，fe-common 亦可能不在链上）则退回三处注释交叉指认，不硬造依赖来测。BE 侧跨进程无法断言，一律靠注释。
   > **已被 §11 推翻**：这条给的两个分支都错——它默认了"常量必须各存一份，只能事后检测漂移"。真正的第三条路是让 Java 侧共享同一个编译期常量，从源头消灭漂移。
4. `build.sh --fe --be` 后 `output/{fe,be}/plugins/` 下有 `trino_plugins/`、无 `connectors/`。

## 11. 施工后修正：常量去重取代断言守门（CI 997269）

§8 落地的"断言守门"方案在 CI 上挂了，本节记录推翻它的理由与替代方案。**§8 第 3 段与 §10 第 3 条已作废，以本节为准。**

### 11.1 为什么原方案必然挂

`TrinoBootstrapTest` 为断言两侧一致，必须 `import org.apache.doris.common.Config`。而 `tools/check-connector-imports.sh` 禁止 fe-connector 模块引用 `org.apache.doris.(catalog|common|datasource|qe|...)`——**且它扫 `src/test/java`**。

时间线说明这不是巧合：

| 日期 | 提交 | 内容 |
|---|---|---|
| 2026-07-12 | `40757d9e453` | 门禁"补 4 个漏洞"，其**第 3 个漏洞正是"只扫 src/main、漏了 src/test"**，并配 self-test 专门 seed 一个 test-source import 当违规样例 |
| 2026-07-15 | `5e9d9449767` | 本设计的断言测试落地，正好踩中该漏洞 |

即门禁没坏（self-test 至今 PASS），是断言测试撞在门禁三天前刚堵上的洞里。**结论：不能改门禁迁就测试**——那等于把刚补的漏洞重新捅开。

### 11.2 原方案的思维盲区

§10 第 3 条把选择限定成"能测就加断言 / 不能测就靠注释"，二者都默认了**常量必须各存一份**。但插件读不到 `Config` 的是**运行时**（隔离 classloader），**编译期**完全读得到——`fe-connector-trino` 本就有 compile-scope 的 fe-common 依赖。于是存在第三条路：共享同一个编译期常量，漂移从"事后检测"变为"结构上不可能"。

### 11.3 替代方案

> **已被 §12 取代**：`TrinoPluginDirs` 已删除。放弃兼容后插件根本不需要默认值，共享常量失去存在意义。§11 的问题诊断仍成立，解法以 §12 为准。

新增 `fe-common` 的 `org.apache.doris.trinoconnector.TrinoPluginDirs.DEFAULT_PLUGIN_SUBDIR` 作为 Java 侧唯一真相，三个消费方共享：

| 消费方 | 模块 | 取用方式 |
|---|---|---|
| `Config.trino_connector_plugin_dir` | fe-common | 同模块直接引用 |
| `TrinoBootstrap`（FE 插件） | fe-connector-trino | import（`org.apache.doris.trinoconnector` 不在门禁 FORBIDDEN 名单内） |
| `TrinoConnectorPluginLoader`（BE scanner） | be-java-extensions | 同包，无需 import |

原设计说的"三处"实为**四处**——`TrinoConnectorPluginLoader` 那份第四拷贝当初被漏数了，此次一并收编。Java 3 份 → 1 份；`be/src/common/config.cpp` 仍是跨语言够不到的最后一份，继续靠注释交叉指认。

**为什么隔离 classloader 下安全**：`org.apache.doris.trinoconnector` 本就是插件运行时够得到的包，无需任何特殊论证——

- `fe-common` 是 `fe-connector-trino` 的 compile-scope 依赖，**被打进插件 zip 的 `lib/fe-common-*.jar`**（实测该 jar 内含 `org/apache/doris/trinoconnector/`）。
- `ConnectorPluginManager.CONNECTOR_PARENT_FIRST_PREFIXES` 只有 `org.apache.doris.connector.` 与 `org.apache.doris.filesystem.`，`org.apache.doris.trinoconnector.` **不在其中** → 走 child-first → 插件从自己捆绑的那份加载。
- 铁证：`TrinoScanPlanProvider.java:300` 早已 `new TrinoColumnMetadata(...)`，即插件在运行时真实实例化同包的 fe-common 类。

> **反面留档（施工中一度写错，经对抗 review 揪出）**：初版注释与本节曾声称"插件从隔离 classloader **够不到** `TrinoPluginDirs`，全靠编译期常量内联才安全"。**这是错的**。内联确实发生（`javap -v` 实证：`TrinoBootstrap.class`/`Config.class` 常量池含内联字面量、对 `TrinoPluginDirs` 符号引用数为 0；`TrinoConnectorPluginLoader.class` 因 target major 52 多留一个无指令引用的 `CONSTANT_Class` 条目，按 JVMS §5.4.3 惰性解析永不解析），但它**不是安全性的成因**——即便不内联，child-first 也照样能加载。把"我们不需要"写成"我们做不到"会误导后人以为某些去重在结构上不可能。

**为什么 legacy 目录列表不一起收编**：纯粹是本次改动范围的边界，**不是技术上做不到**。`LEGACY_PLUGIN_SUBDIRS`（`{"/connectors", "/plugins/connectors"}`）在 `TrinoBootstrap` 与 `TrinoConnectorPluginLoader` 各存一份、无守门，属**遗留的既有重复**，与本次 CI 故障无关，故不在此顺手动。**留作已知欠账**（见 §11.5）：若哪天要收编，把它挪进 `TrinoPluginDirs` 是可行的，代价只是插件多一个真实运行时引用（与既有的 `TrinoColumnMetadata` 同性质）。

### 11.4 代价与验证

代价：删掉 `feConfigDefaultMatchesThePluginsHardcodedDefault()`（漂移已不可能，断言退化为同义反复）。`TrinoBootstrapTest` 从 9 case 变 8 case，§7 兼容矩阵五行覆盖不变。

验证：门禁 exit 0 且 self-test 仍 PASS；fe-common / fe-connector-trino / trino-connector-scanner 三模块编译 + checkstyle 0 violations；`TrinoBootstrapTest` 8 run / 0 fail / 0 skip；全仓 `trino_connector_plugin_dir` 引用（regression 与 samples 的 fe.conf/be.conf）**全部显式设值**，走"显式覆盖"分支，不受默认值来源变更影响，行为零变化。

§8 三条欠账（BE 未全量构建、无 e2e、需 release note）不受本次修正影响，依旧成立。

### 11.5 本次遗留的已知欠账

1. **`LEGACY_PLUGIN_SUBDIRS` 仍是两份无守门的重复**（`TrinoBootstrap.java` 与 `TrinoConnectorPluginLoader.java`，值均为 `{"/connectors", "/plugins/connectors"}`）。漂移后果：FE 与 BE 回退到不同的遗留目录。属既有问题，本次未动（见 §11.3 末）。
2. **`build.sh` 另有两处 `plugins/trino_plugins/` 字面量**（约 1049 / 1276 行，创建投放目录用），未纳入交叉注释网。它们只决定"建哪个空目录"，与"判断用户是否显式设过"的语义链无关，漂移不会误判配置，但全新安装的投放点会与默认解析结果不一致。

## 12. 方案再定：放弃兼容，解析链砍到两步（2026-07-16 用户拍板）

**本节推翻 §3 的 C1、§5 的解析链、§7 的兼容矩阵，以及 §11 的常量去重方案。** §11 记录的问题诊断仍然成立，但它的解法已被更简单的方案取代。

### 12.1 决策

用户明确：**FE 与 BE 都不再考虑任何兼容**。只识别两个来源：

```
1. catalog 属性 trino.plugin.dir 非空  → 用它
2. 否则                                 → trino_connector_plugin_dir 配置值，逐字使用
                                          （默认 DORIS_HOME/plugins/trino_plugins）
```

§5 原链的第 3、4 步（探测 `DORIS_HOME/connectors` 与 `DORIS_HOME/plugins/connectors`）**全部删除**，FE、BE 两侧同步删。

### 12.2 连锁反应：默认值常量整体消失

这不只是删掉两个 if。**"判断用户是否显式设过"这个语义整体没了**——原第 2 步要靠"配置值 ≠ 默认值"反推用户意图，而这个反推唯一的用途就是决定要不要走第 3、4 步的兼容兜底。兜底既然不存在，反推就没有消费者。

于是 §5.1 那个"隐蔽同步点"从根上蒸发：

| 原先 | 现在 |
|---|---|
| 4 处默认值字面量必须同步，漂移会静默误判 | 插件与 scanner **完全不需要知道默认值**，各自逐字使用传入的配置值 |
| `TrinoPluginDirs`（§11 引入的共享常量） | **删除**——只剩 `Config.java` 与 `config.cpp` 各持一份普通配置默认值，不参与任何推断 |
| `Config` 依赖 `org.apache.doris.trinoconnector` | **解除**——`Config` 回到纯字面量，不再依赖 trino |

`Config.java` 与 `config.cpp` 两份默认值仍需一致，但耦合强度已完全不同：它只是"FE 和 BE 该去同一个地方找插件"这种普通的 FE/BE 配置对齐，漂移的后果是直白的加载不到，**不再有"把没设过误判成设过"这种静默错判**。

### 12.3 顺带消失的复杂度

- `LEGACY_PLUGIN_SUBDIRS`（FE + BE 各一份，§11.5 记的第 1 条欠账）——**随兜底一起删除，该欠账关闭**。
- `probeLegacyDirs()` / `isNonEmptyDir()`（FE）、`checkAndReturnPluginDir()` 的探测分支（BE）——删除。
- `RESOLVED_DEFAULT_DIRS` 的 memoize 与 §5.2 那一整节的分析——**删除**。memoize 的存在理由是"探测读文件系统，答案会随磁盘状态变化，而单例 fail-loud 受不了答案漂移"。现在 `resolvePluginDir()` 是入参 `(properties, environment)` 的纯函数，不碰文件系统，§5.2 描述的 `IllegalStateException` 场景从"靠 memoize 压住"变成结构上不可能。
- `TrinoBootstrap` 不再需要 env 里的 `doris_home`（仅日志路径仍用 `System.getenv("DORIS_HOME")`，与解析无关）。

### 12.4 fail-loud 的取舍

`DefaultConnectorContext:563` 无条件 `env.put("trino_connector_plugin_dir", Config.trino_connector_plugin_dir)`，而 `Config` 的该字段恒有值，故 env 里缺这个 key 只可能是引擎侧的 bug。此时**抛 `IllegalStateException`**，而不是猜一个目录：猜错的表现是"catalog 建得出来但每个查询都失败"，排查代价极高；抛在解析点则原因就在现场。

### 12.5 用户可见的破坏性变更（必须写 release note）

**这是本设计里唯一一处真正的破坏性变更，且失败是静默的。**

已发布版本（2.1.8 起）的默认投放点是 `DORIS_HOME/plugins/connectors`。升级到本改动后：

| 部署形态 | 升级后行为 |
|---|---|
| 插件在 `plugins/connectors/`，fe.conf/be.conf 未设该配置 | **插件不再被加载**。catalog 建得出来，每个查询报 "Cannot find Trino ConnectorFactory" |
| 插件在 `DORIS_HOME/connectors/`（pre-2.1.8），未设配置 | 同上 |
| fe.conf/be.conf 显式设了 `trino_connector_plugin_dir` | 行为不变（含回归环境，见 §6"不改"一节） |
| 全新安装 | 行为不变，投放到 `plugins/trino_plugins/` |

**操作者需二选一**：把插件移到 `plugins/trino_plugins/`，或把 `trino_connector_plugin_dir` 指到现有目录。FE 与 BE 两侧都要做。

### 12.6 验证

- 门禁 exit 0；`Config.java` 已无任何 trino import（本轮三条要求之一）。
- fe-common / fe-connector-trino / trino-connector-scanner 三模块编译 + checkstyle 通过（`-Dmaven.build.cache.enabled=false` 强制真跑，避免 build cache 静默跳过）。
- `TrinoBootstrapTest` 重写为 4 case、全绿 0 skip：属性优先、配置逐字生效、**legacy 目录即使有插件也不被理会**、env 缺 key 则 fail-loud。
- **变异测试**（证明回归锁咬得住）：把 legacy 兜底加回 `resolvePluginDir()` → `legacyPluginDirsAreNotConsultedEvenWhenTheyHoldPlugins` 如期变红（`expected: .../plugins/trino_plugins but was: .../plugins/connectors`），其余 3 条不受影响；还原后复绿。

### 12.7 本节关闭/保留的欠账

- **关闭**：§11.5 第 1 条（`LEGACY_PLUGIN_SUBDIRS` 重复）——数组已随兜底删除。
- **保留**：§11.5 第 2 条（`build.sh` 两处 `plugins/trino_plugins/` 字面量）。
- **保留并升级为必办**：§8 欠账第 3 条（release note）。原先它只是"新装用户投放点变了"的提示，现在是**升级即静默失效**的破坏性变更，doris-website 的 trino-connector 安装说明必须同步。
- **保留**：§8 欠账第 1、2 条（BE 未全量构建、无 e2e）。
