# Trino 子插件目录改名设计（`plugins/connectors/` → `plugins/trino_plugins/`）

日期：2026-07-15
状态：设计已批准，**实现被并发构建阻塞**（见 §8）

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

| 部署形态 | `connectors/` | `plugins/connectors/` | 解析结果 | 结论 |
|---|---|---|---|---|
| 全新安装 | 不存在 | 不存在（build.sh 不再建） | `plugins/trino_plugins/` | 新布局 |
| 2.1.8~当前升级上来 | 不存在 | 非空 | `plugins/connectors/`（第 4 步） | **零感知，继续可用** |
| pre-2.1.8 从未迁移 | 非空 | 空/不存在 | `connectors/`（第 3 步） | 行为不变 |
| fe.conf 显式设了值 | — | — | 配置值（第 2 步） | 行为不变（含回归环境） |
| 显式设成旧默认字面量 | — | — | 该字面量（第 2 步，因 ≠ 新默认） | 正确 |

## 8. 阻塞项（2026-07-15）

实现被并发构建阻塞，**代码一行未动**。探测结果：

- `sh build.sh --be`（PID 1515583，14:38:27 启动）在跑，子进程 `ninja -j 5` 正编 `src/exec/`，Release 模式。
- HEAD 由 `aeb5f1dcf25` 变为 `fa2fcf4b246`（另一 session 提交，记录 rebase 到 `c0865b021b0`）。

§6 的四个源文件全在该构建必经之路上：

- `build.sh` —— **正在被执行的脚本本体**。`sh` 按字节偏移边读边执行；1271 行的 `mkdir` 位于 BE 打包段（ninja 编完才走到），即构建尚未读到该处，此时改动会令其从错位偏移读入新字节。
- `be/src/common/config.cpp` —— 正被 ninja 编译。已编过则产出二进制含旧默认值而源码写新值（部署后测 trino 得到旧行为，极其误导）；未编到则产出混合树。
- `TrinoConnectorPluginLoader.java` —— `--be` 带 `BUILD_BE_JAVA_EXTENSIONS`，maven 正编 `trino-connector-scanner` 模块。
- `Config.java` —— fe-common 在 be-java-extensions 依赖链上（`TrinoConnectorPluginLoader` import 了 `EnvUtils`）。

**解除条件**：该构建结束（或用户确认可打断）后方可动手。

## 9. 验收标准

1. `TrinoBootstrapTest` 覆盖 §7 兼容矩阵全部 5 行，且每个 case 断言的是"为什么这样解析"而非仅"解析成了什么"。
2. memoize 后，同一 `doris_home` 下两次 `resolvePluginDir()` 在中途改变磁盘状态时返回同一结果（§5.2 的回归锁）。
3. §5.1 三处常量一致。实现时先确认 `fe-connector-trino` 的测试 classpath 上是否有 fe-common：**有**则加一个断言 `TrinoBootstrap` 的默认值字面量与 `Config.trino_connector_plugin_dir` 一致的测试；**没有**（该模块以零 fe-core 依赖为目标，fe-common 亦可能不在链上）则退回三处注释交叉指认，不硬造依赖来测。BE 侧跨进程无法断言，一律靠注释。
4. `build.sh --fe --be` 后 `output/{fe,be}/plugins/` 下有 `trino_plugins/`、无 `connectors/`。
