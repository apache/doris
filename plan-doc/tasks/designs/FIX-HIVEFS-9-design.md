# FIX-HIVEFS-9 — engine `DFSFileSystem` conf must pin its own classloader (RpcEngine split-brain)

> 状态：**DONE**（code `<pending>`）。发现于 HIVEFS-8 e2e（本地 hive 回归 `test_string_dict_filter` q01）。
> 一句话：`HdfsConfigBuilder.build()` 是全树唯一**未钉 conf classLoader** 的 Hadoop-conf 构造点；在连接器插件 TCCL 下惰性构造引擎 FS 时，conf 捕获了连接器 loader → `RPC.getProtocolEngine` 经 `conf.getClass` 从连接器的 hadoop-common 副本加载 `ProtobufRpcEngine2`，与引擎侧 `RpcEngine`（app）撞类 → `ClassCastException`。

## 症状（HEAD 实测）

`select * from test_string_dict_filter_parquet where o_orderstatus = 'F'`（读 hdfs 分区）q01 炸：

```
class org.apache.hadoop.ipc.ProtobufRpcEngine2 cannot be cast to class org.apache.hadoop.ipc.RpcEngine
 (ProtobufRpcEngine2 is in unnamed module of loader ChildFirstClassLoader @5eea5627;
  RpcEngine   is in unnamed module of loader 'app')
```

FE 侧真实栈（`output/fe/log/fe.log`）：

```
PluginDrivenScanNode.onPluginClassLoader   ← 扫描线程 TCCL 钉到 hive 连接器 loader @5eea5627
 → HiveScanPlanProvider.listAndSplitFiles   （fe-connector-hive，插件）
  → HiveFileListingCache.listFromFileSystem
   → DFSFileSystem.list / getHadoopFs        （fe-filesystem-hdfs，引擎 FS）
    → FileSystem.get → createFileSystem → DistributedFileSystem.initialize
     → RPC.getProtocolEngine(RPC.java:226)    ← (RpcEngine) 强转在此
      → ClassCastException
```

## 根因

1. 翻闸后 hive 扫描全程在 `PluginDrivenScanNode.onPluginClassLoader` 内跑，**TCCL = hive 连接器 ChildFirstClassLoader `@5eea5627`**。
2. HIVEFS-4 把列文件改走引擎 `context.getFileSystem(session)` → per-catalog `SpiSwitchingFileSystem` → 首次触碰 hdfs 路径时**惰性**构造 `DFSFileSystem`；其构造器 `this.conf = HdfsConfigBuilder.build(properties)` 里 `new HdfsConfiguration()` **捕获当时的 live TCCL** 作为 conf 自己的 `classLoader` 字段 = `@5eea5627`（连接器 loader）。
3. `HdfsConfigBuilder.build()` **从不** `conf.setClassLoader(...)`（全树唯一漏钉的 conf 构造点）。
4. `FileSystem.get` → `NameNodeProxiesClient` → `RPC.setProtocolEngine(conf, ClientNamenodeProtocolPB, ProtobufRpcEngine2)` 把引擎类**名**写进 conf；`RPC.getProtocolEngine` 再 `conf.getClass(prop, ProtobufRpcEngine2.class)` 按名回取。`Configuration.getClass` 用的是 **conf 自己的 `classLoader` 字段**（= `@5eea5627`），**不是** live TCCL。→ 从连接器的 hadoop-common 副本加载 `ProtobufRpcEngine2`。
5. 而 `RPC`/`RpcEngine` 自身：`DFSFileSystem.getHadoopFs` 已把 TCCL 钉到 `DFSFileSystem.class.getClassLoader()`（fe-filesystem-hdfs 插件 loader），该 loader **只 bundle hadoop-hdfs、hadoop-common 委派回父 `app`**（症状里 `RpcEngine` 落在 `app` 即铁证）。→ `RpcEngine` = app 副本。
6. 连接器副本的 `ProtobufRpcEngine2` 强转 app 副本的 `RpcEngine` → 撞类。

**要点**：`getHadoopFs` 钉 live TCCL 只修好了 `FileSystem.get` 的 **ServiceLoader 发现**（挡 hive-exec 的 NullScanFileSystem），**修不了 conf 缓存的 classLoader 字段**——`Configuration.getClass` 只认后者。这正是 `HmsConfHelper:51-58` 早已记载的同一机理（HMS metastore-client 路径），只是漏在了引擎 FS 这一处。

> ⚠ 更正 HANDOFF「已探明去风险事实」第 34 行的旧论断「TCCL 无需连接器侧处理…任意 TCCL 调用皆安全」——**该论断错**：`getHadoopFs` 的 TCCL 自钉不足以覆盖 conf 缓存 CL，须在 `build()` 显式钉。

## 修法（surgical，对齐既有 6 处约定）

`HdfsConfigBuilder.build()`：`new HdfsConfiguration()` 后立即
```java
conf.setClassLoader(HdfsConfigBuilder.class.getClassLoader());
```
镜像 `HmsConfHelper.createHiveConf:60` / `PaimonCatalogFactory:257,333` / `IcebergCatalogFactory:641,659,677` / `HiveConnector:639` / `HudiConnector:261`（全树本就是「conf 构造点无条件钉本模块 loader」的约定，`HdfsConfigBuilder` 是唯一漏网）。

**为何钉 `HdfsConfigBuilder.class.getClassLoader()` 正确**：它与 `getHadoopFs` 已钉的 TCCL 同一 loader（fe-filesystem-hdfs 插件 loader）；该 loader 对 hadoop-common 委派回 `app`（症状实证），故 `conf.getClass("...ProtobufRpcEngine2")` 经它解析 = app 副本 = 与 `RpcEngine`（app）同类 → 强转成功。conf 与框架类解析**同源**。

**为何放 `build()` 而非 `getHadoopFs`（scheme 条件钉）**：① 对齐约定（6 处均在 conf 构造点无条件钉）；② conf 是 per-`DFSFileSystem` 单例、`build()` 一次钉定、确定性（不随「先访问哪个 scheme」漂移）；③ 不触碰 `getHadoopFs` 对 non-hdfs 刻意保留调用方 TCCL 的 **ServiceLoader** 逻辑（本改只动 `conf.getClass` 的解析源，与 ServiceLoader/TCCL 正交）；④ bug 非 hdfs 独有——任何 scheme 走 Hadoop 反射类加载都该从本模块 loader 解析。

## 备选与否决

- **getHadoopFs 内按 `needPluginCL` 条件钉 conf**：更「外科」但把逻辑劈两处、conf.classLoader 随访问顺序漂移、且 conf 是共享单例并不能真正 scheme 隔离 → 否。
- **钉 `Configuration.class.getClassLoader()`**：语义也对（直接绑 hadoop-common loader），但偏离全树「钉本类 loader」约定 → 从约定，否。

## Scope / 风险

- **scope = 引擎 fe-filesystem-hdfs 一处**；hive/paimon/iceberg/hudi/mc 连接器均不动。
- non-hdfs（viewfs/ofs/jfs/oss）：本改把 `conf.getClass` 解析从「捕获的任意调用方 TCCL」改为「本插件 loader（→app 委派）」——是**严格改善**（DFSFileSystem 服务的 scheme，其 driver 本就在本插件/ app，不在任意连接器 loader）；ServiceLoader 发现路径不受影响（`getHadoopFs` TCCL 逻辑原样保留）。
- Kerberos/UGI 用同一 conf：一并落到 app 的 hadoop-common（本就应如此），无回归。

## 验证

- `HdfsConfigBuilderTest.buildPinsPluginClassLoaderNotTccl`（新增，镜像 `PaimonCatalogFactoryTest.assembleHiveConfPinsPluginClassLoaderNotTccl`）：装一个 foreign `URLClassLoader` 当 TCCL → `build()` → 断言 `conf.getClassLoader()` == 本插件 loader、≠ foreign。
  - **RED 实证**（抽掉 `setClassLoader`）：`expected <AppClassLoader> but was <URLClassLoader@…>` @ :88。
  - **GREEN**：`fe-filesystem-hdfs` **9/9**、`BUILD SUCCESS`、**0 checkstyle**。
- ⚠ 单测环境是平铺 classpath，`HdfsConfigBuilder.class.getClassLoader()` = AppClassLoader；真跨 loader 强转只在真 child-first 插件环境复现（e2e）。

## 残余 / e2e（用户自跑，勿丢）

- 重打包 + 重部署后：`external_table_p0/hive/test_string_dict_filter` q01 应越过本 ClassCast（读 hdfs 分区）；本 fix 只解**这一 classloader 阻断**，其后各断言/写套件仍按 HIVEFS-8 e2e 矩阵回归。
- 本 fix 属 HIVEFS-8 收尾的一部分（引擎 FS 在连接器 TCCL 下的正确性补漏）。
