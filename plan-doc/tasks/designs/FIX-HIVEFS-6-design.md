# FIX-HIVEFS-6 — 连接器写路径改经引擎注入 `FileSystem`（去 `HiveConnectorTransaction` 的本地 ServiceLoader 造 FS）

> 承接 FIX-HIVEFS（设计 `FIX-HIVEFS-design.md`、清单 `task-list-HIVEFS.md`）。HIVEFS-3（引擎实现）+ HIVEFS-4（读扫描列文件）+ HIVEFS-5（ACID 目录下降）已入库；本步是**最后一条、也是最险的连接器路径**——非-ACID 写事务（staging→target rename / delete / MPU complete·abort）仍用**本地 `ServiceLoader` 造 FileSystem**，是同一"hive 插件类加载器隔离→跨插件拿不到 FS 实现"缺口的写侧分支。
> 范围 = 仅 hive 连接器写路径（`HiveConnectorTransaction`）。去 jar（HIVEFS-7）、全量 e2e（HIVEFS-8）不属本步。

## Problem

`HiveConnectorTransaction`（P7.3 移植自老 fe-core `HMSTransaction`，commit `eeae58bbd23`，此后未再动）的所有写侧文件 I/O **已经**走 Doris `org.apache.doris.filesystem.FileSystem` API（`exists`/`mkdirs`/`delete`/`listFiles*`/`listDirectories`/`renameDirectory`/`FileSystemUtil.asyncRename*` + MPU `ObjFileSystem.completeMultipartUpload`/`getObjStorage().abortMultipartUpload`），**统一经私有 `getFileSystem()`（:755）取 FS**。缺陷**只在 `getFileSystem()` 的 FS 来源**：

```java
// :755 getFileSystem()  懒建 + 字段缓存
StorageProperties objSp = context.getStorageProperties().stream()
        .filter(sp -> sp.kind() == StorageKind.OBJECT_STORAGE)   // ← 只挑对象存储
        .findFirst().orElse(null);
local = resolveObjectStoreFileSystem(objSp);                      // :766

// :784 resolveObjectStoreFileSystem
for (FileSystemProvider provider : ServiceLoader.load(FileSystemProvider.class)) {  // ← 本地 ServiceLoader
    if (provider.supports(objSp.rawProperties())) return provider.create(objSp.rawProperties());
}
throw new DorisConnectorException("No FileSystemProvider supports ...");
```

两处致命：
1. **本地 `ServiceLoader.load(FileSystemProvider.class)` 跨插件拿不到 provider**——FS provider 在**兄弟** filesystem 插件 loader（`output/fe/plugins/filesystem/*`），共享类路径（`output/fe/lib`）无任何 provider，`DirectoryPluginRuntimeManager` 且刻意屏蔽父级 service 描述符。该方法 javadoc 自认是 "cutover-time concern / production ServiceLoader discovery … swappable"（即从来是占位 stub）。
2. **`.filter(OBJECT_STORAGE)`**——只挑对象存储 `StorageProperties`。**HDFS 后端 hive 表** `objSp==null` → `resolveObjectStoreFileSystem(null)` 抛 `"No object-store StorageProperties available for hive write"`。

**动态状态（与 HIVEFS-5 planAcidScan 同型的陈旧注释）**：class-javadoc（:101-103）称 `"Gate-closed / dormant: hive is not in SPI_READY_TYPES, so nothing routes plugin-driven hive writes through this class until the P7.4/P7.5 cutover"` —— **已假**。Phase 2 原子翻闸（commit `fb1624be757`，`hms`∈`SPI_READY_TYPES`）后，`type=hms` 表 INSERT 经 `HiveWritePlanProvider.planWrite` → `beginWrite` → 本类 live 驱动。故这是**修 live 生产 bug**（HDFS hive INSERT 今天必炸 stub、对象存储 hive INSERT 今天必炸跨插件 ServiceLoader），非休眠整洁。本步须一并订正 class-javadoc（否则 ship 自相矛盾的文件）。

老 fe-core `HMSTransaction` 用 `SpiSwitchingFileSystem`（引擎侧、全 scheme）做同样的 rename/delete/MPU；P7.3 移植时换成本地 ServiceLoader 是迁移走样，与 HIVEFS-4/5 读侧同源。

## Root Cause

引擎已经 HIVEFS-3 提供 `ConnectorContext.getFileSystem(session)`（per-catalog `SpiSwitchingFileSystem`，引擎所有、连接器借用不 close、全 scheme 路由），HIVEFS-4/5 已把读扫描 + ACID 接上；写路径是最后一条未接的分支。本步把 `getFileSystem()` 的 FS **来源**从"本地 ServiceLoader 造对象存储 FS"换成"借引擎 `context.getFileSystem(session)`"——**下游 20 个调用点全不动**（它们已用 Doris `FileSystem` API），唯 MPU 两处需从 switching-facade 解出**具体** `ObjFileSystem`。

## Design

### 1. 会话捕获（`beginWrite` 时）
`context.getFileSystem(session)` 需 `ConnectorSession`。`beginWrite(ConnectorSession session, …)`（:207）是唯一预提交入口、已捕获 user。新增字段 `private ConnectorSession session;`，`beginWrite` 首行 `this.session = session;`。`ConnectorSession` 已 import（:25）。
- **session 可为 null 的两条路径**（均安全）：① 单测 `beginWrite(null, …)`；② rollback-before-commit（`rollback()`:292，`hmsCommitter==null`，仅 abort 悬挂 MPU）——生产中 `beginWrite` 恒先于 `addCommitData/commit/rollback`（`planWrite` 在 BE 执行前调），故此路径 session 实为已捕获；纯 rollback（无 beginWrite）仅测试构造。引擎 `DefaultConnectorContext.getFileSystem`（HIVEFS-3）**忽略 session**（catalog 级），null 安全；契约标注 identity 经 `session.getUser()` **预留** per-user。

### 2. `getFileSystem()` 换来源（:755-776）
删懒建 + `fs` 字段 + `resolveObjectStoreFileSystem` + OBJECT_STORAGE 过滤，改为借引擎 FS：
```java
private FileSystem getFileSystem() {
    FileSystem engineFs = context.getFileSystem(session);
    if (engineFs == null) {                                    // loud（对齐 HIVEFS-4/5 null-FS→loud）
        throw new DorisConnectorException("No engine FileSystem available for hive write transaction "
                + transactionId + " (catalog has no storage properties)");
    }
    return engineFs;
}
```
- 引擎已 per-catalog 懒建 + 缓存 `SpiSwitchingFileSystem`（HIVEFS-3），故连接器侧**无需**再本地缓存 `fs` 字段（删之，连带删 :119-121 注释）。
- 空 storage → 引擎返 null → 本处 loud（HDFS/对象存储 hive 表都有 storage，仅无存储的畸形 catalog 命中，即时可诊断优于 NPE）。

### 3. MPU 两处解具体 `ObjFileSystem`（`forLocation`）
`getFileSystem()` 现返 `SpiSwitchingFileSystem`（facade，**非** `ObjFileSystem`）。非-MPU 的 19 个调用点用**基类** `FileSystem` 方法（`exists`/`delete`/`renameDirectory`/`listFiles*`…），switching FS 内部**逐操作** `forLocation` 路由到具体 FS（`SpiSwitchingFileSystem:116-160`）→ **不动**。唯 MPU 两处要 narrow 到 `ObjFileSystem`（`completeMultipartUpload`/`getObjStorage()` 不在基类接口上），须先 `forLocation` 解出具体 FS：

- **`objCommit`（:803-833，complete，strict）**：把 :808 `FileSystem resolved = getFileSystem();` 换为
  ```java
  FileSystem resolved;
  try {
      resolved = getFileSystem().forLocation(Location.of(path));   // path=写目标(native scheme)
  } catch (Exception e) {                                           // 宽 catch：forLocation 可抛 StoragePropertiesException(RuntimeException)
      throw new DorisConnectorException("Failed to resolve object-store filesystem for MPU commit at "
              + path + ": " + e.getMessage(), e);
  }
  if (!(resolved instanceof ObjFileSystem)) { throw …(:809-813 不变); }
  ObjFileSystem objFs = (ObjFileSystem) resolved;                   // 循环体不变
  ```
  `path` 是该分区/表的写目标（**native HMS scheme**：S3 兼容=`s3://`、Azure=`abfss://`；MPU 只在 `hasPendingUploads` 且对象存储写时触发）；`type`-match 解析对所有后端（含 Azure）成立，与循环内 `remotePath="s3://bucket/key"`（传 complete API 的 BE 统一口径）同 catalog 凭证。fail-fast 语义（非对象存储即抛）保留；catch `Exception`（非 `IOException`）兜住 `forLocation` 的 `StoragePropertiesException`（RuntimeException）→ loud-wrap。
- **`abortMultiUploads`（:1330-1356，abort，lenient）**：把 :1334 循环外 `getFileSystem()` 移入**循环内逐 upload** 解析 FS，**用 upload 的 native-scheme 路径 `u.path`**（=`pu.getLocation().getWritePath()`，:465——非合成 `s3://`；与 objCommit 用 `path` 对称）`forLocation(Location.of(u.path))`，`forLocation`/instanceof 失败 → `LOG.warn`+`continue`（对齐现有 :1336-1339 warn+skip 的 abort 宽松语义，不抛）：
  ```java
  for (UncompletedMpuPendingUpload u : uncompletedMpuPendingUploads) {
      TS3MPUPendingUpload mpu = u.s3MPUPendingUpload;
      String remotePath = "s3://" + mpu.getBucket() + "/" + mpu.getKey();   // BE-unified s3:// 路径（传 abort API）
      FileSystem resolved;
      try { resolved = getFileSystem().forLocation(Location.of(u.path)); }   // ← native-scheme 解析（红队 major）
      catch (Exception e) { LOG.warn("… skip MPU abort for {}: {}", remotePath, e.getMessage()); continue; }
      if (!(resolved instanceof ObjFileSystem)) { LOG.warn(…); continue; }
      ObjFileSystem objFs = (ObjFileSystem) resolved;
      … 异步 abort（不变，仍 `objFs.getObjStorage().abortMultipartUpload(remotePath, …)` 在 context.executeAuthenticated 内）
  }
  uncompletedMpuPendingUploads.clear();
  ```
  - **红队 major 折入**：① 解析源用 `u.path`（native scheme：Azure=`abfss://`、S3 兼容=`s3://`）而非合成 `"s3://"+bucket+"/"+key`——否则 Azure-typed catalog（`getStorageName()="AZURE"≠"s3"`）的 `s3://` 无法解析。② catch **`Exception`**（非 `IOException`）——`SpiSwitchingFileSystem.forLocation` props 解析失败抛 `StoragePropertiesException`（**RuntimeException**，非 IOException），窄 catch 无法兑现 abort 的 "resolve 失败→warn+skip" 宽松契约，会逃逸破坏 rollback、泄漏 server 端 MPU（vs HEAD 直建 Azure FS 成功 abort 的回归）。objCommit 同步 catch `Exception`→loud-wrap（strict）。`remotePath`（`s3://bucket/key`，BE 统一口径）仍传 `abortMultipartUpload` API 不变。

### 4. `close()` 不关借用 FS（:188-199）
现 `close()` 关 `fileSystemExecutor`（自有，保留）**并 `fs.close()`**（:190-198，借用引擎 FS，**须删**）。删除 `fs` 字段后 `close()` 仅剩 executor 关闭：
```java
public void close() { shutdownExecutorService(fileSystemExecutor); }
```
引擎 FS 生命周期由 catalog 在 `onClose`/换连接器处关（HIVEFS-3 定），连接器只借。

### 5. class-javadoc 订正（:87-104）
- :96-98 D6 描述 `"object-store multipart uploads via a plugin-side fe-filesystem-spi ObjFileSystem built from context.getStorageProperties() instead of SpiSwitchingFileSystem"` → 改为如实：经 `context.getFileSystem(session)`（引擎 `SpiSwitchingFileSystem`，全 scheme）借用，MPU 经 `forLocation` 解具体 `ObjFileSystem`。
- :101-103 `"Gate-closed / dormant …"` → 翻闸后 live（`type=hms` INSERT 经 `HiveWritePlanProvider.planWrite`→`beginWrite` 直达本类）。

### 6. import 增删
- **删**：`java.util.ServiceLoader`（:73）、`org.apache.doris.filesystem.properties.StorageKind`（:40）、`org.apache.doris.filesystem.properties.StorageProperties`（:41）、`org.apache.doris.filesystem.spi.FileSystemProvider`（:42）——已核实仅 `getFileSystem`/`resolveObjectStoreFileSystem` 用（class-doc 引用随 §5 订正一并去 `{@link}`）。
- **保留**：`org.apache.doris.filesystem.spi.ObjFileSystem`（:43，MPU 仍用）、`FileSystem`/`Location`/`FileEntry`/`FileSystemUtil`、`org.apache.hadoop.fs.Path`（:53，`new Path(...)` 纯路径拼接如 :878/1166/1232/1500，非 FS I/O，保留）。
- **无新增**（`ConnectorSession`/`DorisConnectorException` 已 import）。

## 关键正确性核验（写路径独有，读侧 HIVEFS-4/5 无此需求）

**`instanceof ObjFileSystem` 跨 连接器↔filesystem 插件 classloader 边界成立**（本步最大风险，已逐点取证）：
- `ObjFileSystem` 在 `fe-filesystem/fe-filesystem-spi`（共享 SPI 模块）；`S3CompatibleFileSystem extends ObjFileSystem`，s3 插件具体 FS 是其子类。
- `fe-connector-hive` 依赖 `fe-filesystem-spi` = **`provided`**（pom :97）→ **不打入插件 lib/**，运行时从 fe-core 宿主共享类路径解析。
- `fe-filesystem-s3` 依赖 `fe-filesystem-spi` 亦 `provided`（pom 注释 :42-43：`"SPI/API … live on the fe-core host classpath and are loaded parent-first; provided so they are not bundled into the plugin lib/"`）。
- `FileSystemPluginManager:72` parent-first 前缀含 `"org.apache.doris.filesystem."` → filesystem 插件对 `org.apache.doris.filesystem.spi.ObjFileSystem` **parent-first** 解析。
- ⇒ 连接器与 s3 插件解析**同一** `ObjFileSystem` Class 对象 → `resolved instanceof ObjFileSystem` 真、`(ObjFileSystem) resolved` 不 ClassCast。生产 `SpiSwitchingFileSystem.forLocation("s3://…")` → 具体 `S3CompatibleFileSystem` 子类（is-a `ObjFileSystem`）。

**`forLocation` 默认语义**：`ObjFileSystem` **不** override `forLocation` → 用 `FileSystem` 接口默认 `return this`（HIVEFS-1 加）。故：① 生产具体 S3 FS `forLocation(x)`=自身（already 具体）；② 单测 fake `RecordingObjFileSystem`（extends `ObjFileSystem`）`forLocation(x)`=自身 → instanceof 过。switching FS 的 `forLocation` override 才做 per-scheme 路由（生产入口）。

**TCCL 无新增钉点**：MPU complete/abort 仍在 `context.executeAuthenticated(...)`（:822/1346）内 → `TcclPinningConnectorContext` 已钉插件 loader（memory `catalog-spi-plugin-tccl-classloader-gotcha` 既有 locus）；`forLocation` 建 S3 FS 走 AWS SDK（非 Hadoop `getHadoopFs` 的 "No FileSystem for scheme" 类），且 S3 provider 由引擎 `FileSystemFactory` 造，无连接器侧 TCCL 责任。

## Implementation Plan
1. `HiveConnectorTransaction.java`：字段（+`session`、−`fs`）、`beginWrite` 捕获 session、`getFileSystem()` 换来源、删 `resolveObjectStoreFileSystem`、MPU 两处 `forLocation`、`close()` 去 `fs.close()`、class-javadoc 订正、import 增删（§1-6）。
2. `HiveConnectorTransactionTest.java`：注入缝从 `resolveObjectStoreFileSystem`-override 迁到 `FakeConnectorContext.getFileSystem(session)`-override（返回 fake `ObjFileSystem`）。
3. build（`-pl :fe-connector-hive -am`）+ 靶向 UT（`-Dtest=HiveConnectorTransactionTest`）+ 0 checkstyle + import 门净。

## Risk Analysis
- **`instanceof ObjFileSystem` 跨边界**（最险）→ 已逐点取证成立（见上「关键正确性核验」），provided-scope + parent-first 双证。**e2e 兜底**：真集群对象存储 hive INSERT（MPU complete）+ rollback（MPU abort）。
- **误 close 借用 FS**：§4 删 `fs.close()`；引擎所有、catalog 关。
- **session=null**：引擎/fake 均忽略；契约标注 per-user 预留。
- **`forLocation` × switching 缓存**：`SpiSwitchingFileSystem.forLocation`→`forPath`→按 `StorageProperties` 值相等缓存（`:66/94`）；同 catalog 对象存储凭证唯一 → MPU 各 upload 命中同一具体 FS，无重复建连。
- **rename/delete 语义不变**：19 个非-MPU 点未改一字，仅 FS 实例从"本地 ServiceLoader 造的具体对象存储 FS"变"引擎 switching facade"；facade 逐操作 `forLocation` 委派到**同一类**具体 FS（对象存储场景），字节等价；且新增 HDFS/其它 scheme 支持（架构红利，老 stub 直接抛）。
- **fail 语义**：complete strict（resolve 失败 loud）；abort lenient（warn+skip）——均对齐现有 objCommit/abortMultiUploads 既定语义。
- **铁律核对**：`HiveConnectorTransaction` 不解析属性（借注入 FS）；不新增 fe-core 分支；无源判别式；TCCL 无新增 locus。

## Test Plan

### Unit（连接器模块无 Mockito；真 recording fake）
现有 `HiveConnectorTransactionTest`（12 用例）注入 fake FS 靠 override `resolveObjectStoreFileSystem`（:165-173 `newTxnWithFs`）。本步删该方法 → 注入缝迁到 `FakeConnectorContext.getFileSystem(session)`，**返回一个 non-`ObjFileSystem` 的路由 facade**（镜像生产 `SpiSwitchingFileSystem`）：
- 新增测试内 `RoutingFacadeFileSystem implements FileSystem`（**不 extends `ObjFileSystem`**）：`forLocation(loc)` 返回被包裹的 `RecordingObjFileSystem`；base 方法（exists/mkdirs/delete/rename/list/newInputFile/newOutputFile）委派该 delegate；`close()` 抛 `AssertionError`（借用契约守卫）。
- `newTxnWithFs` 用**匿名 `FakeConnectorContext` 子类**（override `getFileSystem(ConnectorSession)` 返回 `new RoutingFacadeFileSystem(RecordingObjFileSystem)`，忽略 session）。
- **为何 facade 而非直接返回 `RecordingObjFileSystem`（红队 minor-2）**：直接返回则 fake **本身**是 `ObjFileSystem`，`instanceof` 恒过——测不出"漏调 `forLocation`"的回归（生产 facade 非 ObjFileSystem，漏调即 instanceof 失败/throw）。facade 强制走 `forLocation` narrow：漏调 → `getFileSystem() instanceof ObjFileSystem` 对 facade 为 false → 3 MPU 用例 RED。
- **借用守卫（红队 minor-4，改必做非可选）**：facade `close()` 抛 `AssertionError`；现有 3 MPU 用例已 `txn.close()`——若 §4 漏删 `fs.close()`（或回归重加 close 借用 FS）→ RED。
- **3 个 MPU 用例断言不变**：`testCommitCompletesMultipartUploads`（complete 一次、ETag 按 part 排序）、`testRollbackAbortsPendingMultipartUploads`（abort 一次，rollback-before-commit 走 session=null 路径 → **顺带证 null-session 安全**）、`testSecondRollbackIsIdempotent`（幂等 abort 一次）。
- 其余 9 用例（分类/NEW→APPEND/事务表拒/`getUpdateCnt`/`profileLabel`/dup-partition/addPartitions-once）不碰 FS 或走 FILE_S3 write==target 无 rename → 不受影响；`beginWrite(null,…)` 现已传 null，迁移后仍 null（引擎/fake 忽略）。
- **import bookkeeping（红队 minor-1，必做否则编译/checkstyle 挂）**：`HiveConnectorTransactionTest` 加 `import org.apache.doris.connector.api.ConnectorSession;` + `import java.io.IOException;`（facade 委派方法声明）；删 `import org.apache.doris.filesystem.properties.StorageProperties;`（仅被删除的 `resolveObjectStoreFileSystem` override 用）。
- **类 javadoc 订正（红队 minor-5）**：:85 "injected via the `resolveObjectStoreFileSystem` seam" → 改指 `FakeConnectorContext.getFileSystem(session)` 缝。

## 红队结论（wf_8fd372d6-10d，GO_WITH_FIXES）
5 lens 并行（classloader-cast / semantic-equivalence / lifecycle-session-concurrency / forlocation-mpu-fidelity / test-fidelity-completeness）+ 1 独立裁决者复核 severe 声明。**classloader/instanceof、session 捕获、null 安全、close 去除、19 非-MPU 点字节等价 均独立复现 SOUND → 无 blocker**。折入：1 major（MPU abort native-path 解析 + 两处 catch 拓宽 `Exception`，见 §3）+ 4 minor（test import / non-ObjFileSystem facade / close 守卫必做 / 类 javadoc）。裁决者纠正 lens-2 对 Azure 失败的"静默 skip"误判为"未捕获 RuntimeException 破坏 rollback"（同缺陷、正确机制）。

### E2E（用户自跑，勿丢——`hms-iceberg-delegation-needs-e2e`）
翻闸后写路径 live 且当前必炸（HDFS stub / 对象存储跨插件 ServiceLoader）——本 e2e 验 live 生产 bug 修复，非"休眠不可测"：
- `external_table_p0/hive` 含 INSERT 的写套件（rename/delete + MPU complete）：HDFS 后端（验 stub→引擎 FS 转换 + 新 scheme 支持）+ 若有对象存储环境验 s3/oss MPU。
- INSERT 失败回滚（验 MPU abort 不泄漏 server 端 upload）。
- 断言与老 fe-core `HMSTransaction` 逐位一致（落盘、事务提交/回滚、分区 add、统计更新）。

## Open / 已决
- **测试注入缝**（清单 Open #4 的伴生项）**已决**：迁到 `FakeConnectorContext.getFileSystem`-override（非保留 `resolveObjectStoreFileSystem` 死方法）——注入点对齐生产真实来源（引擎给 FS），保真度更高。
- **FS/identity 捕获时机**（清单 Open #4）**已决**：`beginWrite` 捕获 session；FS 解析惰性（引擎 per-catalog 缓存，等价 begin 时建）。
- per-user identity 真正落地 → Future（清单 Future §），不属本步。
