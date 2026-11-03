# ConnectContext 生命周期与场景问题映射

核查日期：2026-11-03。源码基线：`015dbb6b0b0d02b5352b62a72c278fde975d0613`。

`ConnectContext` 的初始化、连接池登记、线程绑定、语句资源释放和会话结束是不同操作。修复应沿用现有 `ConnectContext / ConnectScheduler / ConnectPoolMgr`，使创建方、使用方和清理责任方配对，不新增平行的会话生命周期对象，也不依赖 GC 执行业务清理。

本文覆盖客户端连接、协议请求、跨 FE 转发、内部作用域、后台任务和 HTTP 入口的代表性调用链；不是所有 `new ConnectContext()` 调用点的穷尽审计。问题分为“已确认调用链缺陷”和“待验证的生命周期契约”，后者不等同于已证明的资源泄漏。

旧临时表的 `Env.registerSessionInfo / unregisterSessionInfo / aliveSessionSet` 登记机制及代理构造中的历史 sessionId 处理不列为问题，也不以恢复该机制为修复目标。当前 `registerSessionInfo()` 是停用的空实现。

## 1. 操作边界与持有关系

| 操作 | 当前职责 | 不应据此推断 |
| --- | --- | --- |
| 构造函数、`init()` | 创建协议适配器，初始化 QueryState、会话变量、用户变量、命令状态等 | 创建对象不等于进入客户端连接池 |
| `ConnectScheduler.submit()` | 分配 connectionId、重置登录时间 | 不执行连接池注册，也不表示异步任务已提交 |
| `ConnectPoolMgr.registerConnection()` | 检查配额、增加计数、放入 connectionMap；Flight 还建立 peer identity 索引 | 注册成功不表示后续响应和初始化一定成功 |
| `setThreadLocalInfo()` | 将 Context 绑定到当前线程 | 不是生命周期登记，也不转移资源所有权 |
| `ConnectContext.remove()` | 移除当前线程绑定 | 不关闭连接、不注销连接池、不释放全部资源 |
| `clear()` | 清空 executor、statementContext 等引用，调用协议的 afterStatement | 不是会话 cleanup；置空 StatementContext 不等于调用它的 close |
| `StatementContext.close()`、Query 注销 | 释放语句级资源或查询登记 | 不等于 ConnectContext 生命周期结束 |
| `resetConnection()` | 重置现有连接上的会话状态 | 保留连接和连接池登记，不应当做断连处理 |
| `cleanup()` | 关闭协议连接、清除调用线程绑定，并执行现有会话清理步骤 | MySQL 的连接池注销仍依赖 Channel 关闭回调 |
| `unregisterConnection()` | 释放协议会话资源、回滚会话事务、删除连接池及关联索引、扣减计数 | 各协议抵达该方法的路径不同 |

证据：[ConnectContext][ctx-init]、[submit][submit]、[连接池登记及注销][pool]、[clear][clear]、[resetConnection][reset]。

主要强引用关系：

- 客户端长会话由 `connectionMap` 持有；MySQL 还有 Channel/Listener 引用，Flight 通过 peer identity 查回同一 Context。
- 当前执行线程通过 `MoreFieldsThread` 字段或普通 ThreadLocal 持有 Context。线程池工作线程不会因为一次任务结束而自动清除该引用。[线程绑定实现][thread]
- 内部 Context 可能由局部变量、任务字段、Executor 或 Coordinator 相关对象持有，需要根据实际任务完成边界释放。
- `proxyQueryIdToConnCtx` 是转发查询取消定位索引，不是来源客户端会话的所有权登记。

对象 GC 只能回收不可达的 Java 对象，不能替代连接注销、事务处理、Query 注销或显式关闭。

## 2. 场景总览

| 场景 | 创建及持有者 | 使用边界 | 当前结束路径 | 对应问题 |
| --- | --- | --- | --- | --- |
| S01 MySQL 建连和初始化 | AcceptListener 创建；注册后连接池持有 | 接入、握手、配额登记、安装读回调 | 失败时 cleanup；注册后通常靠关闭回调注销 | P01、P02 |
| S02 MySQL 多条语句、断连、KILL、超时 | 复用连接 Context | 每次 ReadListener 工作任务绑定线程 | 正常语句 clear/remove；连接结束 cleanup 或 killConnection | R03 |
| S03 COM_RESET_CONNECTION | 原客户端连接持有 | 重置状态后继续使用 | resetConnection；不注销连接 | 保持现有重置语义 |
| S04 Flight 逻辑会话和命令 | openSession 创建；连接池和 token 索引定位 | 多个 RPC 复用同一 Context | CloseSession、KILL、空闲超时触发协议注销 | R03、R04 |
| S05 Flight 延迟取结果 | 原 Flight 会话持有 deferred executor | GetFlightInfo 返回后资源可能仍被 DoGet 使用 | 下一次查询、会话关闭或 deferred 超时回收 | 不应在单次 RPC 返回时 cleanup |
| S06 跨 FE 转发 | FrontendServiceImpl 创建代理 Context | 一次转发执行及取消定位 | StatementContext.close；finally 解绑线程并移除代理索引 | R02 |
| S07 内部同步作用域 | 统计、导出等工厂创建；包装器持有 | 一次内部 SQL 或局部操作 | AutoCloseConnectContext.clear/remove/恢复外层绑定 | R01 |
| S08 后台任务 | 任务创建或借用 Context | 作业任务、重试或加载执行 | Job 框架与 LoadTask 框架不同 | P03、R01 |
| S09 HTTP 请求及 Web SQL | 普通请求临时创建；Web SQL 另持有 JDBC 连接 | HTTP 请求或跨请求 SQL 会话 | 部分请求显式 remove；JDBC 会话由管理器关闭 | P04、R05 |
| S10 回放解析、Dummy Context、变量作用域 | helper 创建或借用已有 Context | 解析/校验/变量覆盖作用域 | 部分 finally cleanup，部分只 remove 或无收尾 | P05、R01、R05 |
| S11 FE 异常退出 | 进程内全部 Context | 进程终止 | 不能保证 Java cleanup 执行 | 集群侧资源回收边界 |

## 3. 各场景生命周期

### S01 MySQL 建连和初始化

当前调用链：

```text
AcceptListener.handleEvent
  accept → forMysql（构造及 init）→ setEnv → scheduler.submit
  → worker.execute(handleConnection)

handleConnection
  绑定线程 → 设置 scheduler → negotiate
  → registerConnection
  → 发送成功响应
  → 安装 closeListener(unregisterConnection)
  → 设置 startTime、用户超时
  → startAcceptQuery
  finally：ConnectContext.remove
```

握手或配额失败时，`handleConnection` 的 catch 会调用 cleanup。接入任务提交失败另有拒绝分支，该分支不是 `handleConnection` 的异常出口。P01、P02 分别对应这两个阶段。[AcceptListener][accept]

责任要求：接入方在连接交给正常读事件处理前，负责所有部分初始化失败的清理；注册成功之后必须存在不依赖网络回调安装结果的注销路径。

### S02 MySQL 语句、断连、KILL 和超时

`ReadListener` 暂停读事件，将命令交给工作线程，工作线程绑定已有 Context。正常语句结束后 `MysqlConnectProcessor.processOnce()` 设置 COM_SLEEP 并 clear；Listener 恢复读事件，finally 移除线程绑定。Context 继续由连接池持有。[ReadListener][read] [processOnce][mysql-process]

COM_QUIT 设置 killed 标志；网络 EOF、异常和命令处理异常也会进入连接结束路径。Listener 调用 cleanup；已安装的 Channel closeListener 负责注销连接池。已连接阶段的工作任务拒绝分支使用 try/finally 移除线程绑定，不能与 S01 的接入拒绝分支混为一谈。

KILL CONNECTION 或空闲超时可以从其他线程调用 `killConnection()`；运行中的读任务随后也可能观察到关闭并进入 cleanup。KILL QUERY 和执行超时不必结束连接。R03 对应这些并发入口，而不是普通单条 SQL 的 clear。

### S03 连接重置

`handleResetConnection()` 调用 `resetConnection()`，后者处理事务、现有会话资源、变量和 Prepared Statement 等状态，原连接继续使用。[重置入口][reset-entry]

修复 cleanup 时不能把 reset 改成注销并重建客户端连接；重置失败时也不能直接声称完整重置成功。本文不重新评审旧临时表重置实现。

### S04 Flight 逻辑会话

```text
openSession
  buildConnectContext → forFlight/init
  → 设置 env、身份、startTime、scheduler
  → scheduler.submit → registerConnection
  → 返回 token

命令
  token 查找 Context
  → callCommand 获取命令锁
  → 检查协议 closed 状态、刷新活动时间、绑定线程
  → 执行命令
  → finally 恢复原线程绑定并释放命令锁

CloseSession
  cleanup
  → FlightProtocolAdapter.closeConnection
  → unregisterConnection
  → releaseSession、closeTxn、移除池条目和 token 索引
```

Flight 的逻辑会话不能按一次传输连接或一次 RPC 的结束判断死亡。当前命令锁与 closed 状态已经处理命令串行化及关闭后的拒绝，不应重复添加另一套会话注册对象。[会话入口][flight-session] [命令作用域][flight-command]

配额拒绝分支显式调用 releaseProtocolSession，不能说完全没有清理。R04 关注的是工厂构建及注册前其他异常的资源交接，而不是否定现有配额分支。

### S05 Flight 延迟执行资源

`FlightSqlConnectProcessor.close()` 只 finalize 非 deferred executor，然后 clear/remove；它不是关闭 Flight 会话。deferred executor 被留在会话协议适配器中，供后续 DoGet 使用，并在下一次查询、会话 teardown 或空闲扫描中结束。[Processor.close][flight-close]

会话关闭与命令可能并发。现有 `tearDown` 会关闭 deferred 集合，并处理关闭后才加入的 executor。这是必须保留的异步资源约束，不能把 processor 的 close 简单替换成 Context.cleanup。[协议释放][flight-release]

### S06 跨 FE 转发

`FrontendServiceImpl.forward()` 创建代理 Context，构造 Processor，并按 queryId 登记取消定位；finally 执行线程解绑和代理索引移除。`ConnectProcessor.proxyExecute()` 的 finally 已关闭对应 StatementContext。[forward][forward] [语句释放][proxy-stmt]

来源客户端仍由接入 FE 的原 Context 持有。代理不是另一个客户端会话，不能因转发结束注销来源会话。R02 是代理本地资源退出契约的待核查项；当前没有依据仅凭“没有调用 cleanup”就判定该路径泄漏。

### S07 内部同步作用域

统计查询、导出等创建 Dummy 协议的内部 Context，并使用 `AutoCloseConnectContext` 绑定线程。该包装器退出时执行 clear、remove，并恢复 previousContext，没有执行完整 cleanup。[包装器][auto-close] [统计工厂][stats] [导出调用][export]

需要分别明确：新建 Context 的拥有者是否应在退出时完整关闭；借用已有 Context 的调用点是否只应临时绑定。R01 记录这项契约差异，不推导“每次统计查询都泄漏外部资源”。

### S08 后台任务和加载任务

两套任务框架不能统一概括为“后台任务会自动清理”：

- `InsertTask.before()` 创建并绑定 Context；任务自身的 closeOrReleaseResources 主要置空字段。外层 `TaskProcessor.closeTaskContext()` 会注销 Query、关闭 StatementContext，并在 finally remove。已覆盖线程解绑，但没有完整 cleanup。[InsertTask][insert-task] [TaskProcessor][task-processor]
- `LoadLoadingTask.executeOnce()` 在当前线程无 Context 时新建并绑定；已有 Context 时直接复用。它的 finally 注销 Query，没有解除新建 Context 的线程绑定。`LoadTask.exec()` 和 `MasterTask.run()` 也未做该解绑。P03 对应这条路径。[加载执行][load-once] [LoadTask][load-task] [MasterTask][master-task]

后台任务的 Context 必须存活到实际执行结束，而不是提交方法返回。提交失败、取消、重试、完成都应明确由谁关闭；不能关闭从调用者借来的 Context，也不能让一次任务创建的 Context 留给下一次任务。

### S09 HTTP 请求和 Web SQL

普通 HTTP 路径可能用 `new ConnectContext()` 保存当前请求身份并绑定线程，不进入客户端连接池。一个具体路径是 `ShowAction.show_meta_info()` 在启用 HTTP 认证时调用 `executeCheckPassword()`，随后可直接返回响应；该函数没有 finally 解绑。[HTTP 入口][http-show] [请求 Context 创建][rest-context]

已检查的 AuthInterceptor、ServletTraceIterceptor 和 WebConfigurer 没有统一的 Context 请求结束收尾。P04 记录这条已检查同步请求链上的绑定残留；不把所有 HTTP 路径都归为同一问题。

Web SQL 则由 `WebSqlSessionManager` 跨请求保存 JDBC Connection，并在显式关闭、过期或相关会话退出时关闭它。对应 SQL 服务端 Context 按 MySQL 生命周期管理。HTTP 请求使用的身份 Context 与 JDBC 服务端连接 Context 不能混为一体。[Web SQL 会话][websql]

### S10 解析、回放和临时变量作用域

`ConnectContextUtil.getDummyCtx()` 会新建 Context、创建 StatementContext、设置环境和身份并绑定线程；其契约要求调用者 cleanup。[Dummy helper][dummy]

不同调用点的实现不同：

- `RollupJobV2` 使用 finally cleanup，已经有明确退出路径。
- `MaterializedIndexMeta.parseStmt()` 在自行创建 Dummy Context 时通过 tmpCreate 标记决定 cleanup，不能误关借用的 Context。
- `BulkLoadJob.analyze()` 在当前线程无 Context 时调用 getDummyCtx，但没有对应 finally。P05 对应该分支。
- `AutoCloseSessionVariable` 借用已有 Context 时只恢复变量；无 Context 时可以临时创建，close 只 remove。它还包含一个静态哨兵 Context；哨兵不属于需计入客户端连接数的活跃会话。[变量作用域][vars]

嵌套作用域还应恢复外层线程绑定。仅调用当前 cleanup 的无条件 remove，并不自动满足嵌套恢复要求，见 R05。

### S11 FE 异常退出

进程崩溃、强制终止无法保证任何 Context.cleanup 被执行。进程内 Java 对象随进程消失，不等于 BE 或存储层资源已经回收。

修复本地生命周期只能保证仍在运行的 FE 正确维护注册集合。集群侧依然需要依照已登记的资源归属处理失联 FE 和旧进程实例的资源；Leader 切换也不能假定原会话执行过 cleanup。该边界不是通过新增本地会话对象解决的。

## 4. 已确认的问题及触发路径

以下结论来自当前源码调用链；“已确认”表示存在明确的退出缺口，不表示已经运行故障注入测试。

| 编号 | 场景 | 触发条件与调用链 | 可确认影响 | 修复位置与验证 |
| --- | --- | --- | --- | --- |
| P01 | S01 | registerConnection 成功后，sendResponsePacket 抛 IOException；closeListener 尚未安装；catch 调用 cleanup，而 MySQL closeConnection 只关闭 Channel | connectionMap 和连接计数未注销，Context 仍被强引用。startTime 的设置也尚未执行，不能依靠正常空闲扫描补救 | 接入/清理路径保证注册后的失败直接注销；注入响应发送失败，断言池、计数、Channel 均已清理 |
| P02 | S01 | 接入 worker.execute 抛 RejectedExecutionException；拒绝分支发送错误响应时再次抛 IOException；位于它后面的 cleanup 被跳过 | 该路径没有保证显式关闭已接收的 Channel；尚未注册连接池，不属于池条目泄漏 | 拒绝响应使用 finally 保证关闭；同时注入任务拒绝和响应发送失败 |
| P03 | S08 | LoadLoadingTask.executeOnce 在无 Context 的线程上新建并绑定，结束时仅注销 Query；LoadTask/MasterTask 未解绑 | 方法/任务退出后线程仍持有该 Context，后续工作可能复用它。此事实不意味着每个任务累计一个永久对象 | 记录是否自行创建，拥有者收尾并恢复原绑定；同一工作线程连续执行两个任务验证隔离 |
| P04 | S09 | HTTP 认证开启，show_meta_info 经 executeCheckPassword 绑定 Context 后正常返回，例如缺少 action 的早返回 | 已检查请求链没有解绑；工作线程在返回后仍可持有请求 Context。通常是每线程残留，不能直接描述为每请求无界增长 | 建立请求级 finally；验证正常、早返回、异常和线程复用；异步派发另行明确拥有者 |
| P05 | S10 | BulkLoadJob.analyze 在线程原本无 Context 时创建 Dummy Context；方法正常或异常结束都没有 finally 关闭/解绑 | 当前方法没有兑现 getDummyCtx 的清理契约，返回时线程绑定仍存在；端到端持续时间还取决于调用者边界 | 只清理自行创建的 Context；借用已有 Context 时保留它；验证两种分支及解析失败 |

证据分别见 [AcceptListener][accept]、[MySQL 关闭适配器][mysql-close]、[超时检查][timeout]、[加载执行][load-once]、[HTTP 入口][http-show]、[BulkLoadJob.analyze][bulk-analyze]。

## 5. 待验证契约及不能直接认定的问题

| 编号 | 场景 | 已知事实 | 尚需验证及修复约束 |
| --- | --- | --- | --- |
| R01 | S07/S08/S10 | 多个拥有新 Context 的路径结束时只 clear/remove，或只关闭语句资源 | 逐调用点列出持有资源和其他清理责任方，才能确认缺少 cleanup 的实际影响；不能仅凭方法名判漏 |
| R02 | S06 | 转发结束移除代理索引和线程绑定，StatementContext 已关闭 | 核实其余代理本地资源；清理不能结束来源客户端会话 |
| R03 | S02/S04 | killConnection、cleanup、协议注销入口分散；MySQL 工作线程、超时线程和关闭回调可交错 | 验证重复注销、事务关闭和注册/关闭竞争。连接计数删除已有条件保护，Flight 协议已有 closed/命令锁；不能宣称所有重复关闭都不安全 |
| R04 | S04 | 配额拒绝已释放协议资源；工厂先分配适配器，再补齐环境、身份和 scheduler | 核实构建中途抛异常的资源交接。不能在 scheduler 尚未设置时盲目套用依赖它的 Flight cleanup |
| R05 | S07/S09/S10 | cleanup 无条件 remove 调用线程绑定；部分 helper 会替换已有绑定 | 明确当前作用域是否拥有 Context，退出恢复 previousContext；没有具体跨线程调用证据时，不把 API 风险写成已发生的误解绑 |

以下不列为问题：

- 旧临时表 session 登记遗留机制。
- 正常语句结束只 clear，而保持连接存活。
- Flight 单次 RPC 结束保留逻辑会话及合法的 deferred executor。
- 代理 Context 没有进入客户端连接池。
- 仅用于内部解析的 Context 没有占用客户端连接配额。
- AutoCloseConnectContext 不调用 cleanup 这一事实本身；必须结合具体所有权与资源路径判断。

## 6. 修复后的统一约束

不增加另一套 SessionHandle。仍由 ConnectContext 表达执行/会话状态，由现有连接池维护客户端会话生命周期；内部 Context 根据调用作用域或任务拥有权显式收尾，不强行纳入客户端配额。

1. **构造与注册分开。** 初始化字段不等于活跃连接登记；部分构建失败由创建入口负责释放已获得的资源。
2. **注册和关闭有明确顺序。** 关闭后的 Context 不能重新登记；成功登记的 Context 在任何后续失败中都能注销。
3. **清理入口收敛。** cleanup、KILL、超时和协议关闭共享一致的生命周期结束逻辑；协议适配器保留协议资源释放职责。调整调用方向时避免 cleanup、closeListener、unregisterConnection 互相递归。
4. **语句、线程和 Context 分别管理。** clear 结束一次语句的引用；绑定作用域恢复线程状态；拥有者结束 Context 生命周期。
5. **清理异常不截断其他收尾。** 通过 finally 或错误汇总保证必要步骤执行。需要重试的远端资源不能因已设置关闭标记而丢失；本地关闭与物理回收完成可有不同时间点。
6. **只在真实完成边界清理异步资源。** 提交失败由提交方处理，成功转交后的执行/取消/完成由任务处理；Flight deferred 结果遵循其专门生命周期。
7. **锁内只改状态及登记。** 回滚、取消、网络关闭、RPC 等操作在锁外执行；避免将耗时清理引入 admissionLock 或命令锁的长临界区。
8. **GC 不承担业务职责。** 线程和连接池强引用需要显式解除；弱引用不是漏调 cleanup 的修复方案。

这些约束落实后，临时表续租才能以现有登记中的有效 Owner Context 为依据。内部转发或一次性解析上下文不能仅因具有 sessionId 就参与续租。

## 7. 验证矩阵与推进顺序

| 验证场景 | 核心断言 |
| --- | --- |
| MySQL 握手失败、配额拒绝 | Channel 关闭；连接计数与注册前一致 |
| 注册成功后发送响应失败 | P01 的池条目和用户计数恢复，不能依赖超时或 GC |
| 接入任务拒绝且错误响应失败 | P02 仍显式关闭 Channel |
| 正常多条 SQL | 每条语句结束解绑线程，Context 继续注册可用 |
| COM_RESET_CONNECTION | 状态重置，连接及计数保留；失败不能报告完整成功 |
| KILL、超时、读线程退出交错 | 状态终止一次，索引和计数正确，事务处理不重复产生错误 |
| Flight 命令结束与会话结束 | RPC 结束不注销 token；CloseSession 后 token 不再可用 |
| Flight deferred 与关闭竞争 | 已登记及关闭后到达的 deferred executor 都有正确收尾 |
| 内部 owned/borrowed/nested 作用域 | 新建对象按责任关闭；借用对象不关闭；外层线程绑定恢复 |
| 同工作线程连续加载任务 | 前一个新建 Context 不残留给下一个任务 |
| HTTP 正常、早返回、异常 | 请求结束后原线程状态恢复，跨请求不复用残留 Context |
| 代理转发成功和失败 | 本地 Query/语句资源结束，来源会话保留 |
| FE 进程异常退出 | 不以 cleanup 回调发生作为集群资源回收的前提 |

建议先修 P01/P02，再修 P03/P04/P05 的拥有权和线程边界；最后依据 R01–R05 的逐项验证统一清理接口。并发测试使用 latch/barrier 控制交错，不依靠 sleep 猜测，不通过 System.gc 验证正确性。

现有 [ConnectPoolMgrTest][pool-test] 覆盖部分注销与计数行为，[FlightSessionsInConnectPoolTest][flight-test] 覆盖 CloseSession、KILL、空闲超时，[ConnectContextTest][ctx-test] 覆盖 reset、线程绑定和 deferred executor 部分清理。它们不是 P01–P05 已通过验证的证明；本次文档整理未运行这些测试。

## 8. 核查结论与边界

| 检查项 | 结论 |
| --- | --- |
| 目标与正确性 | 形成场景—调用链—问题—验证映射；P01–P05 有具体触发路径，R01–R05 保留验证边界 |
| 最小范围与复用 | 仅新增文档；建议复用现有 Context/连接池，未引入实现或新会话管理对象 |
| 并发及锁 | 明确 MySQL 读任务、超时线程、关闭回调及 Flight 命令/关闭并发；尚未进行竞态故障注入 |
| 生命周期 | 区分创建、登记、绑定、语句释放、任务完成、会话结束，覆盖 owned/borrowed 与部分构建失败 |
| 平行路径 | 对比 MySQL、Flight、代理、内部任务、HTTP；代表性路径之外的构造点仍需逐一核查 |
| 异常与条件 | P01/P02 为明确 IO 异常路径；内部任务按是否自行创建决定收尾，避免误关借用对象 |
| 性能 | 修复不依赖每次续租写 Journal 或 GC 扫描；避免在登记锁内做 RPC/资源回收 |
| 配置与兼容性 | 本次无新增配置、协议字段、存储格式或行为修改 |
| 事务及持久化 | 记录现有 closeTxn 归属与异常退出边界；未修改事务、Journal 或版本发布逻辑 |
| 数据不变量 | 本文不改变已提交/未提交数据可见性、Partition visible version、MoW delete bitmap；相关实现不在此次评审范围 |
| 资源与失败处理 | 核实强引用保留和显式收尾缺口；没有将所有未调用 cleanup 的上下文断定为泄漏 |
| 测试与结果 | 给出验证矩阵和已有测试位置；无新增或改写测试结果，无运行时复现结论 |
| 可观测性 | 后续修复应可关联 connectionId、生命周期阶段及失败原因；Flight 原始 token 不应出现在日志中 |

## 源码索引

以下链接指向本地核查工作区；行号对应文首源码基线。

- 基础机制：[初始化][ctx-init]、[连接登记][pool]、[线程绑定][thread]、[作用域包装器][auto-close]。
- 客户端与转发：[MySQL 接入][accept]、[读事件][read]、[Flight 会话][flight-session]、[跨 FE 转发][forward]。
- 内部执行：[统计查询][stats]、[导出][export]、[Job 任务框架][task-processor]、[Load 执行][load-once]、[回放解析][bulk-analyze]。
- HTTP：[请求 Context][rest-context]、[Show 接口][http-show]、[Web SQL 会话][websql]。

[ctx-init]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/qe/ConnectContext.java:417
[submit]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/qe/ConnectScheduler.java:75
[pool]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/qe/ConnectPoolMgr.java:116
[clear]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/qe/ConnectContext.java:1034
[reset]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/qe/ConnectContext.java:366
[thread]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/nereids/util/MoreFieldsThread.java:80
[accept]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/mysql/AcceptListener.java:56
[read]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/mysql/ReadListener.java:48
[mysql-process]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/qe/MysqlConnectProcessor.java:567
[reset-entry]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/qe/ConnectProcessor.java:152
[flight-session]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/arrowflight/sessions/FlightSessionsInConnectPool.java:67
[flight-command]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/arrowflight/protocol/FlightProtocolAdapter.java:592
[flight-close]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/arrowflight/FlightSqlConnectProcessor.java:202
[flight-release]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/arrowflight/protocol/FlightProtocolAdapter.java:276
[forward]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/service/FrontendServiceImpl.java:1168
[proxy-stmt]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/qe/ConnectProcessor.java:665
[auto-close]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/qe/AutoCloseConnectContext.java:38
[stats]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/statistics/util/StatisticsUtil.java:189
[export]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/load/ExportTaskExecutor.java:151
[insert-task]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/job/extensions/insert/InsertTask.java:153
[task-processor]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/job/executor/TaskProcessor.java:93
[load-once]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/load/loadv2/LoadLoadingTask.java:168
[load-task]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/load/loadv2/LoadTask.java:85
[master-task]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/task/MasterTask.java:31
[http-show]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/httpv2/rest/ShowAction.java:76
[rest-context]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/httpv2/rest/RestBaseController.java:116
[websql]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/httpv2/websql/WebSqlSessionManager.java:220
[dummy]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/qe/ConnectContextUtil.java:40
[vars]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/qe/AutoCloseSessionVariable.java:26
[mysql-close]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/mysql/protocol/MysqlProtocolAdapter.java:350
[timeout]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/qe/ConnectContext.java:1344
[bulk-analyze]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/main/java/org/apache/doris/load/loadv2/BulkLoadJob.java:309
[pool-test]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/test/java/org/apache/doris/qe/ConnectPoolMgrTest.java:190
[flight-test]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/test/java/org/apache/doris/arrowflight/sessions/FlightSessionsInConnectPoolTest.java:119
[ctx-test]: /Users/zhangwenxin/workplace/code/doris/incubator-doris/fe/fe-core/src/test/java/org/apache/doris/qe/ConnectContextTest.java:86
