# 一些名词

- Transaction： 在描述中事务和导入是同一个概念，为了方便以后事务的集成，这里设计框架的时候考虑了事务的内容，而导入是事务的子内容，所以很多名词直接用事务了。

# 数据结构
## FE 中的数据结构
### 事务的状态
TransactionState 有4个状态： 

- PREPARE 表示事务已经开始了，Coordinator已经开始导入数据了。对于我们改造的旧系统来说就是Loading阶段开始了，由FE启动一个模块当做Coordinator来发起任务。
- COMMITTED 表示事务已经结束了，此时数据在BE上已经quorum存在了，但是FE并没有通知BE这个导入的version是多少，所以数据对查询还不可见。
- VISIBLE 表示这次导入事务对查询可见了。
- ABORTED 表示导入由于内部故障导致失败或者由用户显示的取消了导入任务，由reason字段来说明具体的原因

### 事务表相关的结构

table_version 表 

table_id | visible_version | next_version
---|---|---
1001 | 11 | 20
1002 | 13 | 19

- visible_version 表示这个table可见的版本号，也就是查询时给查询计划指定的版本号
- next_version 是version通知机制运行时给一个导入事务分配的版本号，分配完之后自动+1

transaction_state 表

transaction_id | affected_table_version | coordinator | transaction_state |start_time| commit_time | finish_time
---|---|---|---|--|--|--|
 11 | <1001, 12><1002,13>
 13 | <1002,14>

- transactiond_id 表示一个导入事务唯一的ID
- affected_table_version 表示一个导入事务涉及到的所有table，这个列表对检测导入冲突有用。这里记录的是一个tuple list，<tableid, version> 的list，在publishVersion的时候会用。
- coordinator 记录当前的协调节点，为以后的实时导入服务的，目前就是FE
- start_time 是导入事务进入prepare 状态的时间
- commit_time 是load完成进入committed状态的时间
- finish_time 是一个事务完成的时间，可以是visible的时间，可以是abort的时间
- 这里没有记录label，需要在现有的导入元数据中记录一个transactionid，把label映射到实时导入框架的transactionid上

## BE 中的数据结构
### BE 文件目录结构

目前没有看过BE的目录结构，这里的只是原理上的设计，我们以后可以改，但是思路是这样的。 假定每个tablet都有一个目录，目录结构如下：

- data 存储的就是现在的tablet的数据。
- staging 是数据刚导入的时候把数据存放在这个目录下，等修改version的时候把数据再迁移到deltas和data目录下，在version通知机制的api里会详细写一下。
- delta 存储的是delta文件，这个文件不会被compaction，会等待一定的时间（比如30 min后自动删除），这个文件夹主要用于副本恢复使用。

# API 设计
## FE 实时导入相关API

	service GlobalTransactionManager {
		// 这个API 供Coordinator发起导入任务时调用， 这个API的预期行为是：
		//  1 递增的生成一个TransactionID
		//	2 当前发起请求的节点作为Coordinator节点
		//	3 事务的初始状态为PREPARE
		//	4 把TransactionID， Label，Coordinator, TransactionState 信息写入事务表，TableList和CommitTime为空
		// 3. 把TransactionID信息返回给调用的Coordinator节点
		int64 beginTransaction();

		// 当coordinator导入完成后，把导入的完成情况汇报给FE时使用
		// label 是导入时用户指定的唯一标识
		// tabletsStatus 表示的是本次导入过程中涉及到的各个table的各个分片的副本的导入情况，status表示导入成功还是失败
		// fe收到这个信息后，做以下两个动作：
		//	1. 判断这次导入是否成功，这里主要是一致性方面，如果根据一致性协议判定导入失败，那么就在事务表中把事务状态标记为FAILED，然后返回客户端失败； 另外也需要判断这个导入是否被CANCEL，如果已经CANCEL直接返回失败信息即可。
		//  2. 如果判定导入成功，那么
		//		2.1 将导入失败的tablet标记为CLONE状态，然后生成RepairTabletTask，如果RepairTabletTask的version比现在commit的version小的话，那么要生成一个新的task，然后下发给BE
		//		2.2 计算本次导入涉及到的所有的tableId，将tableid的信息写入事务表，在事务表中把事务的状态标记为COMMITTED
		Status commitTransaction(int64 transactionId, list<tuple<tabletId,Status>> tabletsStatus)

		Status rollbackTransaction(int64 transactionId)
		
		// 获取一个table的一个transaction对应的version
		map<transactionid, version> getTransactionVersion(list<tuple<TransactionIds, tableId>>)
	}

	// 这个模块用来充当DPP,小批量，BROKER方式导入的Coordinator
	Service TransactionCoordinator {
		// BE 执行完load命令后，调用这个API给FE汇报导入完成情况，FE收到这个消息后，在内存中保存下来
		status finishLoad(list<tuple<transactionid,tabletid,status>>)
	}

	Service TabletReplicationManager {
		status finishClone()
	}

## BE Agent的API
	struct RepairTabletTask {
		// 表示从哪个BE上来读取数据
		string sourceIp;
		// 表示当前故障的BE的IP
		string targetIp;
		// 修复数据的version号
		string version; 
	}

	service LocalTransactonManager {
		// FE 调用BE，让BE从sourceBE上clone数据， version表示clone到哪个版本的数据，这个API注意以下几点：
		// 1. BE 在执行时需要跟sourceIp比对一下需要传递哪些数据，哪些数据本地有，哪些数据本地没有，是否需要全量恢复。
		// 2. sourceIP 对应的BE节点上version的数据可能还暂时不存在，那么BE需要等待一下。
		// 3. 这个API 要实现成为一个能够增量执行的API，因为FE会不断的发送不同的Repair任务给BE，比如FE发送一个version=8的，后来又发送一个version=9的，那么BE需要判断本地是否有repair任务在执行，如果在执行，那么就更新一下执行信息，没有就启动。
		// 4. repair过程中下载的文件先放到delta文件夹下，然后再放到data目录下。
		Status repairTablet(RepairTabletTask task)

		// 这个API只是这里假定的，这个应该跟BE现有的Load API 合成一个
		Status loadData(dataUrl, transactionid)
		// 当BE 收到这个消息时，读取本地staging目录下的文件的后缀名.stage3的文件，如果tableid 和 transactionid匹配，那么将文件，重命名，逻辑是：
		// 1. 首先在 delta 目录下建立硬链指向staing目录下的文件。
		// 2. 判断一下data目录中version-1的文件是否存在，如果存在并且当前tablet是正常状态时，那么在data 目录中也建立硬链。
		// 3. 把staging目录下的文件删除。
		// 如果BE 修改version时发生问题，那么把异常的tablet返回给FE
		void publishVersion(list <tuple<tableId, transactionid, version>>)
	}

# 相关流程和机制
## 导入流程

由于这一阶段只针对现有的导入流程优化，所以我们从loading阶段开始描述我们的方案， loading任务之前的阶段保持不变。

- 当Extract和Transform阶段执行完毕后，FE会收到通知，那么FE开始执行Loading任务，此时FE充当了Coordinator的角色，FE调用beginTransaction API在事务表中注册这个导入任务。
- FE 调用BE的loadData API 向BE发送loading 任务，在消息中要附加一个transactionid，表明这是一个新式的导入任务。【这里会不会成为瓶颈？但是以后实时导入不会有这个瓶颈，因为实时导入中load的通知是由coordinator通知的，是分散化的】对于通知不成功的，那么就一直轮训通知即可，实在是通知不成功，那么就把副本设置为异常状态即可。
- BE 收到loading任务后执行loading任务，不论是小批量还是DPP，都从目标源（对于DPP来说就是HDFS，对于小批量来说就是那个BE）上下载文件, 但是要把数据放到 staging 文件夹下，文件的后缀名是.stage1。
- 当BE执行完loading任务时，把文件的后缀名改成 .stage2
- BE 启动一个定时服务，扫描 staging 文件夹下的.stage2 的文件，如果涉及到一个transaction的所有的tablet的导入都完成了，那么BE调用 finishLoad API向FE汇报load完成信息, FE在内存中记录这个导入完成情况，注意这里没有持久化。
- BE 收到返回后，在内存中记录这个文件已经汇报过了，下次汇报的时候不再汇报，这样就能够避免每次都发送所有的导入完成的列表给FE了，但是这引入了一个新的问题，就是FE仅仅把状态保存在内存中，当FE重启或者FE迁移了内存状态就不在了，这个在FE故障处理章节介绍一下。
- FE 启动一个定时任务扫描所有的导入任务，如果一个transaction涉及到的所有的tablet都导入完成，或者连续的quorum完成时，那么FE 调用commitTransaction API 来完成事务，这一步FE要判断一个BE是否是导入不成功了，因为commitTransaction那个API中会根据状态设置tablet的状态。注意这里没有采用触发式的，是为了降低FE的元数据修改次数。

此时导入流程完毕，但是BE端并不知道transactionid和version的对应关系，数据也不可以查询。后续由version 通知机制完成BE端从transactionid到version的变更。

## version 通知机制

这个服务只运行在FE Leader上，不是Leader不运行。

- 遍历transaction_state 表，选择可以通知的version， 选择的方式是：
	-	事务的状态为COMMITTED
	-	事务的tableidlist 中每个table的version都等于 table_version 表中的visible_version + 1
	-	还要检查一下副本的状态是不是健康，如果有不健康的，那么也直接把load任务设置为失败
- 将遍历后获取的结果组装成list <tuple<tableId, transactionid, version>>格式，调用BE的publishVersoin API通知BE。
- BE 收到通知后， 根据tableId和transactionid从staging目录下找导入文件，完成文件名的变更，具体看API的描述，如果遇到异常那么尝试3次，如果还有错误那么跳过，依赖补救措施来搞定。
- 如果所有的BE 都返回成功，那么修改transaction_state 中的transaction修改为VISIBLE，同时把table_version 表中的visible_version 修改为对应的tableidlist 中记录的table的version。
- 如果只有部分BE返回成功，那么把那些没返回成功的BE的状态

## version通知机制的保证/补救措施

在某些情况下可能publishVersion不能更新tablet的transactionid到version，比如我们假设一个现象，BE在接收publishVersion时，一个磁盘掉了，然后又回来了，那么这个磁盘上的tablet的version实际是没有变的，但是FE会误认为所有的tablet的version都更新成功了。

所以在这里我们引入一个强制保证的机制：

- BE 定期的跟FE比对本地所有tablet的version，如果本地的version < FE中记录的visible version，那么判定本地是有问题的。
- BE 扫描本地tablet的staging目录，从中获得所有的transactionid，从FE中获取对应的version，然后更新本地的version。

另外，当BE收到publishVersion做变更时，BE要检查下tablet对应的version-1数据是否存在，如果不存在，说明上次的publish并没有成功，可能有什么意外情况没考虑到，那么BE也需要从FE中主动拉一下transactionid对应的version。


## BE 启动过程

- BE 启动时要完成自检的过程，跟FE通信汇报本地的tablet信息，如果一个磁盘坏了，那么需要向FE汇报，FE需要将这个tablet标记为异常。

## Compaction 逻辑

现在BE上一个tablet目录下的data目录里的数据
保持现有的compaction逻辑不变，因为version的通知是顺序的，所以最后一个version仍然是未决version，仍然不compaction。

## delta目录的清空逻辑

delta目录下的数据单纯是为了副本修复服务的，所以我们这里采取简单的定时的策略，目前考虑半小时自动删除，另外如果磁盘空间不足的话，可以考虑删除。

## 副本故障处理

副本修复的触发机制和类型：

- 当一个tablet的BE从故障中恢复汇报本地的tablet给FE时，FE决定这个tablet仍然由这个BE来存储时，FE直接生成RepairTabletTask。
- 当Coordinator调用commitTransaction的API时，检测到一个tablet没有完成不完整时：
	- 如果这个BE在元数据中标记为正常，那么此时立即生成一个RepairTabletTask。
	- 如果这个BE在元数据中标记为异常了，那么不执行副本修复。
- FE定时检查副本的状态，如果一个副本处于故障状态超过一定的时间（比如15min，这个时间要参考两个机器同时宕机的概率），那么立即生成RepairTabletTask。
- relbance 过程，当一个tablet从A机器迁移到B机器时，直接把B机器当做一个故障的副本，生成一个RepairTabletTask。
- 副本数目调大，新加的副本认为是异常副本，生成一个RepaireTabletTask。

注意： 生成RepairTabletTask的同时把故障的tablet标记为clone状态，对于未来的loading任务也要向这个节点发送，version通知也要发送，只是在计算quorum的时候不计算。

RepairReplicaTask的执行流程：

- FE 调用BE的repairTablet API，将RepairTabletTask发送给对应的BE。
- BE 执行RepairTabletTask。
- 当BE下载完后，跟data目录下的内容比对一下，如果完全对了，那么把数据在data目录下建立硬链，给FE汇报成功消息。

# 一些异常处理的思路
## BE 的一个磁盘掉了，然后重启了，收到publishVersion 消息后如何处理？
此时我们不做处理，仍然认为version通知成功了。 其实现在的BE也是没处理的，这相当于BE给FE汇报导入成功后，BE自己的磁盘又掉了的情况，我们不做处理，等BE的定时汇报由FE发现异常，标记tablet为异常。

## FE 宕机重启
- 内存中保存的导入完成情况丢失： 此时FE 成为Leader时，需要向所有的BE发送一个invalid消息，告诉BE本地的内存状态无效，从硬盘上汇报所有的导入进度信息给FE。
- RepairTabletTask丢失：在创建RepairTabletTask时把tablet设置为clone状态，此时可以遍历CLONE状态的tablet生成RepairTabletTask，让BE重新做即可，这里的sourceIP可能发生变化，但是我们不管了。

# 遗留的导入的改造方式
## 导入的改进方式
- FE Leader 重启时根据元数据中已经loading finished的任务的version，将这个version的最大值根据table写入table_version 的visible_version 字段中。
- FE Leader 根据正在loading的任务【尚未完成导入，ET阶段已经结束，Loading阶段未结束】，获取这些任务的version，获取最大的，把version写入table_version 的next_version 字段中。
- 对于已经开始执行loading阶段的导入，FE Leader继续用轮训的方式，让他们执行loading任务，在loading完成后，要增加修改table对应的visible_version的逻辑。 这里可能不仅仅是quorum导入完成，要等尽量长的时间，让所有的副本都导入成功才可以。 如果副本导入没成功，那么标记为故障。 【或许这一步我们可以直接cancel掉之前所有的导入】
- FE Leader上把现在追副本的任务停止。

# 其他一些考虑
- 处于loading 阶段的任务不能太多，否则BE端压力太大，过去version lock能达到这个效果，现在没有version lock了，需要增加一个限制。 我们是不是有这个机制了呢？