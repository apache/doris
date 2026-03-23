# 现状和问题

存算分离下mow表上高频高并发导入时，无法像存算一体那样通过水平拓展资源来提高导入吞吐

## 存算一体
两阶段提交
- 每个导入在commit阶段确定自己的版本
  - 此后，下一个导入可以直接拿下一个版本，不需要等这个版本对应的事务publish完
- 下发delete bitmap计算任务到各个BE上，BE上tablet计算增量rowset的delete bitmap，然后修改本地rocksdb中tablet元数据
- FE publish daemon线程不断定期轮询所有事务下发的计算任务是否完成，该事务的任务都完成之后，fe修改内存中相关partition的version
- 导入计算delete bitmap的过程和compaction的冲突也是只在tablet级别的，通过BE内存锁来互斥
- mow计算delete bitmap任务时的版本连续性要求导致的任务间的依赖关系只在be节点tablet粒度，某个tablet上的某个计算任务发现前面缺版本，会等该tablet上前面版本publish完成，但这不会阻塞其他后续导入事务在其他tablet上的相关计算
这里由于 tablet元数据在各个BE节点本地的rocksdb中，所有和tablet的相关操作被均摊到各个BE节点上。所以可以通过水平拓展BE个数或增加每个BE资源来减少上述整个commit+publish过程的耗时，从而减少事务延迟，增加吞吐

## 存算分离
一阶段提交，并发导入相互抢版本。
目前是类似悲观锁的处理，各个并发导入首先在fe内存抢表级别的内存写锁，然后在持有这个锁的情况下串行执行以下操作：
- 去ms拿表级别的分布式锁(和该表下所有tablet上的compaction抢)
- 去ms获取各个partition version
  - 如果有pending的大事务，需要同步等待其完成
- 下发delete bitmap计算任务到对应BE节点并同步等到全部成功
- 去ms提交事务
  - 需要读写和导入涉及的tablet个数成正比的rowset kv
  - 修改partition version kv
  - 需要删除和导入涉及的tablet个数成正比的pending delete bitmap kv
  - 即使走lazy commit并且提前返回，下一个事务也需要在FE内存表锁内等待这个大事务推进完成
  - 如果导入涉及tablet很多会很慢，并且可能让ms fdb压力高导致其他ms操作也变慢
增加BE个数或每个BE的资源只能加快delete bitmap的计算，但是由于元数据在fdb中，最后提交事务需要在fdb读写和导入涉及的tablet个数成正比kv，并且各个导入的这个过程是完全串行的。在导入涉及tablet很多的时候，事务延迟很高，导致总体吞吐低。并且并发抢锁导致事务延迟不稳定。

# 主要改动
- Cloud mow导入改为两阶段提交，不考虑兼容旧版本，通过表属性控制是否启用，只能在新表上使用
- 在ms新增parition commit version KV，当前的partition version KV作为visible version。导入commit阶段将partition commit version+1，publish之后将partition visible version+1。查询通过partition visible ersion来查
- FE内存维护一个已经commit还没有publish的事务信息，包括partition commit versions, tabletcommitinfo(tablet和所在的BE)。
- 新增一个后台线程轮询尝试下发delete bitmap计算任务，一个线程池对计算完delete bitmap的事务到ms提交
- 当前，因为导入事务更新delete bitmap到ms后还可能失败，需要用pending delete bitmap KV来让下一个更新delete bitmap的导入或compaction清理在一个tablet上上一次失败导入残留的delete bitmap。两阶段提交后commit成功后事物必然成功，不需要pending delete bitmap KV了。这也优化了commit_txn的时间(不需要删除pending delete bitmap kv了)
- 只有commit partition version和最后到ms提交事务需要在内存表锁中进行，publih阶段计算delete bitmap不需要
- ms的表级别分布式锁不需要了，导入更新delete bitmap和comaption的冲突缩小到tablet级别，改成tablet级别的锁。考虑到导入和comapction绝大多数时候都在同一个BE上进行，大部分情况通过内存锁互斥已经够了，基本不会在ms发生冲突
- 两阶段提交commit成功后必然成功，如果commit成功后有节点重启需要额外处理，目前有些导入相关信息没有持久化
  - commit前算过的rowsetids和对应的delete bitmap，可以不持久化，publish阶段对历史rowset全量重算
  - 导入参数(列更新/灵活列更新等)，publish计算delete bitmap时依赖，必须持久化，放到TxnInfoPB
  - 导入涉及的partition, tablet及所在的BE(tabletcommitinfo); partition commit version。下发delete bitmap计算任务需要，最后到ms提交事务需要。存算一体记录在trasactionstate中通过editlog持久化。这里可以持久化到ms的TxnInfoPB里，TxnInfoPB增加partition commit versions和involved_tablets字段。FE重启切主的时候通过扫ms全量恢复。为了防止每次全量扫对ms压力太大，按照db分批拉取，每次的request: <instance_id, db_id, start_txn_id, limit>
    - 可以对committed状态的事务增加一种KV做索引？
- tmp rowset转正式rowset不需要在最终ms commit_txn的过程中，直接在delete bitmap计算任务完成后各tablet独立convert rowset meta。最终在ms commit_txn只需要修改partition visible version和事务信息
- 和子事务的交叉？ 暂时不考虑
- 和快照链的交叉？暂时不考虑
- BE提交事务的处理？直接禁止？暂时不考虑

# 风险点
- publish对FE依赖很重
- FE内存需要多存一些commit事务信息
- FE切主重启后，从ms全量恢复可能对ms压力很大，并且在之前的commit事务完成之前会一直阻塞后续在对应表上的导入

# 流程对比

## Cloud mow 当前
- begin txn
- 各tablet独立prepare_rowset (write recycle_rowset_key)
- 各tablet独立flush数据，计算delete bitmap
- 各tablet独立commit_rowset (remove recycle_rowset_key, write meta_rowset_tmp_key)。并将当前计算过的历史rowset和对应的delete bitmap，以及导入参数信息(列更新等)放入BE内存的txn info cache(以<txn_id, tablet_id>索引，事务超时时间作为过期时间)

开始提交事务
- 在FE等该表的内存写锁
- 尝试去MS拿表级别的锁(和compaction竞争)，成功拿锁后，从tablet stats KV拿compaction cnts，从tablet meta中拿tablet state(write meta_delete_bitmap_update_lock_key, clear mow_tablet_job_key, read stats_tablet_key, read meta_tablet_key)
- 从MS拿涉及的partition version (read partition_version_key)，如果有pending的大事务，同步等待推进其完成
- 下发delete bitmap计算任务(包括version, compaction cnts, tablet state)到BE，同步等待所有任务结果(成功/失败)
- 对对应tablet计算增量delete bitmap，从txn info cache找<txn_id, tablet_id>的entry是否存在，不存在返回失败，整个导入失败
- tablet状态发生变化则去ms sync_rowset
- 对增量rowset计算delete bitmap
- 将本次导入在这个tablet上产生的delete bitmap更新到ms
  - 检查是否还持有ms表锁
  - 如果该tablet下有pending delete bitmap KV(上一个失败事务留下的)，删除对应的delete bitmap KV
  - 检查要更新的version是否和当前ms中的partition version对的上
  - 将当前需要更新的delete bitmap keys写到tablet的pending delete bitmap value中
  - 将delete bitmap写入ms

在MS提交事务(未达到大事务阈值)
- scan tmp rowset kv
- 读partition version kV获取下一个版本，如果有pending的大事务，同步等待推进完成
- 检查ms表锁是否存在，删除所有tablet上的pending delete bitmap KV
- tmp rowset meta更新version，写meta_rowset_key
- 更新partition version + 1
- 更新TxnInfoPB
- 删除tmp rowset kv
- 释放ms表锁
(达到大事务阈值):.....

- 释放FE内存表锁

在事务最终在MS提交成功之前任何节点挂掉或FE切主则整个导入事务失败


## Cloud mow 两阶段提交
- begin txn
- 各tablet独立prepare_rowset (write recycle_rowset_key)
- 各tablet独立flush数据，计算delete bitmap
- 各tablet独立commit_rowset (remove recycle_rowset_key, write meta_rowset_tmp_key)。并将当前计算过的历史rowset和对应的delete bitmap，以及导入参数信息(列更新等)放入BE内存的txn info cache(以<txn_id, tablet_id>索引，事务超时时间作为过期时间)

commit事务
- 拿FE表内存写锁(和其他并发导入互斥)
- 在ms更新partition commit version + 1
- 更新TxnInfoPB
  - 修改txn_info.status = COMMITTED, (目前COMMITTED表示有大事务正在推进，这里改成表示两阶段提交里的commit状态)
  - txn_info.committed_version=v, 
  - txn_info.involved_tablets=[t1, t2, ...]
- 将事务加入到FE内存的committed txns set，包括partition commit version，tabletcommitinfo
- 释放内存表锁

导入在FE同步等待事务publish完成，超过某个超时时间返回publish_timeout

计算delete bitmap
- FE一个后台线程定期遍历committed txns set拿txn来publish
- 根据partition commit version，tabletcommitinfo下发delete bitmap计算任务到对应的BE
- 从txn info cache找<txn_id, tablet_id>的entry是否存在，不存在则从持久化位置恢复必要信息
- Agent task机制保证了失败重发
- 检查版本是否连续
- 拿BE内存tablet写锁
- 拿ms tablet级别锁
- 计算增量rowset的deletebitmap，更新delete bitmap到meta service
- 向ms convert tmp rowset meta to visible rowset meta
- 将rowset和对应delete bitmap直接放入BE内存cloudtablet中
- 返回成功

publish事务
- 后台线程轮询对计算完delete bitmap的事务尝试publish
- 对某个完成的事务，拿FE表内存写锁
- 拿着commit version， tabletcommitinfo去ms提交事务

在MS提交事务
- 拿FE表内存写锁(和其他并发导入互斥)
- 更新partition visible version + 1
- 更新TxnInfoPB
  - txn_info.status = VISIBLE
  - clear txn_info.involved_tablets
- 释放内存表锁

commit成功之后必须成功，需要额外处理
- 需要额外持久化每个事务的tabletcommitinfo和commit version到ms的TxnInfoPB
- 需要额外持久化导入参数
- FE每次切主/重启需要从MS扫txn_info_key，把其中commit事务信息放到内存，为了防止一次性对ms压力太大，按照db分批拉取，每次的request: <instance_id, db_id, start_txn_id, limit>







