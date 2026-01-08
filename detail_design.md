导入结束通知BE转正内存中的rowset meta 详细设计

思路
- 导入在BE上完成后将产生的临时rowset meta放到内存中
- 在ms commit_txn 之后将需要修改的rowset meta字段数据放入CommitTxnResponse中
- FE(或BE转发给FE)通知这次导入涉及到的tablet所在的所有BE，带上在ms中修改的rowset meta数据
- BE收到通知后，对应修改内存中的临时rowset meta，并将其加入到tablet meta中
FE侧改动
- 在executeCommitTxnRequest向ms commit_txn成功后，根据resp和tabletCommitInfos构造TMakeCloudTmpRsVisibleRequest通知BE
- 对于mow，某一个导入在publish阶段必须看到上一个版本的rowset meta，如果上一个版本对应的转正通知到达be时晚过这个时间点，这个机制就没用了，还是会去ms sync_rowset。而其他表如果没有查询，在导入过程中不需要看到历史版本的rowset。这里可能需要将mow表的通知和非mow表的通知分开，防止在多表高频导入下mow表上导入对应的通知发的太晚。
  - 对mow表，直接在executeCommitTxnRequest中给be发agenttask；对非mow表，在某个队列中提交一个任务，另一个后台线程定时消费这个队列给be发agent task
- 当be conf enable_stream_load_commit_txn_on_be=true时，可能会在be上向ms commit_txn，成功后需要转发到FE通知BE
- FrontendService新增rpc: 
service FrontendService {
    // ...
    TForwardMakeCloudTmpRsVisibleResult ForwardMakeCloudTmpRsVisible(1: ForwardMakeCloudTmpRsVisibleRequest)
}

struct ForwardMakeCloudTmpRsVisibleRequest {
    1: optional i64 txn_id
    2: optional list<Types.TTabletCommitInfo> commitInfos
    3: optional map<Types.TPartitionId, Types.TVersion> partition_version
    4: optional i64 update_version_visible_time
}
BE侧改动
- 增加CloudPendingRSMgr在内存维护一个sharded map：<txn_id, tablet_id> -> <rowset_meta, expiration_time>， 用于存放导入过程中产生的临时rowset meta，过期时间是导入事务超时时间。一个后台线程定期清理其中过期的entry
  - 在CloudMetaMgr::commit_rowset中给ms rpc成功后，对于导入，将rowset meta放入CloudPendingRSMgr中
  - 部分列更新可能会在publish阶段修改rowset meta，在CloudMetaMgr::update_tmp_rowset最后更新CloudPendingRSMgr中对应的rowset meta
- 由于FE的通知可能会乱序到达BE，可能存在version=a+1对应的rowset meta已经被通知转正了而version=a的rowset meta还没有收到FE的转正通知，而加入到tablet meta中rowset meta的版本必须是连续的，需要对每个CloudTablet再维护一个map VisiblePendingRSMap：<version> -> <rowset_meta, expiration_time> 表示已经被FE通知过的，准备放入tablet meta中的rowset meta
- 新增AgentTask：TMakeCloudTmpRsVisibleRequest:
struct TAgentTaskRequest {
    // ...
    1001: optional TMakeCloudTmpRsVisibleRequest make_cloud_tmp_rs_visible
}

struct TMakeCloudTmpRsVisibleRequest {
    1: optional i64 txn_id
    2: optional map<Types.TPartitionId, Types.TVersion> partition_version
    3: optional i64 update_version_visible_time
}
- BE在收到FE的通知rpc TMakeCloudTmpRsVisibleRequest后，在CloudPendingRSMgr中找所有需要修改的rowset meta，对能找到的，修改相应字段，并将其放入所属CloudTablet的VisiblePendingRSMap中
- 对于所有有修改的CloudTablet，设其当前最大版本为cur_max_version：
  - 删除VisiblePendingRSMap中version <= cur_max_version的entry
  - 在VisiblePendingRSMap中找一段从cur_max_version+1开始的最长的连续版本的rowset meta，从map中移除，并通过CloudTablet::add_rowsets放入tablet meta中
  - 对于mow表，还需要处理这些rowset的delete bitmap。直接调用CloudMetaMgr::sync_tablet_delete_bitmap(会先尝试从CloudTxnDeleteBitmapCache中获取，拿不到时再从MS获取)
MS侧改动
MS commit_txn时更改了rowset meta中的start_version/end_version/visible_ts_ms字段，CommitTxnResponse已经包含了rowset meta中需要修改的全部信息(partition_ids/versions/version_update_time_ms)，不用额外改动
正确性保证
通过CloudTable::add_rowsets处理添加rowset meta到tablet meta的逻辑，visible version由ms产生，一个visible version一定对应一个成功的导入。并且添加rowset meta时保证添加的是当前最大版本之后的一段连续版本，和已有版本不会有交叉














