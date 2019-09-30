# SHOW FRONTENDS
## description
    该语句用于查看 FE 节点
    语法：
        SHOW FRONTENDS;

    说明：
        1. name 表示该 FE 节点在 bdbje 中的名称。
        2. Join 为 true 表示该节点曾经加入过集群。但不代表当前还在集群内（可能已失联）
        3. Alive 表示节点是否存活。
        4. ReplayedJournalId 表示该节点当前已经回放的最大元数据日志id。
        5. LastHeartbeat 是最近一次心跳。
        6. IsHelper 表示该节点是否是 bdbje 中的 helper 节点。
        7. ErrMsg 用于显示心跳失败时的错误信息。
        
## keyword
    SHOW, FRONTENDS

