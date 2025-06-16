import org.apache.doris.regression.util.NodeType

suite("test_cloud_version_already_merged") {
    if (!isCloudMode()) {
        return
    }
    def tblName = "test_cloud_version_already_merged"
    sql """ DROP TABLE IF EXISTS ${tblName} FORCE; """
    sql """
            CREATE TABLE IF NOT EXISTS ${tblName} (
                `k1` int NOT NULL,
                `c1` int,
                `c2` int,
                `c3` int
                )UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true",
                "replication_num" = "1");
        """

    sql "insert into ${tblName} values(1,-1,-1,-1);"
    sql "insert into ${tblName} values(2,-2,-2,-2);"
    sql "insert into ${tblName} values(3,-3,-3,-3);"
    sql "insert into ${tblName} values(4,-4,-4,-4)"
    sql "insert into ${tblName} values(5,-5,-5,-5)"
    sql "insert into ${tblName} values(1,1,1,1);"
    sql "insert into ${tblName} values(2,2,2,2);"
    sql "insert into ${tblName} values(3,3,3,3);"
    sql "insert into ${tblName} values(4,4,4,4)"
    sql "insert into ${tblName} values(5,5,5,5)"


    sql "sync;"
    qt_sql "select * from ${tblName} order by k1;"

    def backends = sql_return_maparray('show backends')
    def tabletStats = sql_return_maparray("show tablets from ${tblName};")
    assert tabletStats.size() == 1
    def tabletId = tabletStats[0].TabletId
    def tabletBackendId = tabletStats[0].BackendId
    def tabletBackend
    for (def be : backends) {
        if (be.BackendId == tabletBackendId) {
            tabletBackend = be
            break;
        }
    }
    logger.info("tablet ${tabletId} on backend ${tabletBackend.Host} with backendId=${tabletBackend.BackendId}");     

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    try {
        GetDebugPoint().enableDebugPoint(tabletBackend.Host, tabletBackend.HttpPort as int, NodeType.BE, "Tablet::capture_consistent_versions.inject_failure", [tablet_id: tabletId, skip_by_option: true])
        GetDebugPoint().enableDebugPointForAllBEs("get_peer_replicas_address.enable_local_host")

        qt_sql """ SELECT * from ${tblName} ORDER BY k1 """

    } finally {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("Tablet::capture_consistent_versions.inject_failure", [tablet_id: tabletId])
        GetDebugPoint().enableDebugPointForAllBEs("get_peer_replicas_address.enable_local_host")

        test {
            sql """ SELECT * from ${tblName} ORDER BY k1 """
            exception "version already merged, meet error during remote capturing rowsets"
        }

    } finally {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
    }

    try {
        GetDebugPoint().enableDebugPoint(tabletBackend.Host, tabletBackend.HttpPort as int, NodeType.BE, "Tablet::capture_consistent_versions.inject_failure", [tablet_id: tabletId, skip_by_option: true])
        GetDebugPoint().enableDebugPointForAllBEs("get_peer_replicas_address.enable_local_host")
        GetDebugPoint().enableDebugPointForAllBEs("GetRowsetCntl::start_req_bg.inject_failure");

        test {
            sql """ SELECT * from ${tblName} ORDER BY k1 """
            exception "version already merged, meet error during remote capturing rowsets"
        }

    } finally {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
    }

    try {
        GetDebugPoint().enableDebugPoint(tabletBackend.Host, tabletBackend.HttpPort as int, NodeType.BE, "Tablet::capture_consistent_versions.inject_failure", [tablet_id: tabletId, skip_by_option: true])
        GetDebugPoint().enableDebugPointForAllBEs("get_peer_replicas_address.enable_local_host")
        GetDebugPoint().enableDebugPointForAllBEs("Tablet::_remote_get_rowsets_meta.inject_replica_address_fail");

        test {
            sql """ SELECT * from ${tblName} ORDER BY k1 """
            exception "version already merged, meet error during remote capturing rowsets"
        }

    } finally {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
}
