import org.apache.doris.regression.suite.ClusterOptions

suite("test_sc_stuck_when_be_down", "docker") {
    def options = new ClusterOptions()
    options.cloudMode = false
    options.beNum = 3
    options.feNum = 2
    options.enableDebugPoints()
    options.feConfigs += ["agent_task_health_check_intervals_ms=5000"]

    docker(options) {
        GetDebugPoint().clearDebugPointsForAllBEs()

        def tblName = "test_sc_stuck_when_be_down"
        sql """ DROP TABLE IF EXISTS ${tblName} """
        sql """
                CREATE TABLE IF NOT EXISTS ${tblName} (
                    `k` int NOT NULL,
                    `v0` int NOT NULL,
                    `v1` int NOT NULL
                ) 
                DUPLICATE KEY(`k`)
                DISTRIBUTED BY HASH(`k`) BUCKETS 24
                PROPERTIES (
                    "replication_allocation" = "tag.location.default: 3"
                )
        """
        sql """ INSERT INTO ${tblName} SELECT number, number, number from numbers("number" = "1024") """

        GetDebugPoint().enableDebugPointForAllBEs("SchemaChangeJob::_do_process_alter_tablet.block")
        try {
            sql """ ALTER TABLE ${tblName} MODIFY COLUMN v0 VARCHAR(100) """
            sleep(3000)
            cluster.stopBackends(1)
            GetDebugPoint().clearDebugPointsForAllBEs()
            waitForSchemaChangeDone {
                sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tblName}' ORDER BY createtime DESC LIMIT 1 """
                time 600
            }
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
        }

        GetDebugPoint().enableDebugPointForAllBEs("SchemaChangeJob::_do_process_alter_tablet.block")
        try {
            sql """ ALTER TABLE ${tblName} MODIFY COLUMN v1 VARCHAR(100) """
            sleep(3000)
            cluster.stopBackends(1, 2)
            GetDebugPoint().clearDebugPointsForAllBEs()
            waitForSchemaChangeDone {
                sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tblName}' ORDER BY createtime DESC LIMIT 1 """
                time 600
            }
            assertTrue(false)
        } catch (Exception ignore) {
            // do nothing
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
        }
    }
}