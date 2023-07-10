package org.apache.doris.catalog;

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.alter.AlterJobV2.JobState;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.common.DdlException;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.Planner;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class ModifyStorageMediumTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        SystemInfoService clusterInfo = Env.getCurrentEnv().getClusterInfo();
        List<Backend> allBackends = clusterInfo.getAllBackends();
        // set all backends' storage medium to SSD
        for (Backend backend : allBackends) {
            if (backend.hasPathHash()) {
                backend.getDisks().values().stream()
                        .peek(diskInfo -> diskInfo.setStorageMedium(TStorageMedium.SSD));
            }
        }
    }

    @Override
    protected void runAfterAll() throws Exception {
        Env.getCurrentEnv().clear();
    }

    public void envTest() throws Exception {
        createDatabase("db1");
        System.out.println(Env.getCurrentInternalCatalog().getDbNames());

        String sql1 = "CREATE TABLE IF NOT EXISTS db1.t1 (pk INT, v1 INT sum) AGGREGATE KEY (pk) "
                + "DISTRIBUTED BY HASH(pk) BUCKETS 1 PROPERTIES ('replication_num' = '1');";
        Assertions.assertDoesNotThrow(() -> createTables(sql1));
        String sql2 = "CREATE TABLE IF NOT EXISTS db1.t2 (pk INT, v1 INT sum) AGGREGATE KEY (pk) "
                + "DISTRIBUTED BY HASH(pk) BUCKETS 1 PROPERTIES ('replication_num' = '1', 'storage_medium' = 'ssd');";
        Assertions.assertDoesNotThrow(() -> createTables(sql2));
        String sql3 = "CREATE TABLE IF NOT EXISTS db1.t3 (pk INT, v1 INT sum) AGGREGATE KEY (pk) "
                + "DISTRIBUTED BY HASH(pk) BUCKETS 1 PROPERTIES ('replication_num' = '1', 'storage_medium' = 'hdd');";
        Assertions.assertThrows(DdlException.class, () -> createTables(sql3));
    }

}
