package org.apache.doris.cluster;

import com.google.common.collect.ImmutableMap;
import org.apache.doris.analysis.AlterSystemStmt;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.system.Backend;
import org.apache.doris.utframe.TestWithFeService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DecommissionBackendTest extends TestWithFeService {

    @Override
    protected int backendNum() {
        return 3;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 10;
    }

    @Test
    public void testDecommissionBackend() throws Exception {
        // 1. create connect context
        connectContext = createDefaultCtx();

        ImmutableMap<Long, Backend> idToBackendRef = Env.getCurrentSystemInfo().getIdToBackend();
        Assertions.assertEquals(backendNum(), idToBackendRef.size());

        // 2. create database db1
        createDatabase("db1");
        System.out.println(Env.getCurrentInternalCatalog().getDbNames());

        // 3. create table tbl1
        createTable("create table db1.tbl1(k1 int) distributed by hash(k1) buckets 3 properties('replication_num' = '2');");

        // 4. query tablet num
        int tabletNum = Env.getCurrentInvertedIndex().getTabletMetaMap().size();

        // 5. execute decommission
        Backend backend = idToBackendRef.values().stream().findFirst().orElseThrow(() -> new RuntimeException("Not any be existed"));
        String decommissionStmtStr = "alter system decommission backend \"127.0.0.1:" + backend.getHeartbeatPort() + "\"";
        AlterSystemStmt decommissionStmt = (AlterSystemStmt) parseAndAnalyzeStmt(decommissionStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterCluster(decommissionStmt);

        Assertions.assertEquals(true, backend.isDecommissioned());
        long startTimestamp = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTimestamp < 60000
            && Env.getCurrentSystemInfo().getIdToBackend().containsKey(backend.getId())) {
            Thread.sleep(1000);
        }

        Assertions.assertEquals(backendNum() - 1, Env.getCurrentSystemInfo().getIdToBackend().size());
        Assertions.assertEquals(tabletNum, Env.getCurrentInvertedIndex().getTabletMetaMap().size());

    }

}
