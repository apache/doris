/**
 * @(#)PolicyTest.java, 2022-04-24.
 * <p>
 * Copyright 2022 Stalary.
 */
package org.apache.doris.policy;

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreatePolicyStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

/**
 * PolicyTest
 *
 * @author lirongqian
 * @since 2022/04/24
 */
public class PolicyTest {
    
    private static String runningDir = "fe/mocked/policyTest/" + UUID.randomUUID().toString() + "/";
    
    private static ConnectContext connectContext;
    
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);
    
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
    
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
    
        MetricRepo.init();
        createTable("create table test.table1\n" +
            "(k1 int, k2 int) distributed by hash(k1) buckets 1\n" +
            "properties(\"replication_num\" = \"1\");");
    }
    
    @Test
    public void testSql() throws Exception {
        createPolicy("CREATE POLICY test_row_policy ON test.table1 AS PERMISSIVE TO root USING (k1 = 1)");
        String queryStr = "select * from test.table1;";
        String retStr = Catalog.getCurrentCatalog().getPolicyMgr().rewriteOriginStmt("root", queryStr);
        System.out.println(retStr);
    }
    
    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
    }
    
    private static void createPolicy(String sql) throws Exception {
        CreatePolicyStmt createPolicyStmt = (CreatePolicyStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().getPolicyMgr().createPolicy(createPolicyStmt);
    }
}
