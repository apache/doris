package org.apache.doris.analysis;

import java.util.UUID;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SelectStmtTest {
    private static String runningDir = "fe/mocked/DemoTest/" + UUID.randomUUID().toString() + "/";

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testGroupingSets() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        UtFrameUtils.createMinDorisCluster(runningDir);
        String createDbStmtStr = "create database db1;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, ctx);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
        System.out.println(Catalog.getCurrentCatalog().getDbNames());
        // 3. create table tbl1
        String createTblStmtStr = "create table db1.tbl1(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTblStmtStr, ctx);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
        String selectStmtStr = "select k1,k2,MAX(k4) from db1.tbl1 GROUP BY GROUPING sets ((k1,k2),(k1),(k2),());";
        UtFrameUtils.parseAndAnalyzeStmt(selectStmtStr, ctx);
        String selectStmtStr2 = "select k1,k4,MAX(k4) from db1.tbl1 GROUP BY GROUPING sets ((k1,k4),(k1),(k4),());";
        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("column: `k4` cannot both in select list and aggregate functions when using GROUPING"
                + " SETS/CUBE/ROLLUP, please use union instead.");
        UtFrameUtils.parseAndAnalyzeStmt(selectStmtStr2, ctx);
        String selectStmtStr3 = "select k1,k4,MAX(k4+k4) from db1.tbl1 GROUP BY GROUPING sets ((k1,k4),(k1),(k4),());";
        UtFrameUtils.parseAndAnalyzeStmt(selectStmtStr3, ctx);
        String selectStmtStr4 = "select k1,k4+k4,MAX(k4+k4) from db1.tbl1 GROUP BY GROUPING sets ((k1,k4),(k1),(k4),()"
                + ");";
        UtFrameUtils.parseAndAnalyzeStmt(selectStmtStr4, ctx);





    }


}