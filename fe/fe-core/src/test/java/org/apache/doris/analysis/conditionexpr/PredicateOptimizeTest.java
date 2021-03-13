package org.apache.doris.analysis.conditionexpr;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.ConditionExpr.PredicateOptimize;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.ExistsPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.Predicate;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.Subquery;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class PredicateOptimizeTest {
    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDir = "fe/mocked/predicateoptimizetest/" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinDorisCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();

        // disable bucket shuffle join
        Deencapsulation.setField(connectContext.getSessionVariable(), "enableBucketShuffleJoin", false);

        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
        createTable("create table test.test1\n" +
                "(\n" +
                "    query_id char(48) comment \"Unique query id\",\n" +
                "    time datetime not null comment \"Query start time\",\n" +
                "    client_ip varchar(32) comment \"Client IP\",\n" +
                "    user varchar(64) comment \"User name\",\n" +
                "    db varchar(96) comment \"Database of this query\",\n" +
                "    state varchar(8) comment \"Query result state. EOF, ERR, OK\",\n" +
                "    query_time bigint comment \"Query execution time in millisecond\",\n" +
                "    scan_bytes float comment \"Total scan bytes of this query\",\n" +
                "    scan_rows double comment \"Total scan rows of this query\",\n" +
                "    isAndroid boolean comment \"Returned rows of this query\",\n" +
                "    stmt_id int comment \"An incremental id of statement\",\n" +
                "    is_query tinyint comment \"Is this statemt a query. 1 or 0\",\n" +
                "    uptime date comment \"Frontend ip of executing this statement\",\n" +
                "    stmt varchar(2048) comment \"The original statement, trimed if longer than 2048 bytes\"\n" +
                ")\n" +
                "partition by range(time) ()\n" +
                "distributed by hash(query_id) buckets 1\n" +
                "properties(\n" +
                "    \"dynamic_partition.time_unit\" = \"DAY\",\n" +
                "    \"dynamic_partition.start\" = \"-30\",\n" +
                "    \"dynamic_partition.end\" = \"3\",\n" +
                "    \"dynamic_partition.prefix\" = \"p\",\n" +
                "    \"dynamic_partition.buckets\" = \"1\",\n" +
                "    \"dynamic_partition.enable\" = \"true\",\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");");
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    @Test
    public void testExtractPredicates () throws Exception {
        PredicateOptimize optimize = new PredicateOptimize();
        // with no predicate
        Expr expr1 = null;
        List<Expr> predicates = optimize.extractPredicates(null);
        Assert.assertTrue(predicates  == null);

        // one predicate
        TableName tbl = new TableName("test", "test1");
        Expr left = new SlotRef(tbl, "stmt_id");
        LiteralExpr right = new IntLiteral(1);
        expr1 = new BinaryPredicate(BinaryPredicate.Operator.EQ, left, right);
        predicates = optimize.extractPredicates(expr1);
        Assert.assertTrue(predicates.size() == 1);
        Assert.assertTrue(predicates.get(0).toSql().contains("`stmt_id` = 1"));

        // contains `and`
        Expr left2 = new SlotRef(tbl, "query_id");
        LiteralExpr right2 = new StringLiteral("test");
        Expr expr2 = new BinaryPredicate(BinaryPredicate.Operator.EQ, left2, right2);
        Expr andExpr = new CompoundPredicate(CompoundPredicate.Operator.AND, expr1, expr2);
        predicates = optimize.extractPredicates(andExpr);
        Assert.assertTrue(predicates.size() == 2);
        Assert.assertTrue(predicates.get(0).toSql().contains("`stmt_id` = 1"));
        Assert.assertTrue(predicates.get(1).toSql().contains("`query_id` = 'test'"));

        // contains `or`
        Expr orExpr = new CompoundPredicate(CompoundPredicate.Operator.OR, expr1, expr2);
        predicates = optimize.extractPredicates(orExpr);
        Assert.assertTrue(predicates.size() == 2);
        Assert.assertTrue(predicates.get(0).toSql().contains("`stmt_id` = 1"));
        Assert.assertTrue(predicates.get(1).toSql().contains("`query_id` = 'test'"));

        // contains `and` `or`
        Expr hybridExpr = new CompoundPredicate(CompoundPredicate.Operator.OR, andExpr, orExpr);
        predicates = optimize.extractPredicates(hybridExpr);
        Assert.assertTrue(predicates.size() == 4);
        Assert.assertTrue(predicates.get(0).toSql().contains("`stmt_id` = 1"));
        Assert.assertTrue(predicates.get(1).toSql().contains("`query_id` = 'test'"));
        Assert.assertTrue(predicates.get(2).toSql().contains("`stmt_id` = 1"));
        Assert.assertTrue(predicates.get(3).toSql().contains("`query_id` = 'test'"));
    }



    @Test
    public void testCheckIfSupportPredicate() throws Exception {
        PredicateOptimize optimize = new PredicateOptimize();

        TableName tbl = new TableName("test", "test1");
        Expr left = new SlotRef(tbl, "stmt_id");
        LiteralExpr listExpr1 = new IntLiteral(1);
        LiteralExpr listExpr2 = new IntLiteral(3);
        LiteralExpr listExpr3 = new IntLiteral(5);
        List<Expr> listExpr = new ArrayList<>();
        listExpr.add(listExpr1);
        listExpr.add(listExpr2);
        listExpr.add(listExpr3);
        Predicate inPredicate = new InPredicate(left, listExpr, true);
        // TODO: LikePredicate operator is not poublic
//        Expr column = new SlotRef(tbl, "query_id");
//        Expr strExpr = new StringLiteral("%test%");
//        Predicate likePredicate = new LikePredicate(LikePredicate.Operator.LIKE, column, strExpr);

        QueryStmt stmt = new SelectStmt(null, null, null);
        Subquery subQuery = new Subquery(stmt);
        Predicate existPredicate = new ExistsPredicate(subQuery, true);

        List<Expr> predicates = new ArrayList<>();
        predicates.add(inPredicate);
        predicates.add(existPredicate);
        Assert.assertTrue(!optimize.checkIfSupportPredicate(predicates));
    }

    @Test
    public void testOptimize() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        try {
            // no predicate
            String sql = "select * from test1;";
            String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(!explainString.contains("PREDICATES"));

            // one predicate
            sql = "select * from test1 where stmt_id = 1";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `stmt_id` = 1"));

            // tinyint 'and', can merge
            sql = "select * from test1 where stmt_id > 1 and stmt_id > 2";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `stmt_id` > 2"));
            // tinyint 'or', can merge
            sql = "select * from test1 where stmt_id > 1 or stmt_id > 2";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `stmt_id` > 1"));
            // tinyint 'and', conflict
            sql = "select * from test1 where stmt_id = 1 and stmt_id = 2";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("EMPTYSET"));

            // smallint 'and', can merge
            sql = "select * from test1 where stmt_id > 129 and stmt_id > 2";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `stmt_id` > 129"));
            sql = "select * from test1 where stmt_id > 2 and stmt_id > 129";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `stmt_id` > 129"));
            // smallint 'or', can merge
            sql = "select * from test1 where stmt_id > 129 or stmt_id > 2";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `stmt_id` > 2"));
            sql = "select * from test1 where stmt_id > 2 or stmt_id > 129";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `stmt_id` > 2"));
            // smallint 'and', conflict
            sql = "select * from test1 where stmt_id = 129 and stmt_id = 2";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("EMPTYSET"));
            sql = "select * from test1 where stmt_id = 2 and stmt_id = 129";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("EMPTYSET"));

            // int 'and', can merge
            sql = "select * from test1 where stmt_id > 102400 and stmt_id > 2";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `stmt_id` > 102400"));
            sql = "select * from test1 where stmt_id > 2 and stmt_id > 102400";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `stmt_id` > 102400"));
            // int 'or', can merge
            sql = "select * from test1 where stmt_id > 102400 or stmt_id > 2";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `stmt_id` > 2"));
            sql = "select * from test1 where stmt_id > 2 or stmt_id > 102400";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `stmt_id` > 2"));
            // int 'and', conflict
            sql = "select * from test1 where stmt_id = 102400 and stmt_id = 2";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("EMPTYSET"));
            sql = "select * from test1 where stmt_id = 2 and stmt_id = 102400";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("EMPTYSET"));

            // largeint 'and', can merge
            sql = "select * from test1 where stmt_id > 9223372036854775806 and stmt_id > 2";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `stmt_id` > 9223372036854775806"));
            sql = "select * from test1 where stmt_id > 2 and stmt_id > 9223372036854775806";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `stmt_id` > 9223372036854775806"));
            // largeint 'or', can merge
            sql = "select * from test1 where stmt_id > 9223372036854775806 or stmt_id > 2";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `stmt_id` > 2"));
            sql = "select * from test1 where stmt_id > 2 or stmt_id > 9223372036854775806";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `stmt_id` > 2"));
            // largeint 'and', conflict
            sql = "select * from test1 where stmt_id = 9223372036854775806 and stmt_id = 2";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("EMPTYSET"));
            sql = "select * from test1 where stmt_id = 2 and stmt_id = 9223372036854775806";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("EMPTYSET"));

            // bigint 'and', can merge
            sql = "select * from test1 where stmt_id > 9223372036854775828 and stmt_id > 2";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `stmt_id` > 9223372036854775828"));
            sql = "select * from test1 where stmt_id > 2 and stmt_id > 9223372036854775828";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `stmt_id` > 9223372036854775828"));
            // bigint 'or', can merge
            sql = "select * from test1 where stmt_id > 9223372036854775838 or stmt_id > 2";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `stmt_id` > 2"));
            sql = "select * from test1 where stmt_id > 2 or stmt_id > 9223372036854775838";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `stmt_id` > 2"));
            // bigint 'and', conflict
            sql = "select * from test1 where stmt_id = 9223372036854775828 and stmt_id = 2";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("EMPTYSET"));
            sql = "select * from test1 where stmt_id = 2 and stmt_id = 9223372036854775828";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("EMPTYSET"));

            // char/varchar 'and', can merge
            sql = "select * from test1 where query_id > '123' and query_id > '456'";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `query_id` > '456'"));
            // char/varchar 'or', can merge
            sql = "select * from test1 where query_id > '123' or query_id > '456'";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `query_id` > '123'"));
            // char/varchar 'and', conflict
            sql = "select * from test1 where query_id = '123' and query_id = '456'";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("EMPTYSET"));

            // float 'and', can merge
            sql = "select * from test1 where scan_bytes > 0.1 and scan_bytes > 0.2";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `scan_bytes` > 0.2"));
            // float 'or', can merge
            sql = "select * from test1 where scan_bytes > 0.1 or scan_bytes > 0.2";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `scan_bytes` > 0.1"));
            // float 'and', conflict
            sql = "select * from test1 where scan_bytes = 0.1 and scan_bytes = 0.2";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("EMPTYSET"));

            // double 'and', can merge
            sql = "select * from test1 where scan_rows > 0.11111111 and scan_rows > 0.2222222222";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `scan_rows` > 0.2222222222"));
            // double 'or', can merge
            sql = "select * from test1 where scan_rows > 0.11111111 or scan_rows > 0.2222222222";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `scan_rows` > 0.11111111"));
            // double 'and', conflict
            sql = "select * from test1 where scan_rows = 0.11111111 and scan_rows = 0.2222222222";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("EMPTYSET"));

            // boolean 'and', can merge
            sql = "select * from test1 where isAndroid > true and isAndroid > false";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `isAndroid` > TRUE"));
            // double 'or', can merge
            sql = "select * from test1 where isAndroid > true or isAndroid > false";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `isAndroid` > FALSE"));
            // double 'and', conflict
            sql = "select * from test1 where isAndroid = true and isAndroid = false";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("EMPTYSET"));

            // date 'and', can merge
            sql = "select * from test1 where uptime > '2020-12-12' and uptime > '2020-12-11'";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `uptime` > '2020-12-12 00:00:00'"));
            // date 'or', can merge
            sql = "select * from test1 where uptime > '2020-12-12' or uptime > '2020-12-11'";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `uptime` > '2020-12-11 00:00:00'"));
            // date 'and', conflict
            sql = "select * from test1 where uptime = '2020-12-12' and uptime = '2020-12-11'";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("EMPTYSET"));

            // datetime 'and', can merge
            sql = "select * from test1 where time > '2020-12-12' and time > '2020-12-11'";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `time` > '2020-12-12 00:00:00'"));
            // datetime 'or', can merge
            sql = "select * from test1 where time > '2020-12-12' or time > '2020-12-11'";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("PREDICATES: `time` > '2020-12-11 00:00:00'"));
            // datetime 'and', conflict
            sql = "select * from test1 where time = '2020-12-12' and time = '2020-12-11'";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("EMPTYSET"));

            // binary predicate already been tested
            // isNull predicate is not support
            sql = "select * from test1 where db is null and db = 'db1'";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("`db` IS NULL, `db` = 'db1'"));
            sql = "select * from test1 where db is null or db = 'db1'";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("(`db` IS NULL) OR (`db` = 'db1')"));
            // in predicate is not support
            sql = "select * from test1 where db in ('db1', 'db2')";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("`db` IN ('db1', 'db2')"));
            // exists predicate is not support
            sql = "select * from test1 where exists (select db from test1)";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("predicates is NULL"));
            // like predicate is not support
            sql = "select * from test1 where db like '%db2%'";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("`db` LIKE '%db2%'"));

            // column contains function
            sql = "select * from test1 where from_unixtime(query_time) > 1000 and from_unixtime(query_time) > 200";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("from_unixtime(`query_time`) > 1000.0, from_unixtime(`query_time`) > 200.0"));

            // two columns
            sql = "select * from test1 where db = user and db = user";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("`db` = `user`, `db` = `user`"));
            // right literal contains function
            sql = "select * from test1 where uptime < CURDATE() and uptime > CURDATE()";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("EMPTYSET"));
            sql = "select * from test1 where uptime < from_unixtime(1000) and uptime > from_unixtime(1000)";
            explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "EXPLAIN " + sql);
            Assert.assertTrue(explainString.contains("EMPTYSET"));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
    }
}
