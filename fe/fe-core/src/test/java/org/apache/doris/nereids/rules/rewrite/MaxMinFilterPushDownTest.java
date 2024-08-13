package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;


public class MaxMinFilterPushDownTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        createTable("CREATE TABLE IF NOT EXISTS max_t(\n"
                + "`id` int(32),\n"
                + "`score` int(64) NULL,\n"
                + "`name` varchar(64) NULL\n"
                + ") properties('replication_num'='1');");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    public void testMaxRewrite() {
        String sql = "select id, max(score) from max_t group by id having max(score)>10";
        PlanChecker.from(connectContext).analyze(sql).rewrite()
                .matches(logicalFilter(logicalOlapScan()).when(filter -> filter.getConjuncts().size() == 1));
    }
    @Test
    public void testMinRewrite() {
        String sql = "select id, min(score) from max_t group by id having min(score)<10";
        PlanChecker.from(connectContext).analyze(sql).rewrite()
                .matches(logicalFilter(logicalOlapScan()).when(filter -> filter.getConjuncts().size() == 1));
    }

    @Test
    public void testMaxNotRewrite0() {
        String sql = "select id, max(score) from max_t group by id having max(score)<10";
        PlanChecker.from(connectContext).analyze(sql).rewrite()
                .nonMatch(logicalFilter(logicalOlapScan()));
    }
    @Test
    public void testMinNotRewrite1() {
        String sql = "select id, min(score) from max_t group by id having min(score)>10";
        PlanChecker.from(connectContext).analyze(sql).rewrite()
                .nonMatch(logicalFilter(logicalOlapScan()));
    }
    @Test
    public void testMinNotRewrite2() {
        String sql = "select id, min(score), max(score) from max_t group by id having min(score)>10";
        PlanChecker.from(connectContext).analyze(sql).rewrite()
                .nonMatch(logicalFilter(logicalOlapScan()));
    }
    @Test
    public void testMinNotRewrite3() {
        String sql = "select id, min(score), count(score) from max_t group by id having min(score)>10";
        PlanChecker.from(connectContext).analyze(sql).rewrite()
                .nonMatch(logicalFilter(logicalOlapScan()));
    }
    @Test
    public void testMinNotRewrite4() {
        String sql = "select id, max(score) from max_t group by id having abs(max(score))>10";
        PlanChecker.from(connectContext).analyze(sql).rewrite()
                .nonMatch(logicalFilter(logicalOlapScan()));
    }
}
