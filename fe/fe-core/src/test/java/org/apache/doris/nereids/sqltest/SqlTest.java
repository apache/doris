package org.apache.doris.nereids.sqltest;

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.rewrite.logical.ReorderJoin;
import org.apache.doris.nereids.trees.expressions.NamedExpressionUtil;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SqlTest extends TestWithFeService implements PatternMatchSupported {
    private final NereidsParser parser = new NereidsParser();

    private final List<String> testSql = ImmutableList.of(
            "SELECT * FROM (SELECT * FROM T1 T) T2",
            "SELECT * FROM T1 TT1 JOIN (SELECT * FROM T2 TT2) T ON TT1.ID = T.ID",
            "SELECT * FROM T1 TT1 JOIN (SELECT TT2.ID FROM T2 TT2) T ON TT1.ID = T.ID",
            "SELECT T.ID FROM T1 T",
            "SELECT A.ID FROM T1 A, T2 B WHERE A.ID = B.ID",
            "SELECT * FROM T1 JOIN T1 T2 ON T1.ID = T2.ID"
    );

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");

        createTables(
                "CREATE TABLE IF NOT EXISTS T1 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS T2 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS T3 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n"
        );
    }

    @Override
    protected void runBeforeEach() throws Exception {
        NamedExpressionUtil.clear();
    }

    @Test
    void testSql() {
        // String sql = "SELECT *"
        //         + " FROM T1, T2 LEFT JOIN T3 ON T2.id = T3.id"
        //         + " WHERE T1.id = T2.id";
        String sql = "SELECT *"
                + " FROM T2 LEFT JOIN T3 ON T2.id = T3.id, T1"
                + " WHERE T1.id = T2.id";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .printlnTree()
                .applyTopDown(new ReorderJoin())
                .implement()
                .printlnTree();
    }
}
