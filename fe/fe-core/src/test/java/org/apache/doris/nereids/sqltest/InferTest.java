package org.apache.doris.nereids.sqltest;

import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class InferTest extends SqlTestBase {
    @Test
    void testInferNotNullAndInferPredicates() {
        // Test InferNotNull, EliminateOuter, InferPredicate together
        String sql = "select * from T1 left outer join T2 on T1.id = T2.id where T2.id = 4";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matchesFromRoot(
                        innerLogicalJoin(
                                logicalFilter().when(f -> f.getPredicate().toString().equals("(id#0 = 4)")),
                                logicalFilter().when(f -> f.getPredicate().toString().equals("(id#2 = 4)"))
                        )
                );
    }

    @Test
    void testInferNotNullFromFilterAndEliminateOuter2() {
        String sql
                = "select * from T1 right outer join T2 on T1.id = T2.id where T1.id = 4 OR (T1.id > 4 AND T2.score IS NULL)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .printlnTree()
                .matchesFromRoot(
                        innerLogicalJoin(
                                logicalFilter().when(
                                        f -> f.getPredicate().toString().equals("((id#0 = 4) OR (id#0 > 4))")),
                                logicalOlapScan()
                        )
                );
    }

    @Test
    void testInferNotNullFromFilterAndEliminateOuter3() {
        String sql
                = "select * from T1 full outer join T2 on T1.id = T2.id where T1.id = 4 OR (T1.id > 4 AND T2.score IS NULL)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matchesFromRoot(
                        logicalFilter(
                                leftOuterLogicalJoin(
                                        logicalFilter().when(
                                                f -> f.getPredicate().toString().equals("((id#0 = 4) OR (id#0 > 4))")),
                                        logicalOlapScan()
                                )
                        ).when(f -> f.getPredicate().toString()
                                .equals("((id#0 = 4) OR ((id#0 > 4) AND score IS NULL))"))
                );
    }

    @Test
    @Disabled
    void testInferNotNullFromJoinAndEliminateOuter() {
        String sql
                = "select * from (select T1.id from T1 left outer join T2 on T1.id = T2.id) T1 left semi join T3 on T1.id = T3.id";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .printlnTree()
                .rewrite()
                .printlnTree();
    }
}
