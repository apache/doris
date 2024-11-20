package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableSet;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class JoinSplitForNullSkewTest  extends TestWithFeService implements MemoPatternMatchSupported {
    private static final MockUp<SessionVariable> mockUpForSubClass = new MockUp<SessionVariable>() {
        @Mock
        public Set<Integer> getEnableNereidsRules() {
            return ImmutableSet.of(RuleType.valueOf("JOIN_SPLIT_FOR_NULL_SKEW").type());
        }
    };
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        createTables(
                "create table split_join_for_null_skew_t(a int null, b int not null, c varchar(10) null, d date, dt datetime)"
                        + "distributed by hash(a) properties('replication_num'='1')"
        );
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    void testRewriteLeftJoin() {
        PlanChecker.from(connectContext)
                .analyze("select t1.a,t1.b,t2.c,t2.dt from split_join_for_null_skew_t t1 left join split_join_for_null_skew_t t2 on t1.a=t2.a order by 1,2,3,4")
                .rewrite()
                .printlnTree()
                .matches(logicalUnion(logicalProject(logicalFilter(any()).when(f -> f.getConjuncts().size() == 1 && f.getConjuncts().iterator().next() instanceof IsNull))
                        , logicalProject(logicalJoin(logicalProject(logicalFilter(any()).when(f -> f.getConjuncts().size() == 1 &&  f.getConjuncts().iterator().next() instanceof Not)),any()))));
    }

    @Test
    void testRewriteLeftJoinSelectAll() {
        PlanChecker.from(connectContext)
                .analyze("select * from split_join_for_null_skew_t t1 left join split_join_for_null_skew_t t2 on t1.a=t2.a order by 1,2,3,4")
                .rewrite()
                .printlnTree()
                .matches(logicalUnion(logicalProject(logicalFilter(any()).when(f -> f.getConjuncts().size() == 1 && f.getConjuncts().iterator().next() instanceof IsNull))
                        , logicalProject(logicalJoin(logicalFilter(any()).when(f -> f.getConjuncts().size() == 1 && f.getConjuncts().iterator().next() instanceof Not), any()))));
    }

    @Test
    void testRewriteRightJoinSelectAll() {
        PlanChecker.from(connectContext)
                .analyze("select t1.a,t1.b,t2.dt,t2.c from split_join_for_null_skew_t t1 right join split_join_for_null_skew_t t2 on t1.c=t2.c order by 1,2,3,4")
                .rewrite()
                .printlnTree()
                .matches(logicalUnion(logicalProject(logicalFilter(any()).when(f -> {
                            if (f.getConjuncts().size() != 1) {
                                return false;
                            }
                            Expression firstConjunct = f.getConjuncts().iterator().next();
                            if (!(firstConjunct instanceof IsNull)) {
                                return false;
                            }
                            if (!(firstConjunct.child(0) instanceof SlotReference)) {
                                return false;
                            }
                            if (!((SlotReference) firstConjunct.child(0)).getQualifiedName().equals("internal.test.t2.c")) {
                                return false;
                            }
                            return true;
                        }))
                        , logicalProject(logicalJoin(logicalProject(logicalFilter(any()).when(f -> {
                            if (f.getConjuncts().size() != 1) {
                                return false;
                            }
                            Expression firstConjunct = f.getConjuncts().iterator().next();
                            if (!(firstConjunct instanceof Not) || !(firstConjunct.child(0) instanceof IsNull)) {
                                return false;
                            }
                            Expression expr = firstConjunct.child(0).child(0);
                            if (!(expr instanceof SlotReference)) {
                                return false;
                            }
                            if (!((SlotReference) expr).getQualifiedName().equals("internal.test.t2.c")) {
                                return false;
                            }
                            return true;
                        })), any()))));
    }


    @Test
    void testRewriteWhenLeftChildHasIsNotNullFilter() {
        PlanChecker.from(connectContext)
                .analyze("select t1.dt,t1.b,t2.a,t2.b from split_join_for_null_skew_t t1 left join split_join_for_null_skew_t t2 on t1.a=t2.a where t1.a is not null order by 1,2,3,4")
                .rewrite()
                .printlnTree()
                .matches(logicalUnion(any(), any()));
    }
}
