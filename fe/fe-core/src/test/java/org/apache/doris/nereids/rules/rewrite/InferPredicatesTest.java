// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

class InferPredicatesTest extends TestWithFeService implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");

        createTable("create table test.student (\n"
                + "id int not null,\n"
                + "name varchar(128),\n"
                + "age int,sex int)\n"
                + "distributed by hash(id) buckets 10\n"
                + "properties('replication_num' = '1');");

        createTable("create table test.score (\n"
                + "sid int not null, \n"
                + "cid int not null, \n"
                + "grade double)\n"
                + "distributed by hash(sid,cid) buckets 10\n"
                + "properties('replication_num' = '1');");

        createTable("create table test.course (\n"
                + "id int not null, \n"
                + "name varchar(128), \n"
                + "teacher varchar(128))\n"
                + "distributed by hash(id) buckets 10\n"
                + "properties('replication_num' = '1');");

        createTables("create table test.subquery1\n"
                        + "(k1 bigint, k2 bigint)\n"
                        + "duplicate key(k1)\n"
                        + "distributed by hash(k2) buckets 1\n"
                        + "properties('replication_num' = '1');\n",
                "create table test.subquery2\n"
                        + "(k1 varchar(10), k2 bigint)\n"
                        + "partition by range(k2)\n"
                        + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k2) buckets 1\n"
                        + "properties('replication_num' = '1');",
                "create table test.subquery3\n"
                        + "(k1 int not null, k2 varchar(128), k3 bigint, v1 bigint, v2 bigint)\n"
                        + "distributed by hash(k2) buckets 1\n"
                        + "properties('replication_num' = '1');",
                "create table test.subquery4\n"
                        + "(k1 bigint, k2 bigint)\n"
                        + "duplicate key(k1)\n"
                        + "distributed by hash(k2) buckets 1\n"
                        + "properties('replication_num' = '1');");

        connectContext.setDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    void inferPredicatesTest01() {
        String sql = "select * from student join score on student.id = score.sid where student.id > 1";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalJoin(
                            logicalFilter(
                                    logicalOlapScan()
                            ).when(filter -> !ExpressionUtils.isInferred(filter.getPredicate())
                                    & filter.getPredicate().toSql().contains("id > 1")),
                            logicalFilter(
                                    logicalOlapScan()
                            ).when(filter -> ExpressionUtils.isInferred(filter.getPredicate())
                                    & filter.getPredicate().toSql().contains("sid > 1"))
                        )
                );
    }

    @Test
    void inferPredicatesTest02() {
        String sql = "select * from student join score on student.id = score.sid";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalJoin(
                            logicalOlapScan(),
                            logicalOlapScan()
                        )
                );
    }

    @Test
    void inferPredicatesTest03() {
        String sql = "select * from student join score on student.id = score.sid where student.id in (1,2,3)";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalJoin(
                            logicalFilter(logicalOlapScan()).when(filter -> !ExpressionUtils.isInferred(filter.getPredicate())
                                    & filter.getPredicate().toSql().contains("id IN (1, 2, 3)")),
                            logicalFilter(logicalOlapScan()).when(filter -> ExpressionUtils.isInferred(filter.getPredicate())
                                    & filter.getPredicate().toSql().contains("sid IN (1, 2, 3)"))
                        )
                );
    }

    @Test
    void inferPredicatesTest04() {
        String sql = "select * from student join score on student.id = score.sid and student.id in (1,2,3)";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalJoin(
                            logicalFilter(logicalOlapScan()).when(filter -> !ExpressionUtils.isInferred(filter.getPredicate())
                                    & filter.getPredicate().toSql().contains("id IN (1, 2, 3)")),
                            logicalFilter(logicalOlapScan()).when(filter -> ExpressionUtils.isInferred(filter.getPredicate())
                                    & filter.getPredicate().toSql().contains("sid IN (1, 2, 3)"))
                        )
                );
    }

    @Test
    void inferPredicatesTest05() {
        String sql = "select * from student join score on student.id = score.sid join course on score.sid = course.id where student.id > 1";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalProject(logicalJoin(
                            logicalProject(logicalJoin(
                                logicalFilter(
                                    logicalOlapScan()
                                ).when(filter -> !ExpressionUtils.isInferred(filter.getPredicate())
                                        & filter.getPredicate().toSql().contains("id > 1")),
                                logicalFilter(
                                    logicalOlapScan()
                                ).when(filter -> ExpressionUtils.isInferred(filter.getPredicate())
                                        & filter.getPredicate().toSql().contains("sid > 1"))
                            )),
                            logicalFilter(
                                logicalOlapScan()
                            ).when(filter -> filter.getPredicate().toSql().contains("id > 1"))
                        ))
                );
    }

    @Test
    void inferPredicatesTest06() {
        String sql = "select * from student join score on student.id = score.sid join course on score.sid = course.id and score.sid > 1";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalProject(logicalJoin(
                            logicalProject(logicalJoin(
                                    logicalFilter(
                                            logicalOlapScan()
                                    ).when(filter -> ExpressionUtils.isInferred(filter.getPredicate())
                                            & filter.getPredicate().toSql().contains("id > 1")),
                                    logicalFilter(
                                            logicalOlapScan()
                                    ).when(filter -> !ExpressionUtils.isInferred(filter.getPredicate())
                                            & filter.getPredicate().toSql().contains("sid > 1"))
                            )),
                            logicalFilter(
                                    logicalOlapScan()
                            ).when(filter -> filter.getPredicate().toSql().contains("id > 1"))
                        ))
                );
    }

    @Test
    void inferPredicatesTest07() {
        String sql = "select * from student left join score on student.id = score.sid where student.id > 1";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalJoin(
                            logicalFilter(
                                    logicalOlapScan()
                            ).when(filter -> !ExpressionUtils.isInferred(filter.getPredicate())
                                    & filter.getPredicate().toSql().contains("id > 1")),
                            logicalFilter(
                                    logicalOlapScan()
                            ).when(filter -> ExpressionUtils.isInferred(filter.getPredicate())
                                    & filter.getPredicate().toSql().contains("sid > 1"))
                        )
                );
    }

    @Test
    void inferPredicatesTest08() {
        String sql = "select * from student left join score on student.id = score.sid and student.id > 1";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalJoin(
                            logicalOlapScan(),
                            logicalFilter(
                                    logicalOlapScan()
                            ).when(filter -> ExpressionUtils.isInferred(filter.getPredicate())
                                    & filter.getPredicate().toSql().contains("sid > 1"))
                        )
                );
    }

    @Test
    void inferPredicatesTest09() {
        // convert left join to inner join
        String sql = "select * from student left join score on student.id = score.sid where score.sid > 1";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalJoin(
                            logicalFilter(
                                logicalOlapScan()
                            ).when(filter -> ExpressionUtils.isInferred(filter.getPredicate())
                                    & filter.getPredicate().toSql().contains("id > 1")),
                            logicalFilter(
                                logicalOlapScan()
                            ).when(filter -> !ExpressionUtils.isInferred(filter.getPredicate())
                                    & filter.getPredicate().toSql().contains("sid > 1"))
                        )
                );
    }

    @Test
    void inferPredicatesTest10() {
        String sql = "select * from (select id as nid, name from student) t left join score on t.nid = score.sid where t.nid > 1";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalJoin(
                            logicalProject(
                                logicalFilter(
                                    logicalOlapScan()
                                ).when(filter -> !ExpressionUtils.isInferred(filter.getPredicate())
                                        & filter.getPredicate().toSql().contains("id > 1"))
                            ),
                            logicalFilter(
                                logicalOlapScan()
                            ).when(filter -> ExpressionUtils.isInferred(filter.getPredicate())
                                    & filter.getPredicate().toSql().contains("sid > 1"))
                        )
                );
    }

    @Test
    void inferPredicatesTest11() {
        String sql = "select * from (select id as nid, name from student) t left join score on t.nid = score.sid and t.nid > 1";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalJoin(
                            logicalProject(
                                    logicalOlapScan()
                            ),
                            logicalFilter(
                                    logicalOlapScan()
                            ).when(filter -> ExpressionUtils.isInferred(filter.getPredicate())
                                    & filter.getPredicate().toSql().contains("sid > 1"))
                        )
                );
    }

    @Test
    void inferPredicatesTest12() {
        String sql = "select * from student left join (select sid as nid, sum(grade) from score group by sid) s on s.nid = student.id where student.id > 1";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalJoin(
                            logicalFilter(
                                logicalOlapScan()
                            ).when(filter -> !ExpressionUtils.isInferred(filter.getPredicate())
                                    & filter.getPredicate().toSql().contains("id > 1")),
                            logicalProject(
                                logicalAggregate(
                                    logicalProject(
                                        logicalFilter(
                                            logicalOlapScan()
                                        ).when(filter -> ExpressionUtils.isInferred(filter.getPredicate())
                                                & filter.getPredicate().toSql().contains("sid > 1"))
                                   )
                                )
                            )
                        )
                );
    }

    @Test
    void inferPredicatesTest13() {
        String sql = "select * from (select id, name from student where id = 1) t left join score on t.id = score.sid";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalJoin(
                            logicalProject(
                                    logicalFilter(
                                            logicalOlapScan()
                                    ).when(filter -> !ExpressionUtils.isInferred(filter.getPredicate())
                                            & filter.getPredicate().toSql().contains("id = 1"))
                            ),
                            logicalFilter(
                                    logicalOlapScan()
                            ).when(filter -> ExpressionUtils.isInferred(filter.getPredicate())
                                    & filter.getPredicate().toSql().contains("sid = 1"))
                        )
                );
    }

    @Test
    void inferPredicatesTest14() {
        String sql = "select * from student left semi join score on student.id = score.sid where student.id > 1";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalJoin(
                            logicalFilter(
                                    logicalOlapScan()
                            ).when(filter -> !ExpressionUtils.isInferred(filter.getPredicate())
                                    & filter.getPredicate().toSql().contains("id > 1")),
                            logicalProject(
                                    logicalFilter(
                                            logicalOlapScan()
                                    ).when(filter -> ExpressionUtils.isInferred(filter.getPredicate())
                                            & filter.getPredicate().toSql().contains("sid > 1"))
                            )
                        )
                );
    }

    @Test
    void inferPredicatesTest15() {
        String sql = "select * from student left semi join score on student.id = score.sid and student.id > 1";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalJoin(
                            logicalFilter(
                                    logicalOlapScan()
                            ).when(filter -> !ExpressionUtils.isInferred(filter.getPredicate())
                                    & filter.getPredicate().toSql().contains("id > 1")),
                            logicalProject(
                                    logicalFilter(
                                            logicalOlapScan()
                                    ).when(filter -> ExpressionUtils.isInferred(filter.getPredicate())
                                            & filter.getPredicate().toSql().contains("sid > 1"))
                            )
                        )
                );
    }

    @Test
    void inferPredicatesTest16() {
        String sql = "select * from student left anti join score on student.id = score.sid and student.id > 1";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalJoin(
                            logicalOlapScan(),
                            logicalProject(
                                logicalFilter(
                                    logicalOlapScan()
                                ).when(filter -> ExpressionUtils.isInferred(filter.getPredicate())
                                        & filter.getPredicate().toSql().contains("sid > 1"))
                            )
                        )
                );
    }

    @Test
    void inferPredicatesTest17() {
        String sql = "select * from student left anti join score on student.id = score.sid and score.sid > 1";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalJoin(
                            logicalOlapScan(),
                            logicalProject(
                                    logicalFilter(
                                            logicalOlapScan()
                                    ).when(filter -> !ExpressionUtils.isInferred(filter.getPredicate())
                                            & filter.getPredicate().toSql().contains("sid > 1"))
                            )
                        )
                );
    }

    @Test
    void inferPredicatesTest18() {
        String sql = "select * from student left anti join score on student.id = score.sid where student.id > 1";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalJoin(
                            logicalFilter(
                                    logicalOlapScan()
                            ).when(filter -> !ExpressionUtils.isInferred(filter.getPredicate())
                                    & filter.getPredicate().toSql().contains("id > 1")),
                            logicalProject(
                                    logicalFilter(
                                            logicalOlapScan()
                                    ).when(filter -> ExpressionUtils.isInferred(filter.getPredicate())
                                            & filter.getPredicate().toSql().contains("sid > 1"))
                            )
                        )
                );
    }

    @Test
    void inferPredicatesTest19() {
        String sql = "select * from subquery1\n"
                + "left semi join (\n"
                + "  select t1.k3\n"
                + "  from (\n"
                + "    select *\n"
                + "    from subquery3\n"
                + "    left semi join\n"
                + "    (\n"
                + "      select k1\n"
                + "      from subquery4\n"
                + "      where k1 = 3\n"
                + "    ) t\n"
                + "    on subquery3.k3 = t.k1\n"
                + "  ) t1\n"
                + "  inner join\n"
                + "  (\n"
                + "    select k2,sum(k2) as sk2\n"
                + "    from subquery2\n"
                + "    group by k2\n"
                + "  ) t2\n"
                + "  on t2.k2 = t1.v1 and t1.v2 > t2.sk2\n"
                + ") t3\n"
                + "on t3.k3 = subquery1.k1";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalProject(logicalJoin(
                            logicalFilter(
                                    logicalOlapScan()
                            ).when(filter -> ExpressionUtils.isInferred(filter.getPredicate())
                                    & filter.getPredicate().toSql().contains("k1 = 3")),
                            logicalProject(
                                logicalJoin(
                                    logicalProject(logicalJoin(
                                       logicalProject(
                                               logicalFilter(
                                                       logicalOlapScan()
                                               ).when(filter -> ExpressionUtils.isInferred(filter.getPredicate())
                                                       & filter.getPredicate().toSql().contains("k3 = 3"))
                                       ),
                                       logicalProject(
                                               logicalFilter(
                                                       logicalOlapScan()
                                               ).when(filter -> !ExpressionUtils.isInferred(filter.getPredicate())
                                                       & filter.getPredicate().toSql().contains("k1 = 3"))
                                       )
                                    )),
                                    logicalAggregate(
                                        logicalProject(
                                                logicalOlapScan()
                                        )
                                    )
                                )
                            )
                        ))
                );
    }

    @Test
    void inferPredicatesTest20() {
        String sql = "select * from student left join score on student.id = score.sid and score.sid > 1 inner join course on course.id = score.sid";
        PlanChecker.from(connectContext).analyze(sql).rewrite().printlnTree();
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalProject(innerLogicalJoin(
                            logicalProject(innerLogicalJoin(
                                logicalFilter(
                                    logicalOlapScan()
                                ).when(filter -> ExpressionUtils.isInferred(filter.getPredicate())
                                        & filter.getPredicate().toSql().contains("id > 1")),
                                logicalFilter(
                                    logicalOlapScan()
                                ).when(filter -> !ExpressionUtils.isInferred(filter.getPredicate())
                                        & filter.getPredicate().toSql().contains("sid > 1"))
                            )),
                            logicalFilter(
                                logicalOlapScan()
                            ).when(filter -> filter.getPredicate().toSql().contains("id > 1"))
                        ))
                );
    }

    @Test
    void inferPredicatesTest21() {
        String sql = "select * from student,score,course where student.id = score.sid and score.sid = course.id and score.sid > 1";
        PlanChecker.from(connectContext).analyze(sql).rewrite().printlnTree();
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalProject(logicalJoin(
                            logicalProject(logicalJoin(
                                logicalFilter(
                                    logicalOlapScan()
                                ).when(filter -> ExpressionUtils.isInferred(filter.getPredicate())
                                        & filter.getPredicate().toSql().contains("id > 1")),
                                logicalFilter(
                                    logicalOlapScan()
                                ).when(filter -> !ExpressionUtils.isInferred(filter.getPredicate())
                                        & filter.getPredicate().toSql().contains("sid > 1"))
                            )),
                            logicalFilter(
                                logicalOlapScan()
                            ).when(filter -> filter.getPredicate().toSql().contains("id > 1"))
                        ))
                );
    }

    /**
     * test for #15310
     */
    @Test
    void inferPredicatesTest22() {
        String sql = "select * from student join (select sid as id1, sid as id2, grade from score) s on student.id = s.id1 where s.id1 > 1";
        PlanChecker.from(connectContext).analyze(sql).rewrite().printlnTree();
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalJoin(
                            logicalFilter(
                                    logicalOlapScan()
                            ).when(filter -> ExpressionUtils.isInferred(filter.getPredicate())
                                    & filter.getPredicate().toSql().contains("id > 1")),
                            logicalProject(
                                    logicalFilter(
                                            logicalOlapScan()
                                    ).when(filter -> !ExpressionUtils.isInferred(filter.getPredicate())
                                            & filter.getPredicate().toSql().contains("sid > 1"))
                            )
                        )
                );
    }

    /**
     * in this case, filter on relation s1 should not contain s1.id = 1.
     */
    @Test
    void innerJoinShouldNotInferUnderLeftJoinOnClausePredicates() {
        String sql = "select * from student s1"
                + " left join (select sid as id1, sid as id2, grade from score) s2 on s1.id = s2.id1 and s1.id = 1"
                + " join (select sid as id1, sid as id2, grade from score) s3 on s1.id = s3.id1 where s1.id = 2";
        PlanChecker.from(connectContext).analyze(sql).rewrite().printlnTree();
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalProject(
                        logicalJoin(
                                logicalFilter(
                                        logicalOlapScan()
                                ).when(filter -> filter.getConjuncts().size() == 1
                                        && !ExpressionUtils.isInferred(filter.getPredicate())
                                        && filter.getPredicate().toSql().contains("id = 2")),
                                any()
                        ).when(join -> join.getJoinType() == JoinType.LEFT_OUTER_JOIN)
                ));
    }

    @Test
    void inferPredicateByConstValue() {
        String sql = "select c1 from (select 1 c1 from student) t inner join score t2 on t.c1=t2.sid";
        PlanChecker.from(connectContext).analyze(sql).rewrite().printlnTree();
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalProject(
                         logicalJoin(any(),
                                 logicalProject(
                                         logicalFilter(
                                            logicalOlapScan()
                                         ).when(filter -> filter.getConjuncts().size() == 1
                                         && ExpressionUtils.isInferred(filter.getPredicate())
                                         && filter.getPredicate().toSql().contains("sid = 1"))
                                 )
                         ))
                );
    }
}
