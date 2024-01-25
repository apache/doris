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

import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * column prune ut.
 */
public class ColumnPruningTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");

        createTable("create table test.student (\n" + "id int not null,\n" + "name varchar(128),\n"
                + "age int,sex int)\n" + "distributed by hash(id) buckets 10\n"
                + "properties('replication_num' = '1');");

        createTable("create table test.score (\n" + "sid int not null, \n" + "cid int not null, \n" + "grade double)\n"
                + "distributed by hash(sid,cid) buckets 10\n" + "properties('replication_num' = '1');");

        createTable("create table test.course (\n" + "cid int not null, \n" + "cname varchar(128), \n"
                + "teacher varchar(128))\n" + "distributed by hash(cid) buckets 10\n"
                + "properties('replication_num' = '1');");

        connectContext.setDatabase("test");
    }

    @Test
    public void testPruneColumns1() {
        // TODO: It's inconvenient and less efficient to use planPattern().when(...) to check plan properties.
        // Enhance the generated patterns in the future.
        PlanChecker.from(connectContext)
                .analyze("select id,name,grade from student left join score on student.id = score.sid"
                        + " where score.grade > 60")
                .customRewrite(new ColumnPruning())
                .matches(
                        logicalProject(
                                logicalFilter(
                                        logicalProject(
                                                logicalJoin(
                                                        logicalProject()
                                                                .when(p -> getOutputQualifiedNames(p).containsAll(
                                                                        ImmutableList.of(
                                                                                "test.student.id",
                                                                                "test.student.name"))),
                                                        logicalProject().when(
                                                                p -> getOutputQualifiedNames(p).containsAll(
                                                                        ImmutableList.of(
                                                                                "test.score.sid",
                                                                                "test.score.grade")))
                                                ))
                                                .when(p -> getOutputQualifiedNames(p)
                                                        .containsAll(
                                                                ImmutableList.of("test.student.name",
                                                                        "test.student.id")))
                                )
                        )
                );
    }

    @Test
    public void testPruneColumns2() {
        PlanChecker.from(connectContext)
                .analyze("select name,sex,cid,grade "
                        + "from student left join score on student.id = score.sid "
                        + "where score.grade > 60")
                .customRewrite(new ColumnPruning())
                .matches(
                        logicalProject(
                                logicalFilter(
                                        logicalProject(
                                                logicalJoin(
                                                        logicalProject()
                                                                .when(p -> getOutputQualifiedNames(p).containsAll(
                                                                        ImmutableList.of(
                                                                                "test.student.id",
                                                                                "test.student.name",
                                                                                "test.student.sex"))),

                                                        logicalRelation()
                                                ))
                                                .when(p -> getOutputQualifiedNames(p)
                                                        .containsAll(
                                                                ImmutableList.of("test.student.name",
                                                                        "test.score.cid",
                                                                        "test.score.grade",
                                                                        "test.student.sex")))
                                )
                        )
                );
    }

    @Test
    public void testPruneColumns3() {
        PlanChecker.from(connectContext)
                .analyze("select id,name from student where age > 18")
                .customRewrite(new ColumnPruning())
                .matches(
                        logicalProject(
                                logicalFilter(
                                        logicalProject().when(p -> getOutputQualifiedNames(p)
                                                .containsAll(ImmutableList.of(
                                                        "test.student.name",
                                                        "test.student.id",
                                                        "test.student.age")))
                                )
                        )
                );
    }

    @Test
    public void testPruneColumns4() {
        PlanChecker.from(connectContext)
                .analyze("select name,cname,grade "
                        + "from student left join score "
                        + "on student.id = score.sid left join course "
                        + "on score.cid = course.cid "
                        + "where score.grade > 60")
                .customRewrite(new ColumnPruning())
                .matches(
                        logicalProject(
                                logicalFilter(
                                        logicalProject(
                                                logicalJoin(
                                                        logicalProject(logicalJoin(
                                                                logicalProject(logicalRelation())
                                                                        .when(p -> getOutputQualifiedNames(
                                                                                p).containsAll(ImmutableList.of(
                                                                                "test.student.id",
                                                                                "test.student.name"))),
                                                                logicalRelation()

                                                        )).when(p -> getOutputQualifiedNames(p)
                                                                .containsAll(ImmutableList.of(
                                                                        "test.student.name",
                                                                        "test.score.cid",
                                                                        "test.score.grade"))),
                                                        logicalProject(logicalRelation())
                                                                .when(p -> getOutputQualifiedNames(p)
                                                                        .containsAll(ImmutableList.of(
                                                                                "test.course.cid",
                                                                                "test.course.cname")))
                                                )
                                        ).when(p -> getOutputQualifiedNames(p).containsAll(ImmutableList.of(
                                                "test.student.name",
                                                "test.course.cname",
                                                "test.score.grade")))
                                )
                        )
                );
    }

    @Test
    public void pruneCountStarStmt() {
        PlanChecker.from(connectContext)
                .analyze("SELECT COUNT(*) FROM test.course")
                .customRewrite(new ColumnPruning())
                .matches(
                        logicalAggregate(
                                logicalProject(
                                        logicalOlapScan()
                                ).when(p -> p.getProjects().get(0).getDataType().equals(IntegerType.INSTANCE)
                                        && p.getProjects().size() == 1)
                        )
                );
    }

    @Test
    public void pruneCountConstantStmt() {
        PlanChecker.from(connectContext)
                .analyze("SELECT COUNT(1) FROM test.course")
                .customRewrite(new ColumnPruning())
                .matches(
                        logicalAggregate(
                                logicalProject(
                                        logicalOlapScan()
                                ).when(p -> p.getProjects().get(0).getDataType().equals(IntegerType.INSTANCE)
                                        && p.getProjects().size() == 1)
                        )
                );
    }

    @Test
    public void pruneCountConstantAndSumConstantStmt() {
        PlanChecker.from(connectContext)
                .analyze("SELECT COUNT(1), SUM(2) FROM test.course")
                .customRewrite(new ColumnPruning())
                .matches(
                        logicalAggregate(
                                logicalProject(
                                        logicalOlapScan()
                                ).when(p -> p.getProjects().get(0).getDataType().equals(IntegerType.INSTANCE)
                                        && p.getProjects().size() == 1)
                        )
                );
    }

    @Test
    public void pruneCountStarAndSumConstantStmt() {
        PlanChecker.from(connectContext)
                .analyze("SELECT COUNT(*), SUM(2) FROM test.course")
                .customRewrite(new ColumnPruning())
                .matches(
                        logicalAggregate(
                                logicalProject(
                                        logicalOlapScan()
                                ).when(p -> p.getProjects().get(0).getDataType().equals(IntegerType.INSTANCE)
                                        && p.getProjects().size() == 1)
                        )
                );
    }

    @Test
    public void pruneCountStarAndSumColumnStmt() {
        PlanChecker.from(connectContext)
                .analyze("SELECT COUNT(*), SUM(grade) FROM test.score")
                .customRewrite(new ColumnPruning())
                .matches(
                        logicalAggregate(
                                logicalProject(
                                        logicalOlapScan()
                                ).when(p -> p.getProjects().get(0).getDataType().equals(DoubleType.INSTANCE)
                                        && p.getProjects().size() == 1)
                        )
                );
    }

    @Test
    public void pruneCountStarAndSumColumnAndSumConstantStmt() {
        PlanChecker.from(connectContext)
                .analyze("SELECT COUNT(*), SUM(grade) + SUM(2) FROM test.score")
                .customRewrite(new ColumnPruning())
                .matches(
                        logicalAggregate(
                                logicalProject(
                                        logicalOlapScan()
                                ).when(p -> p.getProjects().get(0).getDataType().equals(DoubleType.INSTANCE)
                                        && p.getProjects().size() == 1)
                        )
                );
    }

    @Test
    public void pruneColumnForOneSideOnCrossJoin() {
        PlanChecker.from(connectContext)
                .analyze("select id,name from student cross join score")
                .customRewrite(new ColumnPruning())
                .matches(
                        logicalProject(
                                    logicalJoin(
                                            logicalProject(logicalRelation())
                                                    .when(p -> getOutputQualifiedNames(p)
                                                            .containsAll(ImmutableList.of(
                                                                    "test.student.id",
                                                                    "test.student.name"))),
                                            logicalProject(logicalRelation())
                                                    .when(p -> getOutputQualifiedNames(p)
                                                            .containsAll(ImmutableList.of(
                                                                    "test.score.sid")))
                                    )
                        )
                );
    }

    @Test
    public void pruneAggregateOutput() {
        PlanChecker.from(connectContext)
                .analyze("select id from (select id, sum(age) from student group by id)a")
                .customRewrite(new ColumnPruning())
                .matches(
                        logicalProject(
                            logicalSubQueryAlias(
                                    logicalProject(
                                        logicalAggregate(
                                            logicalProject(
                                                logicalOlapScan()
                                            ).when(p -> getOutputQualifiedNames(p).equals(
                                                    ImmutableList.of("test.student.id")
                                            ))
                                        ).when(agg -> getOutputQualifiedNames(agg.getOutputs()).equals(
                                                ImmutableList.of("test.student.id")
                                )))
                            )
                        )
                );
    }

    private List<String> getOutputQualifiedNames(LogicalProject<? extends Plan> p) {
        return getOutputQualifiedNames(p.getOutputs());
    }

    private List<String> getOutputQualifiedNames(List<? extends NamedExpression> output) {
        return output.stream().map(NamedExpression::getQualifiedName).collect(Collectors.toList());
    }
}
