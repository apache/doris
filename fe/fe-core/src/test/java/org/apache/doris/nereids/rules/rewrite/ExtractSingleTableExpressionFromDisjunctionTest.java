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

import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Set;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ExtractSingleTableExpressionFromDisjunctionTest implements MemoPatternMatchSupported {
    Plan student;
    Plan course;
    SlotReference courseCid;
    SlotReference courseName;
    SlotReference studentAge;
    SlotReference studentGender;

    @BeforeAll
    public final void beforeAll() {
        student = new LogicalOlapScan(PlanConstructor.getNextRelationId(), PlanConstructor.student, ImmutableList.of(""));
        course = new LogicalOlapScan(PlanConstructor.getNextRelationId(), PlanConstructor.course, ImmutableList.of(""));
        //select *
        //from student join course
        //where (course.cid=1 and student.age=10) or (student.gender = 0 and course.name='abc')
        courseCid = (SlotReference) course.getOutput().get(0);
        courseName = (SlotReference) course.getOutput().get(1);
        studentAge = (SlotReference) student.getOutput().get(3);
        studentGender = (SlotReference) student.getOutput().get(1);
    }
    /**
     *(cid=1 and sage=10) or (sgender=1 and cname='abc')
     * =>
     * (cid=1 or cname='abc') + (sage=10 or sgender=1)
     */

    @Test
    public void testExtract1() {
        Expression expr = new Or(
                new And(
                        new EqualTo(courseCid, new IntegerLiteral(1)),
                        new EqualTo(studentAge, new IntegerLiteral(10))
                ),
                new And(
                        new EqualTo(studentGender, new IntegerLiteral(1)),
                        new EqualTo(courseName, new StringLiteral("abc"))
                )
        );
        Plan join = new LogicalJoin<>(JoinType.CROSS_JOIN, student, course, null);
        LogicalFilter root = new LogicalFilter<>(ImmutableSet.of(expr), join);
        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new ExtractSingleTableExpressionFromDisjunction())
                .matchesFromRoot(
                        logicalFilter()
                                .when(filter -> verifySingleTableExpression1(filter.getConjuncts()))
                );
        Assertions.assertNotNull(studentGender);
    }

    private boolean verifySingleTableExpression1(Set<Expression> conjuncts) {
        Expression or1 = new Or(
                new EqualTo(courseCid, new IntegerLiteral(1)),
                new EqualTo(courseName, new StringLiteral("abc"))
        );
        Expression or2 = new Or(
                new EqualTo(studentAge, new IntegerLiteral(10)),
                new EqualTo(studentGender, new IntegerLiteral(1))
        );

        return conjuncts.size() == 3 && conjuncts.contains(or1) && conjuncts.contains(or2);
    }

    /**
     * (cid=1 and sage=10) or (cid=2 and cname='abc')
     * =>
     * cid=1 or (cid=2 and cname='abc')
     */
    @Test
    public void testExtract2() {

        Expression expr = new Or(
                new And(
                        new EqualTo(courseCid, new IntegerLiteral(1)),
                        new EqualTo(studentAge, new IntegerLiteral(10))
                ),
                new And(
                        new EqualTo(courseCid, new IntegerLiteral(2)),
                        new EqualTo(courseName, new StringLiteral("abc"))
                )
        );
        Plan join = new LogicalJoin<>(JoinType.CROSS_JOIN, student, course, null);
        LogicalFilter root = new LogicalFilter<>(ImmutableSet.of(expr), join);
        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new ExtractSingleTableExpressionFromDisjunction())
                .matchesFromRoot(
                        logicalFilter()
                                .when(filter -> verifySingleTableExpression2(filter.getConjuncts()))
                );
        Assertions.assertNotNull(studentGender);
    }

    private boolean verifySingleTableExpression2(Set<Expression> conjuncts) {
        Expression or1 = new Or(
                new EqualTo(courseCid, new IntegerLiteral(1)),
                new And(
                        new EqualTo(courseCid, new IntegerLiteral(2)),
                        new EqualTo(courseName, new StringLiteral("abc"))));

        return conjuncts.size() == 2 && conjuncts.contains(or1);
    }

    /**
     *(cid=1 and sage=10) or sgender=1
     * =>
     * (sage=10 or sgender=1)
     */

    @Test
    public void testExtract3() {
        Expression expr = new Or(
                new And(
                        new EqualTo(courseCid, new IntegerLiteral(1)),
                        new EqualTo(studentAge, new IntegerLiteral(10))
                ),
                new EqualTo(studentGender, new IntegerLiteral(1))
        );
        Plan join = new LogicalJoin<>(JoinType.CROSS_JOIN, student, course, null);
        LogicalFilter root = new LogicalFilter<>(ImmutableSet.of(expr), join);
        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyTopDown(new ExtractSingleTableExpressionFromDisjunction())
                .matchesFromRoot(
                        logicalFilter()
                                .when(filter -> verifySingleTableExpression3(filter.getConjuncts()))
                );
        Assertions.assertNotNull(studentGender);
    }

    private boolean verifySingleTableExpression3(Set<Expression> conjuncts) {
        Expression or = new Or(
                new EqualTo(studentAge, new IntegerLiteral(10)),
                new EqualTo(studentGender, new IntegerLiteral(1))
        );

        return conjuncts.size() == 2 && conjuncts.contains(or);
    }

    /**
     * test join otherJoinReorderContext
     *(cid=1 and sage=10) or sgender=1
     * =>
     * (sage=10 or sgender=1)
     */
    @Test
    public void testExtract4() {
        Expression expr = new Or(
                new And(
                        new EqualTo(courseCid, new IntegerLiteral(1)),
                        new EqualTo(studentAge, new IntegerLiteral(10))
                ),
                new EqualTo(studentGender, new IntegerLiteral(1))
        );
        Plan join = new LogicalJoin<>(JoinType.CROSS_JOIN, ExpressionUtils.EMPTY_CONDITION, ImmutableList.of(expr),
                student, course, null);
        PlanChecker.from(MemoTestUtils.createConnectContext(), join)
                .applyTopDown(new ExtractSingleTableExpressionFromDisjunction())
                .matchesFromRoot(
                        logicalJoin()
                                .when(j -> verifySingleTableExpression4(j.getOtherJoinConjuncts()))
                );
        Assertions.assertNotNull(studentGender);
    }

    private boolean verifySingleTableExpression4(List<Expression> conjuncts) {
        Expression or = new Or(
                new EqualTo(studentAge, new IntegerLiteral(10)),
                new EqualTo(studentGender, new IntegerLiteral(1))
        );
        return conjuncts.size() == 2 && conjuncts.contains(or);
    }
}
