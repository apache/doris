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

import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.JoinHint;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * initial plan:
 *     join
 *        -hashJoinConjuncts={}
 *        -otherJoinCondition=
 *          "A.x=B.x and A.y+1=B.y and A.x=1 and (A.y=B.y or B.x=A.x) and A.x>B.x"
 * after transform
 *    join
 *       -hashJoinConjuncts={A.x=B.x, A.y+1=B.y}
 *       -otherJoinCondition="A.x=1 and (A.x=1 or B.x=A.x) and A.x>B.x"
 */
class FindHashConditionForJoinTest implements MemoPatternMatchSupported {
    @Test
    void testFindHashCondition() {
        Plan student = new LogicalOlapScan(PlanConstructor.getNextRelationId(), PlanConstructor.student,
                ImmutableList.of(""));
        Plan score = new LogicalOlapScan(PlanConstructor.getNextRelationId(), PlanConstructor.score,
                ImmutableList.of(""));

        Slot studentId = student.getOutput().get(0);
        Slot gender = student.getOutput().get(1);
        Slot scoreId = score.getOutput().get(0);
        Slot cid = score.getOutput().get(1);

        Expression eq1 = new EqualTo(studentId, scoreId); // a=b
        Expression eq2 = new EqualTo(studentId, new IntegerLiteral(1)); // a=1
        Expression eq3 = new EqualTo(new Add(studentId, new IntegerLiteral(1)), cid);
        Expression or = new Or(
                new EqualTo(scoreId, studentId),
                new EqualTo(gender, cid));
        Expression less = new LessThan(scoreId, studentId);
        List<Expression> expr = ImmutableList.of(eq1, eq2, eq3, or, less);
        LogicalJoin join = new LogicalJoin<>(JoinType.INNER_JOIN, new ArrayList<>(),
                expr, JoinHint.NONE, Optional.empty(), student, score);

        PlanChecker.from(new ConnectContext(), join)
                        .applyTopDown(new FindHashConditionForJoin())
                        .matches(
                            logicalJoin()
                                    .when(j -> j.getHashJoinConjuncts().equals(ImmutableList.of(eq1, eq3)))
                                    .when(j -> j.getOtherJoinConjuncts().equals(ImmutableList.of(eq2, or, less))));
    }
}
