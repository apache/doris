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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeMatchedClause;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeNotMatchedClause;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

class PaimonMergeCommandTest {
    @Test
    void matchedOnlyMergeKeepsTargetOnLeft() {
        assertMergeJoin(false, JoinType.INNER_JOIN);
    }

    @Test
    void mergeWithInsertPreservesSourceOnRight() {
        assertMergeJoin(true, JoinType.RIGHT_OUTER_JOIN);
    }

    private void assertMergeJoin(boolean hasInsert, JoinType expectedType) {
        LogicalPlan source = new LogicalEmptyRelation(new RelationId(1), ImmutableList.of());
        PaimonMergeCommand command = new PaimonMergeCommand(ImmutableList.of("target"),
                Optional.of("t"), Optional.empty(), source, BooleanLiteral.TRUE,
                ImmutableList.of(new MergeMatchedClause(Optional.empty(), ImmutableList.of(), true)),
                hasInsert ? ImmutableList.of(new MergeNotMatchedClause(Optional.empty(),
                        ImmutableList.of(), ImmutableList.of())) : ImmutableList.of());
        LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) command.getExplainPlan(new ConnectContext()).child(0);

        Assertions.assertEquals(expectedType, join.getJoinType());
        Assertions.assertSame(source, join.right());
        Assertions.assertInstanceOf(LogicalSubQueryAlias.class, join.left());
        Assertions.assertEquals("t", ((LogicalSubQueryAlias<?>) join.left()).getAlias());
        Assertions.assertEquals(ImmutableList.of(BooleanLiteral.TRUE), join.getOtherJoinConjuncts());
    }
}
