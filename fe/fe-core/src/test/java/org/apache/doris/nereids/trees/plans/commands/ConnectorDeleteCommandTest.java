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

import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.IsTrue;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class ConnectorDeleteCommandTest {

    @Test
    void survivorPredicateKeepsFalseAndNullRows() {
        LogicalPlan child = unboundRelation();
        UnboundSlot deletePredicate = new UnboundSlot("should_delete");
        LogicalFilter<LogicalPlan> deleteQuery = new LogicalFilter<>(
                ImmutableSet.of(deletePredicate), child);

        LogicalFilter<?> survivorQuery = (LogicalFilter<?>)
                ConnectorDeleteCommand.buildSurvivorQuery(deleteQuery);

        Not survivorPredicate = (Not) survivorQuery.getConjuncts().iterator().next();
        Assertions.assertInstanceOf(IsTrue.class, survivorPredicate.child());
        Assertions.assertSame(deletePredicate, survivorPredicate.child().child(0));
        Assertions.assertSame(child, survivorQuery.child());
    }

    @Test
    void deleteWithoutPredicateProducesNoSurvivors() {
        LogicalPlan child = unboundRelation();

        LogicalFilter<?> survivorQuery = (LogicalFilter<?>)
                ConnectorDeleteCommand.buildSurvivorQuery(child);

        Assertions.assertEquals(
                ImmutableSet.of(BooleanLiteral.FALSE), survivorQuery.getConjuncts());
        Assertions.assertSame(child, survivorQuery.child());
    }

    @Test
    void partitionScopedDeleteIsRejected() {
        Assertions.assertDoesNotThrow(
                () -> ConnectorDeleteCommand.requireFullTableDelete(false, List.of()));
        Assertions.assertThrows(AnalysisException.class,
                () -> ConnectorDeleteCommand.requireFullTableDelete(
                        false, List.of("p20260822")));
        Assertions.assertThrows(AnalysisException.class,
                () -> ConnectorDeleteCommand.requireFullTableDelete(true, List.of()));
    }

    private static LogicalPlan unboundRelation() {
        return new UnboundRelation(
                StatementScopeIdGenerator.newRelationId(), List.of("events"));
    }
}
