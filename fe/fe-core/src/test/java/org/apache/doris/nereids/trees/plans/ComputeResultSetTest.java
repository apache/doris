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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimestampTzLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral;
import org.apache.doris.nereids.trees.plans.physical.PhysicalBlackholeSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalResultSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSqlCache;
import org.apache.doris.qe.CommonResultSet;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.thrift.TUniqueId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

class ComputeResultSetTest {
    @Test
    void testLiteralEligibilityDoesNotFormatTimestamps() {
        PhysicalOneRowRelation relation = oneRow(List.of(
                new Alias(new TimestampTzLiteral(9999, 12, 31, 23, 59, 59), "ts")));
        Assertions.assertTrue(relation.canComputeResultInFe(relation.getOutput()));
    }

    @Test
    void testOneRowEligibilityPreservesBackendFallbacks() {
        for (NamedExpression output : List.of(new Alias(new VarBinaryLiteral("FF"), "binary_value"),
                new Alias(new Add(new IntegerLiteral(1), new IntegerLiteral(2)), "unfolded"))) {
            PhysicalOneRowRelation relation = oneRow(List.of(output));
            Assertions.assertFalse(relation.canComputeResultInFe(relation.getOutput()));
            Assertions.assertFalse(relation.computeResultInFe(null, relation.getOutput()).isPresent());
        }
    }

    @Test
    void testOneRowEligibilityChecksOnlyRequestedOutputs() {
        NamedExpression selected = new Alias(new IntegerLiteral(1), "selected");
        PhysicalOneRowRelation relation = oneRow(List.of(selected,
                new Alias(new VarBinaryLiteral("FF"), "unused_binary"),
                new Alias(new Add(new IntegerLiteral(1), new IntegerLiteral(2)), "unused_unfolded")));
        Assertions.assertFalse(relation.canComputeResultInFe(relation.getOutput()));
        Assertions.assertTrue(relation.canComputeResultInFe(List.of(selected.toSlot())));
        CascadesContext context = Mockito.mock(CascadesContext.class);
        Mockito.when(context.getStatementContext()).thenReturn(new StatementContext());
        Assertions.assertEquals(List.of(List.of("1")), relation.computeResultInFe(
                context, List.of(selected.toSlot())).get().getResultRows());
    }

    @Test
    void testSinkEligibilityDelegatesToChild() {
        for (NamedExpression output : List.of(new Alias(new IntegerLiteral(1), "number_value"),
                new Alias(new VarBinaryLiteral("FF"), "binary_value"))) {
            PhysicalOneRowRelation relation = oneRow(List.of(output));
            PhysicalResultSink<PhysicalOneRowRelation> sink = new PhysicalResultSink<>(
                    relation.getProjects(), Optional.empty(), relation.getLogicalProperties(), relation);
            PhysicalBlackholeSink<PhysicalOneRowRelation> blackhole = new PhysicalBlackholeSink<>(
                    relation.getProjects(), Optional.empty(), relation.getLogicalProperties(), relation);
            boolean eligible = relation.canComputeResultInFe(relation.getOutput());
            Assertions.assertEquals(eligible, sink.canComputeResultInFe(relation.getOutput()));
            Assertions.assertEquals(eligible, blackhole.canComputeResultInFe(relation.getOutput()));
        }
        LogicalProperties properties = properties(List.of());
        PhysicalPlan child = new PhysicalProject<>(List.of(), properties, oneRow(List.of()));
        Assertions.assertFalse(new PhysicalResultSink<>(List.of(), Optional.empty(), properties, child)
                .canComputeResultInFe(List.of()));
        Assertions.assertFalse(new PhysicalBlackholeSink<>(List.of(), Optional.empty(), properties, child)
                .canComputeResultInFe(List.of()));
    }

    @Test
    void testEmptyRelationDoesNotNeedToFormatOutputs() {
        List<NamedExpression> projects = List.of(new Alias(new VarBinaryLiteral("FF"), "binary_value"));
        PhysicalEmptyRelation relation = new PhysicalEmptyRelation(new RelationId(1), projects, properties(projects));
        Assertions.assertTrue(relation.canComputeResultInFe(relation.getOutput()));
        Assertions.assertTrue(relation.computeResultInFe(null, relation.getOutput()).isPresent());
    }

    @Test
    void testSqlCacheEligibilityRequiresFeResult() {
        ResultSet resultSet = new CommonResultSet(new CommonResultSet.CommonResultSetMetaData(List.of()), List.of());
        for (Optional<ResultSet> result : List.of(Optional.of(resultSet), Optional.<ResultSet>empty())) {
            PhysicalSqlCache cache = new PhysicalSqlCache(new TUniqueId(), List.of(), List.of(), List.of(),
                    result, List.of(), "", "");
            Assertions.assertEquals(result.isPresent(), cache.canComputeResultInFe(List.of()));
            Assertions.assertEquals(result, cache.computeResultInFe(null, List.of()));
        }
    }

    private static PhysicalOneRowRelation oneRow(List<NamedExpression> projects) {
        return new PhysicalOneRowRelation(new RelationId(1), projects, properties(projects));
    }

    private static LogicalProperties properties(List<NamedExpression> projects) {
        return new LogicalProperties(() -> projects.stream().map(NamedExpression::toSlot).collect(Collectors.toList()),
                () -> DataTrait.EMPTY_TRAIT);
    }
}
