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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.connector.spi.write.ConnectorChangelogMode;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.commands.info.ConnectorChangelogRowChangeSpec;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoTestUtils;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class ConnectorChangelogPlanBuilderTest {
    private static final ConnectorChangelogMode MODE =
            new ConnectorChangelogMode("connector_operation", (byte) 4, (byte) 6, (byte) 8);
    private static final List<Column> SCHEMA = ImmutableList.of(
            new Column("id", ScalarType.createType(PrimitiveType.INT)),
            new Column("value", ScalarType.createType(PrimitiveType.INT)));

    @Test
    void updateUsesConnectorOwnedOperationEncoding() {
        LogicalPlan child = targetRow();
        CascadesContext context = MemoTestUtils.createCascadesContext(child);
        ConnectorChangelogRowChangeSpec.Update spec = new ConnectorChangelogRowChangeSpec.Update(
                ImmutableList.of("target"), ImmutableList.of(
                        new EqualTo(new UnboundSlot("value"), new IntegerLiteral(99))));

        LogicalPlan result = ConnectorChangelogPlanBuilder.build(
                SCHEMA, ImmutableList.of("id"), MODE, spec, child, context);

        Assertions.assertInstanceOf(LogicalProject.class, result);
        Assertions.assertEquals(ImmutableList.of("connector_operation", "id", "value"),
                result.getOutput().stream().map(NamedExpression::getName)
                        .collect(ImmutableList.toImmutableList()));
        Assertions.assertTrue(((LogicalProject<?>) result).getProjects().get(0).toSql().contains("6"));
        Assertions.assertTrue(((LogicalProject<?>) result).getProjects().get(2).toSql().contains("99"));
    }

    @Test
    void deleteUsingDeduplicatesByConnectorPrimaryKey() {
        LogicalPlan child = targetRow();
        CascadesContext context = MemoTestUtils.createCascadesContext(child);
        ConnectorChangelogRowChangeSpec.Delete spec = new ConnectorChangelogRowChangeSpec.Delete(
                ImmutableList.of("target"), true);

        LogicalPlan result = ConnectorChangelogPlanBuilder.build(
                SCHEMA, ImmutableList.of("id"), MODE, spec, child, context);

        Assertions.assertInstanceOf(LogicalAggregate.class, result);
        Assertions.assertEquals(ImmutableList.of("connector_operation", "id", "value"),
                result.getOutput().stream().map(NamedExpression::getName)
                        .collect(ImmutableList.toImmutableList()));
    }

    private LogicalPlan targetRow() {
        SlotReference id = new SlotReference("id", IntegerType.INSTANCE, false,
                ImmutableList.of("target"));
        SlotReference value = new SlotReference("value", IntegerType.INSTANCE, true,
                ImmutableList.of("target"));
        return new LogicalEmptyRelation(new RelationId(1), ImmutableList.of(id, value));
    }
}
