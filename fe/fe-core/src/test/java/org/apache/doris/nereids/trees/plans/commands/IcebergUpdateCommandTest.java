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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.iceberg.IcebergMergeOperation;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.commands.delete.DeleteCommandContext;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class IcebergUpdateCommandTest {

    @BeforeAll
    public static void setUp() {
        FeConstants.runningUnitTest = true;
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    @Test
    public void testBuildMergeProjectPlanProjectsRowId() {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
        LogicalPlan basePlan = new LogicalOneRowRelation(new RelationId(0),
                ImmutableList.of(new UnboundAlias(new IntegerLiteral(1), "dummy")));
        IcebergUpdateCommand command = new IcebergUpdateCommand(
                ImmutableList.of("test_catalog", "test_db", "test_table"),
                null,
                ImmutableList.of(),
                basePlan,
                new DeleteCommandContext());

        List<Column> columns = ImmutableList.of(new Column("c1", ScalarType.createType(PrimitiveType.INT)));
        LogicalPlan plan = command.buildMergeProjectPlan(ctx, basePlan, ImmutableList.of(), columns, "t");
        Assertions.assertTrue(plan instanceof LogicalProject);
        List<NamedExpression> projects = ((LogicalProject<?>) plan).getProjects();
        Assertions.assertEquals(3, projects.size());
        boolean hasOperation = false;
        boolean hasRowId = false;
        boolean hasC1 = false;
        for (NamedExpression project : projects) {
            if (project instanceof UnboundAlias
                    && project.toString().contains(IcebergMergeOperation.OPERATION_COLUMN)) {
                hasOperation = true;
            }
            if (project instanceof UnboundSlot
                    && Column.ICEBERG_ROWID_COL.equalsIgnoreCase(
                            ((UnboundSlot) project).getNameParts().get(0))) {
                hasRowId = true;
            }
            if (project instanceof UnboundSlot
                    && "c1".equalsIgnoreCase(((UnboundSlot) project).getNameParts().get(
                    ((UnboundSlot) project).getNameParts().size() - 1))) {
                hasC1 = true;
            }
        }
        Assertions.assertTrue(hasOperation);
        Assertions.assertTrue(hasRowId);
        Assertions.assertTrue(hasC1);
    }

    @Test
    public void testBuildUpdateSelectItemsSkipsHiddenColumns() {
        IcebergUpdateCommand command = new IcebergUpdateCommand(
                ImmutableList.of("test_catalog", "test_db", "test_table"),
                "t",
                ImmutableList.of(),
                new LogicalOneRowRelation(new RelationId(1),
                        ImmutableList.of(new UnboundAlias(new IntegerLiteral(1), "dummy"))),
                new DeleteCommandContext());

        Map<String, org.apache.doris.nereids.trees.expressions.Expression> assignments =
                new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        assignments.put("c1", new IntegerLiteral(1));

        Column col1 = new Column("c1", ScalarType.createType(PrimitiveType.INT));
        Column hidden = new Column(Column.ICEBERG_ROWID_COL, ScalarType.createStringType());
        hidden.setIsVisible(false);
        Column col2 = new Column("c2", ScalarType.createType(PrimitiveType.INT));
        List<Column> columns = Arrays.asList(col1, hidden, col2);

        List<NamedExpression> selectItems = command.buildUpdateSelectItems(assignments, columns, "t");
        Assertions.assertEquals(2, selectItems.size());

        boolean hasRowId = selectItems.stream()
                .filter(UnboundSlot.class::isInstance)
                .map(UnboundSlot.class::cast)
                .anyMatch(slot -> Column.ICEBERG_ROWID_COL.equals(slot.getNameParts().get(0)));
        Assertions.assertFalse(hasRowId);

        boolean hasC2Slot = selectItems.stream()
                .filter(UnboundSlot.class::isInstance)
                .map(UnboundSlot.class::cast)
                .anyMatch(slot -> "c2".equalsIgnoreCase(
                        slot.getNameParts().get(slot.getNameParts().size() - 1)));
        Assertions.assertTrue(hasC2Slot);
    }
}
