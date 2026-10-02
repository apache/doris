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
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.IsTrue;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

class ConnectorUpdateCommandTest {

    @Test
    void updateProjectionPreservesUnmatchedRows() {
        LogicalPlan relation = unboundRelation();
        EqualTo predicate = new EqualTo(new UnboundSlot("e", "id"), new IntegerLiteral(10));
        LogicalFilter<LogicalPlan> filtered = new LogicalFilter<>(
                ImmutableSet.of(predicate), relation);
        ConnectorUpdateCommand command = new ConnectorUpdateCommand(
                List.of("delta_catalog", "default", "events"), "e",
                List.of(new EqualTo(new UnboundSlot("e", "payload"),
                        new VarcharLiteral("updated"))), filtered, Optional.empty());
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Column id = column("id");
        Column payload = column("payload");
        Mockito.when(table.getBaseSchema(true)).thenReturn(List.of(id, payload));
        Mockito.when(table.getName()).thenReturn("events");

        ConnectorUpdateCommand.UpdatePlan updatePlan = command.buildUpdatePlan(
                Mockito.mock(ConnectContext.class), table);

        LogicalProject<?> project = (LogicalProject<?>) updatePlan.getQuery();
        Assertions.assertSame(relation, project.child());
        Assertions.assertEquals(2, project.getProjects().size());
        Assertions.assertInstanceOf(UnboundSlot.class, project.getProjects().get(0));
        UnboundAlias updatedPayload = (UnboundAlias) project.getProjects().get(1);
        Assertions.assertInstanceOf(If.class, updatedPayload.child());
        Assertions.assertInstanceOf(IsTrue.class, updatedPayload.child().child(0));
        Assertions.assertEquals(Optional.of(predicate), updatePlan.getPredicate());
    }

    @Test
    void affectedRowSqlPinsVersionAndPreservesAlias() {
        ConnectorUpdateCommand command = new ConnectorUpdateCommand(
                List.of("delta_catalog", "default", "events"), "e", List.of(),
                unboundRelation(), Optional.empty());
        EqualTo predicate = new EqualTo(
                new UnboundSlot("e", "id"), new IntegerLiteral(10));

        String sql = command.buildCountSql(
                7L, Optional.of(predicate));

        Assertions.assertTrue(sql.contains("FOR VERSION AS OF 7"));
        Assertions.assertTrue(sql.contains(" AS `e` WHERE "));
        Assertions.assertDoesNotThrow(() -> new NereidsParser().parseSingle(sql));
    }

    @Test
    void updateRequiresSimpleShape() {
        LogicalPlan relation = unboundRelation();
        Assertions.assertDoesNotThrow(() -> ConnectorUpdateCommand.requireSimpleUpdateShape(
                relation, Optional.empty()));
        Assertions.assertThrows(AnalysisException.class,
                () -> ConnectorUpdateCommand.requireSimpleUpdateShape(
                        relation, Optional.of(unboundRelation())));
    }

    private static Column column(String name) {
        Column column = Mockito.mock(Column.class);
        Mockito.when(column.getName()).thenReturn(name);
        Mockito.when(column.isVisible()).thenReturn(true);
        return column;
    }

    private static LogicalPlan unboundRelation() {
        return new UnboundRelation(
                StatementScopeIdGenerator.newRelationId(),
                List.of("delta_catalog", "default", "events"));
    }

}
