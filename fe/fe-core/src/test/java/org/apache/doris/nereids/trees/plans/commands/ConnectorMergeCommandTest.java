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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeMatchedClause;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeNotMatchedClause;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

class ConnectorMergeCommandTest {

    @Test
    void affectedRowSqlUsesPinnedTargetAndClausePredicates() {
        EqualTo on = new EqualTo(new UnboundSlot("t", "id"), new UnboundSlot("s", "id"));
        EqualTo deletePredicate = new EqualTo(
                new UnboundSlot("s", "action"), new VarcharLiteral("D"));
        EqualTo insertPredicate = new EqualTo(
                new UnboundSlot("s", "action"), new VarcharLiteral("I"));
        ConnectorMergeCommand command = new ConnectorMergeCommand(
                List.of("delta_catalog", "default", "events"), Optional.of("t"),
                Optional.empty(), sourceRelation(), on,
                List.of(new MergeMatchedClause(Optional.of(deletePredicate), List.of(), true),
                        new MergeMatchedClause(Optional.empty(), List.of(
                                new EqualTo(new UnboundSlot("payload"),
                                        new UnboundSlot("s", "payload"))), false)),
                List.of(new MergeNotMatchedClause(Optional.of(insertPredicate),
                        List.of("id", "payload"), List.of(
                                new UnboundAlias(new UnboundSlot("s", "id")),
                                new UnboundAlias(new UnboundSlot("s", "payload"))))));
        ConnectorMergeCommand.SourceInfo sourceInfo = new ConnectorMergeCommand.SourceInfo(
                Mockito.mock(OlapTable.class),
                List.of("regression_test", "merge_source"), "s");

        String sql = command.buildCountSql(7L, sourceInfo);

        Assertions.assertTrue(sql.contains("FOR VERSION AS OF 7"));
        Assertions.assertTrue(sql.contains("FULL OUTER JOIN"));
        Assertions.assertTrue(sql.contains("s.action"));
        Assertions.assertDoesNotThrow(() -> new NereidsParser().parseSingle(sql));
    }

    @Test
    void branchExpressionIsTotalForMatchedAndNotMatchedRows() {
        Expression on = new EqualTo(
                new UnboundSlot("t", "id"), new UnboundSlot("s", "id"));
        ConnectorMergeCommand command = new ConnectorMergeCommand(
                List.of("delta_catalog", "default", "events"), Optional.of("t"),
                Optional.empty(), sourceRelation(), on,
                List.of(new MergeMatchedClause(Optional.empty(), List.of(
                        new EqualTo(new UnboundSlot("payload"),
                                new UnboundSlot("s", "payload"))), false)),
                List.of(new MergeNotMatchedClause(Optional.empty(),
                        List.of("id", "payload"), List.of(
                                new UnboundAlias(new IntegerLiteral(1)),
                                new UnboundAlias(new VarcharLiteral("inserted"))))));

        Assertions.assertNotNull(command.buildBranchExpression());
        Assertions.assertTrue(command.buildBranchExpression().toSql().contains(
                "__DORIS_CONNECTOR_MERGE_TARGET_PRESENT__"));
    }

    @Test
    void sourceScanModifiersAreRejectedBecauseCountQueryCannotPreserveThem() {
        Assertions.assertDoesNotThrow(
                () -> ConnectorMergeCommand.requirePlainSourceRelation(sourceRelation()));
        UnboundRelation partitionScan = new UnboundRelation(
                StatementScopeIdGenerator.newRelationId(),
                List.of("regression_test", "merge_source"), List.of("p1"), false);

        Assertions.assertThrows(AnalysisException.class,
                () -> ConnectorMergeCommand.requirePlainSourceRelation(partitionScan));
    }

    private static UnboundRelation sourceRelation() {
        return new UnboundRelation(StatementScopeIdGenerator.newRelationId(),
                List.of("regression_test", "merge_source"));
    }

}
