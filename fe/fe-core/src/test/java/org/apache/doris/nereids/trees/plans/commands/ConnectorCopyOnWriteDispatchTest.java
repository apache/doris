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

import org.apache.doris.catalog.Env;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.write.ConnectorRowChangeStyle;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Copy-on-write admission must run before position-delete/changelog registry dispatch. */
class ConnectorCopyOnWriteDispatchTest {
    private static final List<String> TARGET = List.of("delta_catalog", "default", "events");

    @Test
    void deleteUsesCopyOnWriteAdmissionForRunAndExplain() {
        assertCopyOnWriteAdmission(new DeleteFromCommand(
                TARGET, null, false, List.of(), relation()), "DELETE", WriteOperation.UPDATE);
    }

    @Test
    void updateUsesCopyOnWriteAdmissionForRunAndExplain() {
        assertCopyOnWriteAdmission(new UpdateCommand(
                TARGET, null, List.of(), relation(), Optional.empty()), "UPDATE", WriteOperation.DELETE);
    }

    @Test
    void mergeUsesCopyOnWriteAdmissionForRunAndExplain() {
        assertCopyOnWriteAdmission(new MergeIntoCommand(
                TARGET, Optional.of("target"), Optional.empty(), relation(), BooleanLiteral.TRUE,
                List.of(), List.of()), "MERGE", WriteOperation.DELETE);
    }

    private static void assertCopyOnWriteAdmission(Command command, String operation, WriteOperation admitted) {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(context.getEnv()).thenReturn(env);
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getName()).thenReturn("events");
        Mockito.when(table.connectorSupportsCopyOnWriteDml()).thenReturn(true);
        Mockito.when(table.connectorSupportedWriteOperations()).thenReturn(Set.of(admitted));
        Mockito.when(table.getConnectorRowChangeStyle()).thenReturn(ConnectorRowChangeStyle.NONE);
        // The ordinary registry cannot serve this provider. A dispatcher that consults it before
        // the COW capability throws a registry error instead of the statement's admission error.
        Assertions.assertThrows(AnalysisException.class, () -> RowLevelDmlRegistry.find(table));
        try (MockedStatic<RelationUtil> relations = Mockito.mockStatic(RelationUtil.class)) {
            relations.when(() -> RelationUtil.getQualifierName(context, TARGET)).thenReturn(TARGET);
            relations.when(() -> RelationUtil.getTable(TARGET, env, Optional.empty())).thenReturn(table);
            String expected = "Connector does not support " + operation + " for table: events";
            AnalysisException runError = Assertions.assertThrows(AnalysisException.class,
                    () -> command.run(context, Mockito.mock(StmtExecutor.class)));
            Assertions.assertEquals(expected, runError.getMessage());
            AnalysisException explainError = Assertions.assertThrows(AnalysisException.class,
                    () -> ((Explainable) command).getExplainPlan(context));
            Assertions.assertEquals(expected, explainError.getMessage());
        }
    }

    private static LogicalPlan relation() {
        return new UnboundRelation(StatementScopeIdGenerator.newRelationId(), TARGET);
    }
}
