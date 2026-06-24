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

package org.apache.doris.connector.iceberg.action;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.procedure.ConnectorProcedureResult;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;
import org.apache.doris.foundation.util.ArgumentParsers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Pins the connector base for iceberg {@code ALTER TABLE EXECUTE} actions, the standalone port of legacy
 * {@code BaseIcebergAction} + the consumed half of {@code BaseExecuteAction}.
 *
 * <p><b>WHY this matters:</b> the base owns the machinery every procedure shares — argument validation
 * delegation, the {@code validateIcebergAction()} hook, the partition/{@code WHERE} guards, and the
 * <em>single-row result contract</em> ({@code resultSchema.size() == row.size()}, legacy
 * {@code BaseExecuteAction:106-108}). It must reproduce that contract and the legacy guard messages under
 * the SPI types ({@code List<String>} partitions, {@link ConnectorPredicate} where,
 * {@link ConnectorColumn} schema). The {@code ALTER} privilege check is intentionally absent — the engine
 * keeps it (D-062 §2). Exercised through a fake subclass; no SDK table needed.</p>
 */
public class BaseIcebergActionTest {

    private static final ConnectorPredicate ANY_WHERE =
            new ConnectorPredicate(new ConnectorLiteral(ConnectorType.of("BOOLEAN"), Boolean.TRUE));

    /** A 2-column BIGINT schema captured at construction (mirrors rollback_to_snapshot's shape). */
    private static List<ConnectorColumn> twoBigintSchema() {
        return ImmutableList.of(
                new ConnectorColumn("previous_snapshot_id", ConnectorType.of("BIGINT"), null, false, null),
                new ConnectorColumn("current_snapshot_id", ConnectorType.of("BIGINT"), null, false, null));
    }

    /**
     * Fake action over the base. {@code registerIcebergArguments}/{@code getResultSchema} run during the
     * super constructor, so they read no instance fields; {@code validateIcebergAction}/{@code executeAction}
     * run later and may.
     */
    private static final class FakeAction extends BaseIcebergAction {
        private final List<String> row;
        private final boolean rejectPartitions;
        private final boolean rejectWhere;
        private final boolean requirePartitions;
        private final boolean requireWhere;

        FakeAction(Map<String, String> properties, List<String> partitionNames, ConnectorPredicate where,
                List<String> row, boolean rejectPartitions, boolean rejectWhere,
                boolean requirePartitions, boolean requireWhere) {
            super("fake", properties, partitionNames, where);
            this.row = row;
            this.rejectPartitions = rejectPartitions;
            this.rejectWhere = rejectWhere;
            this.requirePartitions = requirePartitions;
            this.requireWhere = requireWhere;
        }

        @Override
        protected void registerIcebergArguments() {
            namedArguments.registerRequiredArgument("snapshot_id", "Snapshot ID",
                    ArgumentParsers.positiveLong("snapshot_id"));
        }

        @Override
        protected void validateIcebergAction() {
            if (rejectPartitions) {
                validateNoPartitions();
            }
            if (rejectWhere) {
                validateNoWhereCondition();
            }
            if (requirePartitions) {
                validateRequiredPartitions();
            }
            if (requireWhere) {
                validateRequiredWhereCondition();
            }
        }

        @Override
        protected List<ConnectorColumn> getResultSchema() {
            return twoBigintSchema();
        }

        @Override
        protected List<String> executeAction(Table table, ConnectorSession session) {
            return row;
        }
    }

    private static FakeAction action(Map<String, String> props, List<String> partitions,
            ConnectorPredicate where, List<String> row) {
        return new FakeAction(props, partitions, where, row, false, false, false, false);
    }

    @Test
    public void validateDelegatesArgumentValidationToNamedArguments() {
        FakeAction a = action(ImmutableMap.of("snapshot_id", "1", "bogus", "2"),
                Collections.emptyList(), null, ImmutableList.of("a", "b"));
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class, a::validate);
        Assertions.assertEquals("Unknown argument: bogus", e.getMessage());
    }

    @Test
    public void validateAcceptsValidArgumentsWithoutPartitionsOrWhere() {
        FakeAction a = action(ImmutableMap.of("snapshot_id", "1"),
                Collections.emptyList(), null, ImmutableList.of("a", "b"));
        Assertions.assertDoesNotThrow(a::validate);
    }

    @Test
    public void validateNoPartitionsGuardRejectsWithLegacyMessage() {
        FakeAction a = new FakeAction(ImmutableMap.of("snapshot_id", "1"),
                ImmutableList.of("p1"), null, ImmutableList.of("a", "b"),
                true, false, false, false);
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class, a::validate);
        Assertions.assertEquals("Action 'fake' does not support partition specification", e.getMessage());
    }

    @Test
    public void validateNoWhereGuardRejectsWithLegacyMessage() {
        FakeAction a = new FakeAction(ImmutableMap.of("snapshot_id", "1"),
                Collections.emptyList(), ANY_WHERE, ImmutableList.of("a", "b"),
                false, true, false, false);
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class, a::validate);
        Assertions.assertEquals("Action 'fake' does not support WHERE condition", e.getMessage());
    }

    @Test
    public void validateRequiredPartitionsGuardRejectsWithLegacyMessage() {
        FakeAction a = new FakeAction(ImmutableMap.of("snapshot_id", "1"),
                Collections.emptyList(), null, ImmutableList.of("a", "b"),
                false, false, true, false);
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class, a::validate);
        Assertions.assertEquals("Action 'fake' requires partition specification", e.getMessage());
    }

    @Test
    public void validateRequiredWhereGuardRejectsWithLegacyMessage() {
        FakeAction a = new FakeAction(ImmutableMap.of("snapshot_id", "1"),
                Collections.emptyList(), null, ImmutableList.of("a", "b"),
                false, false, false, true);
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class, a::validate);
        Assertions.assertEquals("Action 'fake' requires WHERE condition", e.getMessage());
    }

    @Test
    public void executeWrapsExactlyOneRowMatchingSchemaWidth() {
        FakeAction a = action(ImmutableMap.of("snapshot_id", "1"),
                Collections.emptyList(), null, ImmutableList.of("10", "20"));

        ConnectorProcedureResult result = a.execute((Table) null, null);

        Assertions.assertEquals(2, result.getResultSchema().size());
        Assertions.assertEquals("previous_snapshot_id", result.getResultSchema().get(0).getName());
        Assertions.assertEquals(1, result.getRows().size(), "a procedure emits exactly one row");
        Assertions.assertEquals(ImmutableList.of("10", "20"), result.getRows().get(0));
    }

    @Test
    public void executeFailsLoudWhenRowWidthDoesNotMatchSchema() {
        // Schema is 2 columns but the body returns 1 value: the single-row contract guard must fire.
        FakeAction a = action(ImmutableMap.of("snapshot_id", "1"),
                Collections.emptyList(), null, ImmutableList.of("only-one"));
        Assertions.assertThrows(IllegalStateException.class, () -> a.execute((Table) null, null));
    }
}
