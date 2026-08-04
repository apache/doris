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

package org.apache.doris.connector;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContractValidator;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.write.ConnectorWritePlanProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.EnumSet;

/**
 * Rule-9 behavior gates for {@link ConnectorContractValidator}: it must fail loud
 * ({@link IllegalStateException}) when a connector's own delegators are internally inconsistent, and it
 * must pass silently when they are not. These are the primary enforcement of the two structural invariants
 * (static per-connector properties, checked here and in each connector's own contract test rather than at
 * catalog registration). The traits are stubbed on a fake {@link ConnectorWritePlanProvider} — the interface
 * that owns them — behind a fake {@link Connector} that hands it out, which is exactly how the validator and
 * the engine reach them.
 */
public class ConnectorContractValidatorTest {

    @Test
    void validatorRejectsBranchWithoutInsert() {
        // Invariant #2: supportsWriteBranch() implies supportedWriteOperations() contains INSERT (a
        // branch write is an INSERT modifier, never a capability on its own). A connector claiming
        // branch support with no declared INSERT is self-contradictory -> must fail loud at registration
        // instead of surfacing as a confusing failure the first time someone writes to a branch.
        // MUTATION: dropping the `!` in the validator's #2 check makes this test go red (see task report).
        ConnectorWritePlanProvider provider = writeProvider();
        Connector fake = connectorWith(provider);
        Mockito.when(provider.supportsWriteBranch()).thenReturn(true);
        Mockito.when(provider.supportedOperations()).thenReturn(EnumSet.noneOf(WriteOperation.class));

        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class,
                () -> ConnectorContractValidator.validate(fake, "fake_branch_no_insert"));
        Assertions.assertTrue(ex.getMessage().contains("supportsWriteBranch"), "got: " + ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("fake_branch_no_insert"), "got: " + ex.getMessage());
    }

    @Test
    void validatorRejectsLocalSortWithoutParallelAndFullSchema() {
        // Invariant #3: requiresPartitionLocalSort() implies BOTH requiresParallelWrite() AND
        // requiresFullSchemaWriteOrder() — the local-sort write plan hash-distributes by partition
        // columns and depends on full-schema positional output, so declaring local-sort without the
        // other two is self-contradictory and must fail loud rather than silently mis-plan the sink
        // distribution (PhysicalConnectorTableSink.getRequirePhysicalProperties reads these).
        ConnectorWritePlanProvider provider = writeProvider();
        Connector fake = connectorWith(provider);
        Mockito.when(provider.requiresPartitionLocalSort()).thenReturn(true);
        Mockito.when(provider.requiresParallelWrite()).thenReturn(false);
        Mockito.when(provider.requiresFullSchemaWriteOrder()).thenReturn(true);

        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class,
                () -> ConnectorContractValidator.validate(fake, "fake_localsort_no_parallel"));
        Assertions.assertTrue(ex.getMessage().contains("requiresPartitionLocalSort"), "got: " + ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("fake_localsort_no_parallel"), "got: " + ex.getMessage());
    }

    @Test
    void validatorRejectsLocalSortWithoutFullSchema() {
        // Invariant #3, the OTHER half: local-sort with parallel write but WITHOUT full-schema write order is
        // equally self-contradictory. This is the distinguishing input (localSort=T, parallel=T, fullSchema=F)
        // that validatorRejectsLocalSortWithoutParallelAndFullSchema cannot exercise (it fixes parallel=F). A
        // mutant dropping the `&& requiresFullSchemaWriteOrder()` conjunct still throws on that other case but
        // NOT here, so this test is what actually kills that mutation — both conjuncts of #3 are now covered.
        ConnectorWritePlanProvider provider = writeProvider();
        Connector fake = connectorWith(provider);
        Mockito.when(provider.requiresPartitionLocalSort()).thenReturn(true);
        Mockito.when(provider.requiresParallelWrite()).thenReturn(true);
        Mockito.when(provider.requiresFullSchemaWriteOrder()).thenReturn(false);

        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class,
                () -> ConnectorContractValidator.validate(fake, "fake_localsort_no_fullschema"));
        Assertions.assertTrue(ex.getMessage().contains("requiresPartitionLocalSort"), "got: " + ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("fake_localsort_no_fullschema"), "got: " + ex.getMessage());
    }

    @Test
    void validatorRejectsHashWriteWithoutParallelAndFullSchema() {
        // Invariant #4: requiresPartitionHashWrite() (hash-by-partition without a local sort) likewise
        // implies BOTH requiresParallelWrite() AND requiresFullSchemaWriteOrder() — the hash arm in
        // PhysicalConnectorTableSink indexes partition columns by full-schema position and distributes in
        // parallel, so declaring hash-write without the other two must fail loud, not silently mis-plan.
        ConnectorWritePlanProvider provider = writeProvider();
        Connector fake = connectorWith(provider);
        Mockito.when(provider.requiresPartitionHashWrite()).thenReturn(true);
        Mockito.when(provider.requiresParallelWrite()).thenReturn(false);
        Mockito.when(provider.requiresFullSchemaWriteOrder()).thenReturn(true);

        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class,
                () -> ConnectorContractValidator.validate(fake, "fake_hash_no_parallel"));
        Assertions.assertTrue(ex.getMessage().contains("requiresPartitionHashWrite"), "got: " + ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("fake_hash_no_parallel"), "got: " + ex.getMessage());
    }

    @Test
    void validatorRejectsBothPartitionDistributionArms() {
        // Invariant #5: the two hash arms are mutually exclusive. PhysicalConnectorTableSink checks
        // requirePartitionLocalSortOnWrite() BEFORE requirePartitionHashOnWrite(), so a connector declaring
        // both would silently get the local-sort arm and never the hash-without-sort it asked for. That is a
        // misconfiguration, so it must fail loud at registration. Both are otherwise internally consistent
        // (parallel + full-schema) to isolate the mutual-exclusion check as the sole reason for the throw.
        ConnectorWritePlanProvider provider = writeProvider();
        Connector fake = connectorWith(provider);
        Mockito.when(provider.requiresParallelWrite()).thenReturn(true);
        Mockito.when(provider.requiresFullSchemaWriteOrder()).thenReturn(true);
        Mockito.when(provider.requiresPartitionLocalSort()).thenReturn(true);
        Mockito.when(provider.requiresPartitionHashWrite()).thenReturn(true);

        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class,
                () -> ConnectorContractValidator.validate(fake, "fake_both_arms"));
        Assertions.assertTrue(ex.getMessage().contains("requiresPartitionHashWrite"), "got: " + ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("fake_both_arms"), "got: " + ex.getMessage());
    }

    @Test
    void validatorPassesForAHashWriteConnector() {
        // Positive control (Rule 9) for the hive-shaped connector: parallel write + full-schema write order +
        // hash-write (no local sort), INSERT/OVERWRITE, no branch — internally consistent, must NOT throw.
        ConnectorWritePlanProvider provider = writeProvider();
        Connector fake = connectorWith(provider);
        Mockito.when(provider.supportedOperations())
                .thenReturn(EnumSet.of(WriteOperation.INSERT, WriteOperation.OVERWRITE));
        Mockito.when(provider.supportsWriteBranch()).thenReturn(false);
        Mockito.when(provider.requiresParallelWrite()).thenReturn(true);
        Mockito.when(provider.requiresFullSchemaWriteOrder()).thenReturn(true);
        Mockito.when(provider.requiresPartitionHashWrite()).thenReturn(true);

        Assertions.assertDoesNotThrow(() -> ConnectorContractValidator.validate(fake, "fake_hash_consistent"));
    }

    @Test
    void validatorPassesForAnInternallyConsistentConnector() {
        // Positive control (Rule 9): a maxcompute-shaped fake (parallel write + full-schema write order +
        // partition-local sort, INSERT/OVERWRITE, no branch) satisfies both invariants and must NOT throw.
        // Without this, a validator bug that always throws would make the two negative tests above pass
        // for the wrong reason.
        ConnectorWritePlanProvider provider = writeProvider();
        Connector fake = connectorWith(provider);
        Mockito.when(provider.supportedOperations())
                .thenReturn(EnumSet.of(WriteOperation.INSERT, WriteOperation.OVERWRITE));
        Mockito.when(provider.supportsWriteBranch()).thenReturn(false);
        Mockito.when(provider.requiresParallelWrite()).thenReturn(true);
        Mockito.when(provider.requiresFullSchemaWriteOrder()).thenReturn(true);
        Mockito.when(provider.requiresPartitionLocalSort()).thenReturn(true);

        Assertions.assertDoesNotThrow(() -> ConnectorContractValidator.validate(fake, "fake_consistent"));
    }

    /**
     * A write plan provider with every trait at its default (false / no operations). supportedOperations is
     * stubbed explicitly because a bare Mockito mock would answer null and the validator reads the set.
     */
    private static ConnectorWritePlanProvider writeProvider() {
        ConnectorWritePlanProvider provider = Mockito.mock(ConnectorWritePlanProvider.class);
        Mockito.when(provider.supportedOperations()).thenReturn(EnumSet.noneOf(WriteOperation.class));
        return provider;
    }

    private static Connector connectorWith(ConnectorWritePlanProvider provider) {
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getWritePlanProvider()).thenReturn(provider);
        return connector;
    }

    @Test
    void validatorPassesForAConnectorWithoutWriteSupport() {
        // A connector exposing NO write plan provider declares no write capability at all, so no invariant
        // can be violated. MUTATION: dropping the null guard makes this throw NullPointerException.
        Connector fake = connectorWith(null);
        Assertions.assertDoesNotThrow(() -> ConnectorContractValidator.validate(fake, "fake_read_only"));
    }
}
