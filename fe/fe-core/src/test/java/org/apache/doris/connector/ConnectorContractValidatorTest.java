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

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorContractValidator;
import org.apache.doris.connector.api.handle.WriteOperation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.EnumSet;

/**
 * Rule-9 behavior gates for {@link ConnectorContractValidator}: it must fail loud
 * ({@link IllegalStateException}) when a connector's own delegators are internally inconsistent, and it
 * must pass silently when they are not. These are the primary enforcement of the two structural invariants
 * (static per-connector properties, checked here and in each connector's own contract test rather than at
 * catalog registration). Fake {@link Connector}s (plain Mockito mocks, stubbing only the argless delegators
 * the two invariants read) stand in for a real connector.
 */
public class ConnectorContractValidatorTest {

    @Test
    void validatorRejectsBranchWithoutInsert() {
        // Invariant #2: supportsWriteBranch() implies supportedWriteOperations() contains INSERT (a
        // branch write is an INSERT modifier, never a capability on its own). A connector claiming
        // branch support with no declared INSERT is self-contradictory -> must fail loud at registration
        // instead of surfacing as a confusing failure the first time someone writes to a branch.
        // MUTATION: dropping the `!` in the validator's #2 check makes this test go red (see task report).
        Connector fake = Mockito.mock(Connector.class);
        Mockito.when(fake.supportsWriteBranch()).thenReturn(true);
        Mockito.when(fake.supportedWriteOperations()).thenReturn(EnumSet.noneOf(WriteOperation.class));

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
        Connector fake = Mockito.mock(Connector.class);
        Mockito.when(fake.requiresPartitionLocalSort()).thenReturn(true);
        Mockito.when(fake.requiresParallelWrite()).thenReturn(false);
        Mockito.when(fake.requiresFullSchemaWriteOrder()).thenReturn(true);

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
        Connector fake = Mockito.mock(Connector.class);
        Mockito.when(fake.requiresPartitionLocalSort()).thenReturn(true);
        Mockito.when(fake.requiresParallelWrite()).thenReturn(true);
        Mockito.when(fake.requiresFullSchemaWriteOrder()).thenReturn(false);

        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class,
                () -> ConnectorContractValidator.validate(fake, "fake_localsort_no_fullschema"));
        Assertions.assertTrue(ex.getMessage().contains("requiresPartitionLocalSort"), "got: " + ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("fake_localsort_no_fullschema"), "got: " + ex.getMessage());
    }

    @Test
    void validatorPassesForAnInternallyConsistentConnector() {
        // Positive control (Rule 9): a maxcompute-shaped fake (parallel write + full-schema write order +
        // partition-local sort, INSERT/OVERWRITE, no branch) satisfies both invariants and must NOT throw.
        // Without this, a validator bug that always throws would make the two negative tests above pass
        // for the wrong reason.
        Connector fake = Mockito.mock(Connector.class);
        Mockito.when(fake.supportedWriteOperations())
                .thenReturn(EnumSet.of(WriteOperation.INSERT, WriteOperation.OVERWRITE));
        Mockito.when(fake.supportsWriteBranch()).thenReturn(false);
        Mockito.when(fake.requiresParallelWrite()).thenReturn(true);
        Mockito.when(fake.requiresFullSchemaWriteOrder()).thenReturn(true);
        Mockito.when(fake.requiresPartitionLocalSort()).thenReturn(true);

        Assertions.assertDoesNotThrow(() -> ConnectorContractValidator.validate(fake, "fake_consistent"));
    }
}
