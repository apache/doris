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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.spi.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Tests for {@link MaxComputeConnectorMetadata#checkStaticPartitionColumns} — MaxCompute's static-partition
 * name guard.
 *
 * <p>WHY this guard exists (Rule 11, fail loud): MaxCompute consumes the static-partition spec by EXACT key
 * lookup — {@code MaxComputeWritePlanProvider.buildStaticPartitionSpecString} does
 * {@code partitionColumnNames.stream().filter(staticPartitionSpec::containsKey)} — and those keys are the raw
 * case the user typed (built in {@code InsertIntoTableCommand} / {@code InsertOverwriteTableCommand}), which
 * the engine's static-partition name canonicalization does not reach. So a case-mismatched
 * {@code PARTITION(DS='x')} against a column {@code ds} would produce an EMPTY {@code PartitionSpec} and write
 * to the wrong place. Before the engine canonicalized those names, the mismatch surfaced as a column-count
 * error; this keeps it loud with a message that names the real cause.</p>
 *
 * <p>Matching is deliberately case-SENSITIVE here, unlike hive: the downstream lookup this guards is itself
 * exact, and ODPS's own case semantics for partition names are not verified in this environment. Accepting a
 * case-mismatched name would be a behavior change needing a real ODPS cluster to validate.</p>
 */
public class MaxComputeStaticPartitionValidationTest {

    /** The ordinary case: an exactly-spelled partition column is accepted. */
    @Test
    public void acceptsExactPartitionColumn() {
        Assertions.assertDoesNotThrow(() -> MaxComputeConnectorMetadata.checkStaticPartitionColumns(
                "t", Arrays.asList("ds", "region"), Collections.singletonList("ds")));
    }

    /** Every column of a multi-level spec must resolve. */
    @Test
    public void acceptsAllExactColumnsOfMultiLevelSpec() {
        Assertions.assertDoesNotThrow(() -> MaxComputeConnectorMetadata.checkStaticPartitionColumns(
                "t", Arrays.asList("ds", "region"), Arrays.asList("ds", "region")));
    }

    /**
     * THE regression guard: a case-mismatched name must fail loud rather than reach
     * {@code buildStaticPartitionSpecString}, whose exact {@code containsKey} would drop it and silently
     * build {@code PartitionSpec("")}.
     */
    @Test
    public void rejectsCaseMismatchedPartitionColumn() {
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> MaxComputeConnectorMetadata.checkStaticPartitionColumns(
                        "t", Arrays.asList("ds", "region"), Collections.singletonList("DS")));
        Assertions.assertTrue(ex.getMessage().contains("Unknown partition column"),
                "expected the unknown-column reject, got: " + ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("DS"), "message must quote the user's spelling");
        Assertions.assertTrue(ex.getMessage().contains("Available partition columns"),
                "message must list the legal targets so the case fix is obvious");
    }

    /** A name that is no partition column at all is rejected the same way. */
    @Test
    public void rejectsUnknownPartitionColumn() {
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> MaxComputeConnectorMetadata.checkStaticPartitionColumns(
                        "t", Arrays.asList("ds", "region"), Collections.singletonList("nope")));
        Assertions.assertTrue(ex.getMessage().contains("Unknown partition column"),
                "got: " + ex.getMessage());
    }

    /** Static-partition syntax on an unpartitioned table is rejected. */
    @Test
    public void rejectsStaticPartitionOnUnpartitionedTable() {
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> MaxComputeConnectorMetadata.checkStaticPartitionColumns(
                        "t", Collections.emptyList(), Collections.singletonList("ds")));
        Assertions.assertTrue(ex.getMessage().contains("is not a partitioned table"),
                "got: " + ex.getMessage());
    }
}
