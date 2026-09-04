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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.spi.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Tests for {@link HiveConnectorMetadata#validateStaticPartitionColumns} — the connector-owned rejection of
 * an illegal {@code INSERT [OVERWRITE] ... PARTITION(col='val')} target (port of upstream #65991's
 * {@code BindSink.validateHiveStaticPartition}).
 *
 * <p>WHY this lives in the connector: the engine cannot see hive's partition-key list, and by this branch's
 * SPI contract ({@code ConnectorWriteOps#validateStaticPartitionColumns}) the rejection AND its message are
 * connector-authored. The wording deliberately differs from iceberg's ("is not a partitioned table" vs
 * iceberg's "is not partitioned") because it is upstream's hive wording, which the e2e suite
 * {@code test_hive_write_static_partition.groovy} substring-matches.</p>
 *
 * <p>The metadata is built with a null {@link HmsClient}, the established pattern in this module: for a hive
 * handle this validator reads only the handle's partition-key names, so it never touches the metastore.</p>
 */
public class HiveConnectorMetadataStaticPartitionTest {

    private static HiveConnectorMetadata metadata() {
        return new HiveConnectorMetadata(null, HiveTestProperties.minimal(), new FakeConnectorContext());
    }

    private static HiveTableHandle handle(String... partitionKeys) {
        return new HiveTableHandle.Builder("db", "tbl", HiveTableType.HIVE)
                .partitionKeyNames(Arrays.asList(partitionKeys))
                .build();
    }

    /** The ordinary case: a partition key named exactly as stored is accepted. */
    @Test
    public void acceptsExactPartitionKey() {
        Assertions.assertDoesNotThrow(() -> metadata().validateStaticPartitionColumns(
                null, handle("ts_date"), Collections.singletonList("ts_date")));
    }

    /**
     * A case-mismatched name is accepted. Encodes WHY: HMS lowercases hive column names, so
     * {@code PARTITION(TS_DATE='x')} names the same column as {@code ts_date}. Rejecting it would break
     * upstream #65991's success case and contradict the engine's own case-insensitive column lookup.
     */
    @Test
    public void acceptsCaseMismatchedPartitionKey() {
        Assertions.assertDoesNotThrow(() -> metadata().validateStaticPartitionColumns(
                null, handle("ts_date"), Collections.singletonList("TS_DATE")));
    }

    /** Multi-level partitions: every named key must resolve, in any case. */
    @Test
    public void acceptsAllKeysOfMultiLevelPartitioning() {
        Assertions.assertDoesNotThrow(() -> metadata().validateStaticPartitionColumns(
                null, handle("dt", "region"), Arrays.asList("DT", "region")));
    }

    /**
     * A name that is not a partition key at all is rejected. Encodes WHY: without this the engine would
     * exclude a non-existent column from the bound columns, the value would never reach the row, and the
     * write would land in an unrelated partition instead of failing. Upstream #65991 case 5.1.
     */
    @Test
    public void rejectsUnknownPartitionColumn() {
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata().validateStaticPartitionColumns(
                        null, handle("ts_date"), Collections.singletonList("not_exist_col")));
        Assertions.assertTrue(ex.getMessage().contains("Unknown partition column"),
                "e2e asserts on this substring, got: " + ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("not_exist_col"),
                "message must quote the user's spelling");
        Assertions.assertTrue(ex.getMessage().contains("Available partition columns"),
                "message must list the legal targets");
    }

    /**
     * A DATA column is not a partition key, so naming it is the same error. Guards against a lazy
     * implementation that validates against the full schema instead of the partition keys.
     */
    @Test
    public void rejectsDataColumnAsPartitionColumn() {
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata().validateStaticPartitionColumns(
                        null, handle("ts_date"), Collections.singletonList("tag_value")));
        Assertions.assertTrue(ex.getMessage().contains("Unknown partition column"),
                "a non-partition column must be rejected, got: " + ex.getMessage());
    }

    /**
     * Static-partition syntax on an unpartitioned table is rejected with upstream's hive wording.
     * Upstream #65991 case 5.3.
     */
    @Test
    public void rejectsStaticPartitionOnUnpartitionedTable() {
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata().validateStaticPartitionColumns(
                        null, handle(), Collections.singletonList("ts_date")));
        Assertions.assertTrue(ex.getMessage().contains("is not a partitioned table"),
                "e2e asserts on this exact substring (iceberg says 'is not partitioned'), got: "
                        + ex.getMessage());
    }

    /**
     * An empty spec is a silent no-op, so a plain {@code INSERT ... SELECT} — including one on an
     * unpartitioned table — is byte-unchanged. This is also what keeps
     * {@code HiveConnectorMetadataSiblingDelegationTest}'s empty-list call green.
     */
    @Test
    public void emptySpecIsNoOpEvenOnUnpartitionedTable() {
        Assertions.assertDoesNotThrow(() -> metadata().validateStaticPartitionColumns(
                null, handle(), Collections.emptyList()));
        Assertions.assertDoesNotThrow(() -> metadata().validateStaticPartitionColumns(
                null, handle(), null));
    }
}
