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

import org.apache.doris.connector.spi.ConnectorContractValidator;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.write.ConnectorWritePlanProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;

/**
 * The positive sample {@link ConnectorContractValidator}'s partition-hash-write invariants were missing.
 *
 * <p>WHY (Rule 9): hive is the ONLY connector that declares {@code requiresPartitionHashWrite}, and its tests
 * did not call the validator — so invariants #4 (hash write implies parallel write AND full-schema write
 * order) and #5 (the two partition-distribution arms are mutually exclusive) were exercised solely by
 * fe-core's fake connector. A fake proves the validator rejects a bad declaration; only a real connector
 * proves the shipped declaration is a good one. Dropping any of the three traits hive declares, or adding
 * {@code requiresPartitionLocalSort} alongside the hash arm, must fail loud here.</p>
 *
 * <p>The provider is built directly ({@code HiveWritePlanProvider}'s constructor is pure field assignment)
 * with a null client: the validator reads only the declared traits, none of which touch the metastore. The
 * no-arg {@link HiveConnector#getWritePlanProvider()} is not used because it builds a real HmsClient, whose
 * Hadoop stack is absent from connector unit tests.</p>
 */
public class HiveConnectorContractTest {

    /** A hive connector whose connector-level write provider is the REAL one, minus the metastore client. */
    private HiveConnector connectorWithRealWriteProvider() {
        FakeConnectorContext context = new FakeConnectorContext();
        return new HiveConnector(HiveTestProperties.minimalMap(), context) {
            @Override
            public ConnectorWritePlanProvider getWritePlanProvider() {
                return new HiveWritePlanProvider(null, HiveTestProperties.minimal(), context);
            }
        };
    }

    @Test
    public void declaredWriteCapabilitiesMatchAndPassContractValidator() {
        HiveConnector connector = connectorWithRealWriteProvider();

        ConnectorWritePlanProvider writeProvider = connector.getWritePlanProvider();
        Assertions.assertEquals(EnumSet.of(WriteOperation.INSERT, WriteOperation.OVERWRITE),
                writeProvider.supportedOperations(),
                "hive writes are INSERT and INSERT OVERWRITE; no branch/delete/merge/rewrite");
        Assertions.assertFalse(writeProvider.supportsWriteBranch(),
                "hive has no named table branches");
        // The triad invariant #4 checks: the sink indexes partition columns by full-schema position and
        // distributes in parallel, so the hash arm is meaningless without these two.
        Assertions.assertTrue(writeProvider.requiresParallelWrite());
        Assertions.assertTrue(writeProvider.requiresFullSchemaWriteOrder());
        Assertions.assertTrue(writeProvider.requiresPartitionHashWrite(),
                "hive is the connector that exercises the hash-without-sort arm");
        Assertions.assertFalse(writeProvider.requiresPartitionLocalSort(),
                "invariant #5: a connector picks at most one partition-distribution arm");
        // Hive's data files STRIP the partition column, yet the BE derives the partition DIRECTORY from the
        // row value (vhive_table_writer.cpp::_create_partition_values), where a NULL becomes the literal
        // __HIVE_DEFAULT_PARTITION__. There is no static-partition field on THiveTableSink to carry the value
        // out of band, so the row is the only channel. If this flips to false, BindSink stops re-projecting
        // the PARTITION literal and INSERT ... PARTITION(dt='x') silently writes to the wrong partition.
        Assertions.assertTrue(writeProvider.requiresMaterializeStaticPartitionValues(),
                "hive must materialize the PARTITION literal into the row, or the BE writes "
                        + "__HIVE_DEFAULT_PARTITION__");

        ConnectorContractValidator.validate(connector, "hive");
    }
}
