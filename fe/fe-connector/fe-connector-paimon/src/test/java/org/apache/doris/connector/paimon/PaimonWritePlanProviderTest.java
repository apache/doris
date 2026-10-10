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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorWriteHandle;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.write.ConnectorChangelogMode;
import org.apache.doris.connector.spi.write.ConnectorWriteDistribution;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PaimonWritePlanProviderTest {

    @Test
    public void changelogModeMatchesJniWriterEncoding() {
        PaimonWritePlanProvider provider = new PaimonWritePlanProvider(null, null, null);
        ConnectorChangelogMode mode = provider.getChangelogMode().orElseThrow(AssertionError::new);

        Assertions.assertEquals(PaimonWritePlanProvider.ROW_KIND_COLUMN,
                mode.getOperationColumnName());
        Assertions.assertEquals(PaimonWritePlanProvider.INSERT_OPERATION, mode.getInsertValue());
        Assertions.assertEquals(PaimonWritePlanProvider.UPDATE_OPERATION, mode.getUpdateValue());
        Assertions.assertEquals(PaimonWritePlanProvider.DELETE_OPERATION, mode.getDeleteValue());
    }

    @Test
    public void paimonWritesRequireDedicatedExecVersion() {
        Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonWritePlanProvider.requireBeExecVersion(
                        PaimonWritePlanProvider.MIN_BE_EXEC_VERSION - 1));
        Assertions.assertDoesNotThrow(() -> PaimonWritePlanProvider.requireBeExecVersion(
                PaimonWritePlanProvider.MIN_BE_EXEC_VERSION));
    }

    @Test
    public void insertColumnNamesFollowBoundSchemaOrder() {
        ConnectorWriteHandle handle = handle(
                columns("score", "name", "id"),
                columns("id", "name", "score"),
                WriteOperation.INSERT);

        Assertions.assertEquals(Arrays.asList("id", "name", "score"),
                PaimonWritePlanProvider.outputColumnNames(handle));
    }

    @Test
    public void changelogColumnNamesPrefixRowKindToBoundSchema() {
        ConnectorWriteHandle handle = handle(
                columns("score", "id"),
                columns("id", "name", "score"),
                WriteOperation.UPDATE);

        Assertions.assertEquals(
                Arrays.asList(PaimonWritePlanProvider.ROW_KIND_COLUMN, "id", "name", "score"),
                PaimonWritePlanProvider.outputColumnNames(handle));
    }

    @Test
    public void partialPrimaryKeyWriteRequiresPartialUpdateMergeEngine() {
        Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonWritePlanProvider.validateWriteColumnsForMergeEngine(
                        2, 3, true, CoreOptions.MergeEngine.FIRST_ROW));

        Assertions.assertDoesNotThrow(
                () -> PaimonWritePlanProvider.validateWriteColumnsForMergeEngine(
                        2, 3, true, CoreOptions.MergeEngine.PARTIAL_UPDATE));
    }

    @Test
    public void fullOrAppendOnlyWriteDoesNotRequirePartialUpdateMergeEngine() {
        Assertions.assertDoesNotThrow(() -> PaimonWritePlanProvider.validateWriteColumnsForMergeEngine(
                3, 3, true, CoreOptions.MergeEngine.FIRST_ROW));
        Assertions.assertDoesNotThrow(() -> PaimonWritePlanProvider.validateWriteColumnsForMergeEngine(
                2, 3, false, CoreOptions.MergeEngine.FIRST_ROW));
    }

    @Test
    public void writeColumnsConvertPaimonDefaultsToDorisSqlAndPreserveSchemaProperties() {
        List<ConnectorColumn> columns = PaimonWritePlanProvider.mapWriteColumns(
                Arrays.asList(
                        new DataField(7, "id", DataTypes.INT().notNull(), "identifier"),
                        new DataField(9, "note", DataTypes.STRING(), null, "default-note"),
                        new DataField(11, "ts", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6))),
                Collections.singletonList("ID"), PaimonTypeMapping.Options.DEFAULT);

        Assertions.assertFalse(columns.get(0).isNullable());
        Assertions.assertTrue(columns.get(0).isKey());
        Assertions.assertEquals(7, columns.get(0).getUniqueId());
        Assertions.assertEquals("default-note", columns.get(1).getDefaultValue());
        Assertions.assertEquals("'default-note'", columns.get(1).getDefaultValueSql());
        Assertions.assertTrue(columns.get(2).isWithTimeZone());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> columns.add(columns.get(0)));
    }

    @Test
    public void fixedBucketAppendTableWithoutWriteOnlyRequiresSingleWriter() {
        // A fixed-bucket table whose bucket key cannot be routed natively (DATE) falls back to the
        // planner distribution. Unless the table is write-only, its writers also compact, and two
        // writers compacting the same bucket conflict, so the write must be gathered to one writer.
        Assertions.assertEquals(ConnectorWriteDistribution.Mode.GATHER, writeDistribution(
                Collections.emptyList(), Collections.emptyList(), fixedBucketOptions(false),
                BucketMode.HASH_FIXED));
        Assertions.assertEquals(ConnectorWriteDistribution.Mode.EXTERNAL_UNPARTITIONED, writeDistribution(
                Collections.emptyList(), Collections.emptyList(), fixedBucketOptions(true),
                BucketMode.HASH_FIXED));
    }

    @Test
    public void primaryKeyAndDynamicBucketTablesRequireSingleWriter() {
        Assertions.assertEquals(ConnectorWriteDistribution.Mode.GATHER, writeDistribution(
                Collections.emptyList(), Collections.singletonList("dt"), fixedBucketOptions(true),
                BucketMode.HASH_FIXED));
        Assertions.assertEquals(ConnectorWriteDistribution.Mode.GATHER, writeDistribution(
                Collections.emptyList(), Collections.singletonList("id"), dynamicBucketOptions(),
                BucketMode.HASH_DYNAMIC));
        // The primary key leaves out the partition column, so an upsert can move a key across
        // partitions and the bucket of a key comes from the cross-partition index.
        Assertions.assertEquals(ConnectorWriteDistribution.Mode.GATHER, writeDistribution(
                Collections.singletonList("region"), Collections.singletonList("id"), dynamicBucketOptions(),
                BucketMode.KEY_DYNAMIC));
        Assertions.assertEquals(ConnectorWriteDistribution.Mode.EXECUTION_ANY, writeDistribution(
                Collections.emptyList(), Collections.emptyList(), dynamicBucketOptions(),
                BucketMode.BUCKET_UNAWARE));
    }

    private static ConnectorWriteDistribution.Mode writeDistribution(List<String> partitionKeys,
            List<String> primaryKeys, Map<String, String> options, BucketMode expectedBucketMode) {
        List<DataField> fields = Arrays.asList(
                new DataField(0, "id", DataTypes.INT().notNull()),
                new DataField(1, "dt", DataTypes.DATE().notNull()),
                new DataField(2, "region", DataTypes.STRING().notNull()));
        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(),
                new Path("file:///tmp/paimon-write-distribution/tbl"),
                new TableSchema(0L, fields, 2, partitionKeys, primaryKeys, options, ""));
        Assertions.assertEquals(expectedBucketMode, table.bucketMode(), "fixture bucket mode");
        PaimonTableHandle handle = new PaimonTableHandle("db", "tbl", partitionKeys, primaryKeys);
        handle.setPaimonTable(table);
        return new PaimonWritePlanProvider(null, null, null).getWriteDistribution(null, handle).getMode();
    }

    private static Map<String, String> fixedBucketOptions(boolean writeOnly) {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "2");
        options.put(CoreOptions.BUCKET_KEY.key(), "dt");
        options.put(CoreOptions.WRITE_ONLY.key(), Boolean.toString(writeOnly));
        return options;
    }

    private static Map<String, String> dynamicBucketOptions() {
        return Collections.singletonMap(CoreOptions.BUCKET.key(), "-1");
    }

    private static List<ConnectorColumn> columns(String... names) {
        return Arrays.stream(names)
                .map(name -> new ConnectorColumn(
                        name, ConnectorType.of("INT"), null, true, null))
                .collect(Collectors.toList());
    }

    private static ConnectorWriteHandle handle(List<ConnectorColumn> columns,
            List<ConnectorColumn> boundTargetColumns, WriteOperation operation) {
        return new ConnectorWriteHandle() {
            @Override
            public ConnectorTableHandle getTableHandle() {
                return null;
            }

            @Override
            public List<ConnectorColumn> getColumns() {
                return columns;
            }

            @Override
            public List<ConnectorColumn> getBoundTargetColumns() {
                return boundTargetColumns;
            }

            @Override
            public boolean isOverwrite() {
                return false;
            }

            @Override
            public Map<String, String> getStaticPartitionSpec() {
                return Collections.emptyMap();
            }

            @Override
            public WriteOperation getWriteOperation() {
                return operation;
            }
        };
    }
}
