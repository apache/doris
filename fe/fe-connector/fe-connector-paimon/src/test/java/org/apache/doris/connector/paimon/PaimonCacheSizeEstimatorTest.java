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

import org.apache.doris.connector.cache.MetaCacheSizeEstimate;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.ResolvingFileIO;
import org.apache.paimon.fs.hadoop.HadoopFileIO;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTToken;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.iceberg.IcebergTable;
import org.apache.paimon.table.lance.LanceTable;
import org.apache.paimon.table.object.ObjectTable;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class PaimonCacheSizeEstimatorTest {
    private static final Identifier TABLE = Identifier.create("db", "t");

    @TempDir
    java.nio.file.Path warehouse;

    @Test
    void admissionWeightCoversTheStoreAndIndexesBuiltAfterAdmission() throws Exception {
        // A cached table is weighed once, but the first snapshot lookup or scan builds its store (RowType copies
        // of the schema, a copy of the options, a partial-update table's merge function) and field lookups build
        // every RowType's lazy index maps. The admission weight must already cover that grown graph without the
        // FileIO allowance, which pays for the FileIO. MUTATION: dropping the store-growth reservation -> every
        // fixture's grown graph exceeds its weight -> red; dropping the partial-update reservation -> the wide
        // partial-update fixture does -> red.
        List<FileStoreTable> fixtures = new ArrayList<>();
        fixtures.add(table("append_narrow", 10, 0, 0, 0, null));
        fixtures.add(table("append_wide", 300, 0, 0, 0, null));
        fixtures.add(table("pk_narrow", 10, 2, 0, 0, null));
        fixtures.add(table("pk_wide_options", 300, 2, 50, 0, null));
        fixtures.add(table("pk_many_keys", 64, 30, 8, 0, null));
        fixtures.add(table("options", 10, 0, 100, 0, null));
        fixtures.add(table("nested_narrow", 0, 0, 0, 1, null));
        fixtures.add(table("nested_wide", 0, 0, 0, 200, null));
        fixtures.add(table("array_of_rows", 0, 0, 0, 0, new ArrayType(rowOf(20))));
        fixtures.add(table("map_of_rows", 0, 0, 0, 0, new MapType(DataTypes.INT(), rowOf(20))));
        fixtures.add(table("multiset", 0, 0, 0, 0, new MultisetType(DataTypes.STRING())));
        fixtures.add(mergeEngineTable("partial_update_wide", 300, "partial-update"));
        fixtures.add(mergeEngineTable("aggregation_wide", 300, "aggregation"));
        for (FileStoreTable table : fixtures) {
            long weight = weight(table) - PaimonCacheSizeEstimator.estimateFileIO(table.fileIO());
            Assertions.assertNull(readField(table, table.getClass(), "lazyStore"),
                    table.name() + ": estimation must not build the store");
            grow(table);
            long grown = EstimatorCalibrationAssertions.graphSize(table);
            Assertions.assertTrue(weight >= grown,
                    table.name() + ": weight " + weight + " does not cover the grown graph " + grown);
        }
    }

    @Test
    void storeGrowthReservationScalesWithTheSchema() throws Exception {
        assertGrownDelta("fields", table("fields_narrow", 10, 0, 0, 0, null),
                table("fields_wide", 300, 0, 0, 0, null));
        assertGrownDelta("primary-key fields", table("pk_fields_narrow", 10, 2, 0, 0, null),
                table("pk_fields_wide", 300, 2, 0, 0, null));
        assertGrownDelta("options", table("options_none", 10, 0, 0, 0, null),
                table("options_many", 10, 0, 100, 0, null));
        assertGrownDelta("nested fields", table("nested_one", 0, 0, 0, 1, null),
                table("nested_many", 0, 0, 0, 200, null));
    }

    @Test
    void everyTableCarriesAnAllowanceForItsFileIo() throws Exception {
        // A REST catalog gives each table its own RESTTokenFileIO or ResolvingFileIO and a Hive catalog a FileIO
        // per table location; all of them grow after admission, and none can be told apart from a FileIO the
        // catalog shares. So a table's weight carries the same allowance whatever its FileIO is. MUTATION:
        // charging only RESTTokenFileIO -> the other tables weigh a whole allowance less -> red.
        FileStoreTable local = table("file_io", 10, 0, 0, 0, null);
        CatalogContext context = CatalogContext.create(new Options());
        ResolvingFileIO resolving = resolvingFileIO(context);
        HadoopFileIO hadoop = new HadoopFileIO(local.location());
        hadoop.configure(context);
        List<FileIO> fileIOs = Arrays.asList(local.fileIO(), resolving, hadoop,
                new RESTTokenFileIO(context, null, TABLE, local.location()));
        long withoutFileIO = weight(local) - PaimonCacheSizeEstimator.estimateFileIO(local.fileIO());
        for (FileIO fileIO : fileIOs) {
            FileStoreTable table = FileStoreTableFactory.create(
                    fileIO, local.location(), local.schema(), CatalogEnvironment.empty());
            long allowance = PaimonCacheSizeEstimator.estimateFileIO(fileIO);
            Assertions.assertTrue(allowance >= 16L * 1024L);
            Assertions.assertEquals(withoutFileIO + allowance, weight(table),
                    fileIO.getClass().getSimpleName() + " must be charged to its table");
        }

        // A ResolvingFileIO builds its backend on the first data access, after admission.
        resolving.exists(local.location());
        long resolved = EstimatorCalibrationAssertions.graphSize(resolving)
                - EstimatorCalibrationAssertions.graphSize(context);
        Assertions.assertTrue(PaimonCacheSizeEstimator.estimateFileIO(resolving) >= resolved,
                "the allowance does not cover a ResolvingFileIO with its backend: " + resolved);
    }

    @Test
    void tablesOutsideTheFileStoreCarryTheFileIoAllowanceToo() throws Exception {
        // A REST catalog builds an external table of any kind on a ResolvingFileIO of its own, even with data
        // tokens enabled, so format, object, Lance and Iceberg tables grow after admission like a FileStoreTable.
        // MUTATION: charging the FileIO only on the FileStoreTable path -> each weight falls below the allowance
        // alone -> red.
        CatalogContext context = CatalogContext.create(new Options());
        String location = warehouse.resolve("external").toUri().toString();
        List<Table> tables = Arrays.asList(
                FormatTable.builder().fileIO(resolvingFileIO(context)).identifier(TABLE).rowType(rowOf(10))
                        .partitionKeys(Collections.emptyList()).location(location).format(FormatTable.Format.CSV)
                        .options(Collections.emptyMap()).build(),
                ObjectTable.builder().fileIO(resolvingFileIO(context)).identifier(TABLE).location(location)
                        .options(Collections.emptyMap()).build(),
                LanceTable.builder().fileIO(resolvingFileIO(context)).identifier(TABLE).rowType(rowOf(10))
                        .location(location).options(Collections.emptyMap()).build(),
                IcebergTable.builder().fileIO(resolvingFileIO(context)).identifier(TABLE).rowType(rowOf(10))
                        .partitionKeys(Collections.emptyList()).location(location)
                        .options(Collections.emptyMap()).build());
        for (Table table : tables) {
            String kind = table.getClass().getSimpleName();
            long weight = weight(table);
            long allowance = PaimonCacheSizeEstimator.estimateFileIO(table.fileIO());
            Assertions.assertTrue(weight > allowance,
                    kind + ": weight " + weight + " leaves out the FileIO allowance " + allowance);
            // A ResolvingFileIO builds its backend on the first data access, after admission.
            table.fileIO().exists(new Path(location));
            long grown = EstimatorCalibrationAssertions.graphSize(table)
                    - EstimatorCalibrationAssertions.graphSize(context);
            Assertions.assertTrue(weight >= grown,
                    kind + ": weight " + weight + " does not cover the grown graph " + grown);
        }
    }

    @Test
    void fileIoAllowanceCoversARestDataTokenAndItsRefresh() throws Exception {
        // The token arrives after admission and a refresh replaces it, so the one allowance must cover the token
        // the first data access fetches as well as a later, larger one. The 1,200-character security token is the
        // usual STS size; the 8,192-character one stands for a credential several times larger.
        // MUTATION: a 4 KB allowance -> the refreshed token outgrows it -> red.
        FileStoreTable local = table("rest_token", 10, 0, 0, 0, null);
        CatalogContext context = CatalogContext.create(new Options());
        RESTTokenFileIO fileIO = new RESTTokenFileIO(context, null, TABLE, local.location());
        long allowance = PaimonCacheSizeEstimator.estimateFileIO(fileIO);
        for (int securityTokenChars : new int[] {1200, 8192}) {
            Map<String, String> token = new HashMap<>();
            token.put("fs.oss.accessKeyId", "STS." + "a".repeat(28));
            token.put("fs.oss.accessKeySecret", "b".repeat(44));
            token.put("fs.oss.securityToken", "c".repeat(securityTokenChars));
            token.put("fs.oss.endpoint", "oss-cn-hangzhou-internal.aliyuncs.com");
            setToken(fileIO, new RESTToken(token, System.currentTimeMillis() + 3_600_000L));
            long owned = EstimatorCalibrationAssertions.graphSize(fileIO)
                    - EstimatorCalibrationAssertions.graphSize(context);
            Assertions.assertTrue(allowance >= owned, "allowance " + allowance
                    + " does not cover the table-owned graph with a " + securityTokenChars + "-character token "
                    + owned);
        }
    }

    /**
     * A primary-key table with {@code fieldCount} INT value fields under {@code mergeEngine}: a partial-update
     * table puts them all in one sequence group with a default aggregate function, an aggregation table sums them.
     */
    private FileStoreTable mergeEngineTable(String name, int fieldCount, String mergeEngine) throws Exception {
        Schema.Builder builder = Schema.newBuilder()
                .column("part", DataTypes.INT())
                .column("key_0", DataTypes.INT().notNull())
                .column("seq_0", DataTypes.INT());
        List<String> grouped = new ArrayList<>();
        for (int i = 0; i < fieldCount; i++) {
            builder.column("field_" + i, DataTypes.INT());
            grouped.add("field_" + i);
        }
        builder.primaryKey("key_0", "part").partitionKeys("part").option("merge-engine", mergeEngine);
        if ("partial-update".equals(mergeEngine)) {
            builder.option("fields.seq_0.sequence-group", String.join(",", grouped));
            builder.option("fields.default-aggregate-function", "last_non_null_value");
        } else if ("aggregation".equals(mergeEngine)) {
            builder.option("fields.default-aggregate-function", "sum");
        }
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(warehouse.resolve(name).toUri());
        new SchemaManager(fileIO, path).createTable(builder.build());
        return FileStoreTableFactory.create(fileIO, path);
    }

    private static ResolvingFileIO resolvingFileIO(CatalogContext context) {
        ResolvingFileIO fileIO = new ResolvingFileIO();
        fileIO.configure(context);
        return fileIO;
    }

    private static void setToken(RESTTokenFileIO fileIO, RESTToken token) throws ReflectiveOperationException {
        Field tokenField = RESTTokenFileIO.class.getDeclaredField("token");
        tokenField.setAccessible(true);
        tokenField.set(fileIO, token);
    }

    private void assertGrownDelta(String fixture, FileStoreTable small, FileStoreTable large) throws Exception {
        long smallWeight = weight(small);
        long largeWeight = weight(large);
        grow(small);
        grow(large);
        EstimatorCalibrationAssertions.assertConservativeDelta(fixture, smallWeight, largeWeight, small, large);
    }

    private static long weight(Table table) {
        MetaCacheSizeEstimate estimate = PaimonCacheSizeEstimator.estimateTable(
                TABLE, table, PaimonMetaCacheCatalog.TABLE_ENTRY_OVERHEAD_BYTES);
        Assertions.assertTrue(estimate.isComplete(), estimate.getIncompleteReason());
        return estimate.getBytes();
    }

    /**
     * A table with a partition column, {@code fieldCount} INT fields, {@code primaryKeyCount} primary key fields
     * (besides the partition column), {@code optionCount} options, an optional ROW of {@code nestedFieldCount}
     * fields and an optional {@code extraType} column.
     */
    private FileStoreTable table(String name, int fieldCount, int primaryKeyCount, int optionCount,
            int nestedFieldCount, DataType extraType) throws Exception {
        Schema.Builder builder = Schema.newBuilder().column("part", DataTypes.INT());
        for (int i = 0; i < fieldCount; i++) {
            builder.column("field_" + i, DataTypes.INT());
        }
        List<String> primaryKeys = new ArrayList<>();
        for (int i = 0; i < primaryKeyCount; i++) {
            builder.column("key_" + i, DataTypes.INT().notNull());
            primaryKeys.add("key_" + i);
        }
        if (nestedFieldCount > 0) {
            builder.column("payload", rowOf(nestedFieldCount));
        }
        if (extraType != null) {
            builder.column("extra", extraType);
        }
        if (!primaryKeys.isEmpty()) {
            primaryKeys.add("part");
            builder.primaryKey(primaryKeys);
        }
        builder.partitionKeys("part");
        for (int i = 0; i < optionCount; i++) {
            builder.option("option_" + i, "value_" + i);
        }
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(warehouse.resolve(name).toUri());
        new SchemaManager(fileIO, path).createTable(builder.build());
        return FileStoreTableFactory.create(fileIO, path);
    }

    private static RowType rowOf(int fieldCount) {
        List<DataField> fields = new ArrayList<>();
        for (int i = 0; i < fieldCount; i++) {
            fields.add(new DataField(1000 + i, "nested_" + i, DataTypes.INT()));
        }
        return new RowType(fields);
    }

    /** What query planning builds on a cached table: the store, then every reachable RowType's index maps. */
    private static void grow(FileStoreTable table) throws Exception {
        table.latestSnapshot();
        table.newReadBuilder().newScan();
        Object store = readField(table, table.getClass(), "lazyStore");
        for (Class<?> owner = store.getClass(); owner != null && owner != Object.class;
                owner = owner.getSuperclass()) {
            for (Field field : owner.getDeclaredFields()) {
                if (RowType.class.isAssignableFrom(field.getType())) {
                    field.setAccessible(true);
                    Object rowType = field.get(store);
                    if (rowType != null) {
                        buildIndexes((RowType) rowType);
                    }
                }
            }
        }
        for (DataField field : table.schema().fields()) {
            buildIndexes(field.type());
        }
    }

    private static void buildIndexes(DataType type) {
        if (type instanceof RowType) {
            RowType rowType = (RowType) type;
            // The store also holds empty row types (for example the bucket key type of an unbucketed table).
            if (!rowType.getFields().isEmpty()) {
                DataField first = rowType.getFields().get(0);
                rowType.getField(first.name());
                rowType.getFieldIndex(first.name());
                rowType.getField(first.id());
                rowType.getFieldIndexByFieldId(first.id());
            }
            for (DataField field : rowType.getFields()) {
                buildIndexes(field.type());
            }
        } else if (type instanceof ArrayType) {
            buildIndexes(((ArrayType) type).getElementType());
        } else if (type instanceof MultisetType) {
            buildIndexes(((MultisetType) type).getElementType());
        } else if (type instanceof MapType) {
            buildIndexes(((MapType) type).getKeyType());
            buildIndexes(((MapType) type).getValueType());
        }
    }

    private static Object readField(Object target, Class<?> owner, String fieldName) throws Exception {
        Field field = owner.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }
}
