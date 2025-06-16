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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.source.IcebergTableQueryInfo;

import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.GenericPartitionFieldSummary;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFile.PartitionFieldSummary;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StructType;
import org.checkerframework.org.plumelib.util.ArrayMap;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class IcebergUtilsTest {
    @Test
    public void testParseTableName() {
        try {
            IcebergHMSExternalCatalog c1 =
                    new IcebergHMSExternalCatalog(1, "name", null, new HashMap<>(), "");
            HiveCatalog i1 = IcebergUtils.createIcebergHiveCatalog(c1, "i1");
            Assert.assertTrue(getListAllTables(i1));

            IcebergHMSExternalCatalog c2 =
                    new IcebergHMSExternalCatalog(1, "name", null,
                            new HashMap<String, String>() {{
                                    put("list-all-tables", "true");
                                }},
                            "");
            HiveCatalog i2 = IcebergUtils.createIcebergHiveCatalog(c2, "i1");
            Assert.assertTrue(getListAllTables(i2));

            IcebergHMSExternalCatalog c3 =
                    new IcebergHMSExternalCatalog(1, "name", null,
                            new HashMap<String, String>() {{
                                    put("list-all-tables", "false");
                                }},
                        "");
            HiveCatalog i3 = IcebergUtils.createIcebergHiveCatalog(c3, "i1");
            Assert.assertFalse(getListAllTables(i3));
        } catch (Exception e) {
            Assert.fail();
        }
    }

    private boolean getListAllTables(HiveCatalog hiveCatalog) throws IllegalAccessException, NoSuchFieldException {
        Field declaredField = hiveCatalog.getClass().getDeclaredField("listAllTables");
        declaredField.setAccessible(true);
        return declaredField.getBoolean(hiveCatalog);
    }

    @Test
    public void testGetMatchingManifest() {

        // partition : 100 - 200
        GenericManifestFile f1 = getGenericManifestFileForDataTypeWithPartitionSummary(
                "manifest_f1.avro",
                Collections.singletonList(new GenericPartitionFieldSummary(
                    false, false, getByteBufferForLong(100), getByteBufferForLong(200))));

        // partition : 300 - 400
        GenericManifestFile f2 = getGenericManifestFileForDataTypeWithPartitionSummary(
                "manifest_f2.avro",
                Collections.singletonList(new GenericPartitionFieldSummary(
                    false, false, getByteBufferForLong(300), getByteBufferForLong(400))));

        // partition : 500 - 600
        GenericManifestFile f3 = getGenericManifestFileForDataTypeWithPartitionSummary(
                "manifest_f3.avro",
                    Collections.singletonList(new GenericPartitionFieldSummary(
                        false, false, getByteBufferForLong(500), getByteBufferForLong(600))));

        List<ManifestFile> manifestFiles = new ArrayList<ManifestFile>() {{
                add(f1);
                add(f2);
                add(f3);
            }};

        Schema schema = new Schema(
                StructType.of(
                        Types.NestedField.required(1, "id", LongType.get()),
                        Types.NestedField.required(2, "data", LongType.get()),
                        Types.NestedField.required(3, "par", LongType.get()))
                    .fields());

        // test empty partition spec
        HashMap<Integer, PartitionSpec> emptyPartitionSpecsById = new HashMap<Integer, PartitionSpec>() {{
                put(0, PartitionSpec.builderFor(schema).build());
            }};
        assertManifest(manifestFiles, emptyPartitionSpecsById, Expressions.alwaysTrue(), manifestFiles);

        // test long partition spec
        HashMap<Integer, PartitionSpec> longPartitionSpecsById = new HashMap<Integer, PartitionSpec>() {{
                put(0, PartitionSpec.builderFor(schema).identity("par").build());
            }};
        // 1. par > 10
        UnboundPredicate<Long> e1 = Expressions.greaterThan("par", 10L);
        assertManifest(manifestFiles, longPartitionSpecsById, Expressions.and(Expressions.alwaysTrue(), e1), manifestFiles);

        // 2. 10 < par < 90
        UnboundPredicate<Long> e2 = Expressions.greaterThan("par", 90L);
        assertManifest(manifestFiles, longPartitionSpecsById, Expressions.and(e1, e2), manifestFiles);

        // 3. 10 < par < 300
        UnboundPredicate<Long> e3 = Expressions.lessThan("par", 300L);
        assertManifest(manifestFiles, longPartitionSpecsById, Expressions.and(e1, e3), Collections.singletonList(f1));

        // 4. 10 < par < 400
        UnboundPredicate<Long> e4 = Expressions.lessThan("par", 400L);
        ArrayList<ManifestFile> expect1 = new ArrayList<ManifestFile>() {{
                add(f1);
                add(f2);
            }};
        assertManifest(manifestFiles, longPartitionSpecsById, Expressions.and(e1, e4), expect1);

        // 5. 10 < par < 501
        UnboundPredicate<Long> e5 = Expressions.lessThan("par", 501L);
        assertManifest(manifestFiles, longPartitionSpecsById, Expressions.and(e1, e5), manifestFiles);

        // 6. 200 < par < 501
        UnboundPredicate<Long> e6 = Expressions.greaterThan("par", 200L);
        ArrayList<ManifestFile> expect2 = new ArrayList<ManifestFile>() {{
                add(f2);
                add(f3);
            }};
        assertManifest(manifestFiles, longPartitionSpecsById, Expressions.and(e6, e5), expect2);

        // 7. par > 600
        UnboundPredicate<Long> e7 = Expressions.greaterThan("par", 600L);
        assertManifest(manifestFiles, longPartitionSpecsById, Expressions.and(Expressions.alwaysTrue(), e7), Collections.emptyList());

        // 8. par < 100
        UnboundPredicate<Long> e8 = Expressions.lessThan("par", 100L);
        assertManifest(manifestFiles, longPartitionSpecsById, Expressions.and(Expressions.alwaysTrue(), e8), Collections.emptyList());
    }

    private void assertManifest(List<ManifestFile> dataManifests,
                                Map<Integer, PartitionSpec> specsById,
                                Expression dataFilter,
                                List<ManifestFile> expected) {
        CloseableIterable<ManifestFile> matchingManifest =
                IcebergUtils.getMatchingManifest(dataManifests, specsById, dataFilter);
        List<ManifestFile> ret = new ArrayList<>();
        matchingManifest.forEach(ret::add);
        ret.sort(Comparator.comparing(ManifestFile::path));
        Assert.assertEquals(expected, ret);
    }

    private ByteBuffer getByteBufferForLong(long num) {
        return Conversions.toByteBuffer(Types.LongType.get(), num);
    }

    private GenericManifestFile getGenericManifestFileForDataTypeWithPartitionSummary(
            String path,
            List<PartitionFieldSummary> partitionFieldSummaries) {
        return new GenericManifestFile(
            path,
            1024L,
            0,
            ManifestContent.DATA,
            1,
            1,
            123456789L,
            2,
            100,
            0,
            0,
            0,
            0,
            partitionFieldSummaries,
            null);
    }

    @Test
    public void testGetQuerySpecSnapshot() throws UserException {
        Table table = Mockito.mock(Table.class);

        // init schemas 0,1,2
        HashMap<Integer, Schema> schemas = new HashMap<>();
        schemas.put(0, mockSchemaWithId(0));
        schemas.put(1, mockSchemaWithId(1));
        schemas.put(2, mockSchemaWithId(2));
        Mockito.when(table.schemas()).thenReturn(schemas);
        // init current schema
        Mockito.when(table.schema()).thenReturn(schemas.get(2));

        // init snapshot 1,2,3,4
        Snapshot s1 = mockSnapshot(1, 0);
        Mockito.when(table.snapshot(1)).thenReturn(s1);
        Snapshot s2 = mockSnapshot(2, 0);
        Mockito.when(table.snapshot(2)).thenReturn(s2);
        Snapshot s3 = mockSnapshot(3, 1);
        Mockito.when(table.snapshot(3)).thenReturn(s3);
        Snapshot s4 = mockSnapshot(4, 1);
        Mockito.when(table.snapshot(4)).thenReturn(s4);

        // init history for snapshots
        List<HistoryEntry> history = new ArrayList<>();
        history.add(mockHistory(1, "2025-05-01 12:34:56"));
        history.add(mockHistory(2, "2025-05-01 22:34:56"));
        history.add(mockHistory(3, "2025-05-02 12:34:56"));
        history.add(mockHistory(4, "2025-05-03 12:34:56"));
        Mockito.when(table.history()).thenReturn(history);

        // create some refs
        HashMap<String, SnapshotRef> refs = new HashMap<>();
        String tag1 = "tag1";
        refs.put(tag1, SnapshotRef.tagBuilder(1).build());
        String branch1 = "branch1";
        refs.put(branch1, SnapshotRef.branchBuilder(1).build());
        String branch2 = "branch2";
        refs.put(branch2, SnapshotRef.branchBuilder(3).build());
        Mockito.when(table.refs()).thenReturn(refs);

        // query tag1
        assertQuerySpecSnapshotByVersionOf(table, tag1, 1, 0, tag1);
        assertQuerySpecSnapshotByAtTagMap(table, tag1, 1, 0, tag1);
        assertQuerySpecSnapshotByAtTagList(table, tag1, 1, 0, tag1);

        // query branch1
        assertQuerySpecSnapshotByVersionOf(table, branch1, 1, 2, branch1);
        assertQuerySpecSnapshotByAtBranchMap(table, branch1, 1, 2, branch1);
        assertQuerySpecSnapshotByAtBranchList(table, branch1, 1, 2, branch1);

        // query branch2
        assertQuerySpecSnapshotByVersionOf(table, branch2, 3, 2, branch2);
        assertQuerySpecSnapshotByAtBranchMap(table, branch2, 3, 2, branch2);
        assertQuerySpecSnapshotByAtBranchList(table, branch2, 3, 2, branch2);

        // query snapshotId 1
        assertQuerySpecSnapshotByVersionOf(table, "1", 1, 0, null);

        // query snapshotId 2
        assertQuerySpecSnapshotByVersionOf(table, "2", 2, 0, null);

        // query snapshotId 3
        assertQuerySpecSnapshotByVersionOf(table, "3", 3, 1, null);

        // query ref not exists
        Assert.assertThrows(
                UserException.class,
                () -> assertQuerySpecSnapshotByVersionOf(table, "ref_not_exists", -1, -1, null));

        // query snapshotId not exists
        Assert.assertThrows(
                UserException.class,
                () -> assertQuerySpecSnapshotByVersionOf(table, "99", -3, -1, null));

        // query branch not exists
        Assert.assertThrows(
                UserException.class,
                () -> assertQuerySpecSnapshotByAtBranchMap(table, "branch_not_exists", -3, -1, null));

        // query tag not exists
        Assert.assertThrows(
                UserException.class,
                () -> assertQuerySpecSnapshotByAtTagMap(table, "tag_not_exists", -3, -1, null));

        // query tag with @branch
        Assert.assertThrows(
                UserException.class,
                () -> assertQuerySpecSnapshotByAtBranchMap(table, tag1, -3, -1, null));

        // query branch with @tag
        Assert.assertThrows(
                UserException.class,
                () -> assertQuerySpecSnapshotByAtTagMap(table, branch1, -3, -1, null));
        Assert.assertThrows(
                UserException.class,
                () -> assertQuerySpecSnapshotByAtTagMap(table, branch2, -3, -1, null));

        // query version with tag
        Assert.assertThrows(
                IllegalArgumentException.class,
                () -> IcebergUtils.getQuerySpecSnapshot(
                    table,
                    Optional.of(TableSnapshot.timeOf("v1")),
                    Optional.of(new TableScanParams("tag", null,
                            new ArrayList<String>() {{
                                    add("v1");
                                }
                            }))
                ));

        // query version with branch
        Assert.assertThrows(
                IllegalArgumentException.class,
                () -> IcebergUtils.getQuerySpecSnapshot(
                    table,
                    Optional.of(TableSnapshot.timeOf("v1")),
                    Optional.of(new TableScanParams("branch", null,
                            new ArrayList<String>() {{
                                    add("v1");
                                }
                            }))
                ));

        // query branch with invalid param
        Assert.assertThrows(
                IllegalArgumentException.class,
                () -> IcebergUtils.getQuerySpecSnapshot(
                        table,
                        Optional.empty(),
                        Optional.of(new TableScanParams("branch",
                                    new ArrayMap<String, String>() {{
                                        put("k1", "k2");
                                    }
                                },
                                null))
                ));

        // query time
        assertQuerySpecSnapshotByTimeOf(table, "2025-05-01 12:34:56", 1, 0, null);
        assertQuerySpecSnapshotByTimeOf(table, "2025-05-01 14:34:56", 1, 0, null);
        assertQuerySpecSnapshotByTimeOf(table, "2025-05-02 11:34:56", 2, 0, null);
        assertQuerySpecSnapshotByTimeOf(table, "2025-05-02 12:34:56", 3, 1, null);
        assertQuerySpecSnapshotByTimeOf(table, "2025-05-03 12:34:56", 4, 1, null);

        // query invalid time format
        Assert.assertThrows(
                DateTimeException.class,
                () -> assertQuerySpecSnapshotByTimeOf(table, "1212-240", 3, 1, null)
        );

        // query invalid time
        Assert.assertThrows(
                IllegalArgumentException.class,
                () -> assertQuerySpecSnapshotByTimeOf(table, "2025-05-01 12:34:55", 3, 1, null)
        );
    }

    private Snapshot mockSnapshot(long snapshotId, int schemaId) {
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Mockito.when(snapshot.snapshotId()).thenReturn(snapshotId);
        Mockito.when(snapshot.schemaId()).thenReturn(schemaId);
        return snapshot;
    }

    private HistoryEntry mockHistory(long snapshotId, String time) {
        HistoryEntry historyEntry = Mockito.mock(HistoryEntry.class);
        Mockito.when(historyEntry.snapshotId()).thenReturn(snapshotId);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime dateTime = LocalDateTime.parse(time, formatter);
        long millis = dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

        Mockito.when(historyEntry.timestampMillis()).thenReturn(millis);
        return historyEntry;
    }

    private Schema mockSchemaWithId(int id) {
        Schema schema = Mockito.mock(Schema.class);
        Mockito.when(schema.schemaId()).thenReturn(id);
        return schema;
    }

    // select * from tb for version as of ...
    private void assertQuerySpecSnapshotByVersionOf(
            Table table,
            String version,
            long expectSnapshotId,
            int expectSchemaId,
            String expectRef) throws UserException {
        Optional<TableSnapshot> tableSnapshot = Optional.of(TableSnapshot.versionOf(version));
        IcebergTableQueryInfo queryInfo = IcebergUtils.getQuerySpecSnapshot(table, tableSnapshot, Optional.empty());
        assertQueryInfo(queryInfo, expectSnapshotId, expectSchemaId, expectRef);
    }

    // select * from tb for time as of ...
    private void assertQuerySpecSnapshotByTimeOf(
            Table table,
            String version,
            long expectSnapshotId,
            int expectSchemaId,
            String expectRef) throws UserException {
        Optional<TableSnapshot> tableSnapshot = Optional.of(TableSnapshot.timeOf(version));
        IcebergTableQueryInfo queryInfo = IcebergUtils.getQuerySpecSnapshot(table, tableSnapshot, Optional.empty());
        assertQueryInfo(queryInfo, expectSnapshotId, expectSchemaId, expectRef);
    }

    // select * from abc@tag('name'='tag_name')
    private void assertQuerySpecSnapshotByAtTagMap(
            Table table,
            String version,
            long expectSnapshotId,
            int expectSchemaId,
            String expectRef) throws UserException {
        HashMap<String, String> map = new HashMap<>();
        map.put("name", version);
        TableScanParams tsp = new TableScanParams("tag", map, null);
        IcebergTableQueryInfo queryInfo = IcebergUtils.getQuerySpecSnapshot(table, Optional.empty(), Optional.of(tsp));
        assertQueryInfo(queryInfo, expectSnapshotId, expectSchemaId, expectRef);
    }

    // select * from abc@tag(tag_name)
    private void assertQuerySpecSnapshotByAtTagList(
            Table table,
            String version,
            long expectSnapshotId,
            int expectSchemaId,
            String expectRef) throws UserException {
        List<String> list = new ArrayList<>();
        list.add(version);
        TableScanParams tsp = new TableScanParams("tag", null, list);
        IcebergTableQueryInfo queryInfo = IcebergUtils.getQuerySpecSnapshot(table, Optional.empty(), Optional.of(tsp));
        assertQueryInfo(queryInfo, expectSnapshotId, expectSchemaId, expectRef);
    }

    // select * from abc@branch('name'='branch_name')
    private void assertQuerySpecSnapshotByAtBranchMap(
            Table table,
            String version,
            long expectSnapshotId,
            int expectSchemaId,
            String expectRef) throws UserException {
        HashMap<String, String> map = new HashMap<>();
        map.put("name", version);
        TableScanParams tsp = new TableScanParams("branch", map, null);
        IcebergTableQueryInfo queryInfo = IcebergUtils.getQuerySpecSnapshot(table, Optional.empty(), Optional.of(tsp));
        assertQueryInfo(queryInfo, expectSnapshotId, expectSchemaId, expectRef);
    }

    // select * from abc@branch(branch_name)
    private void assertQuerySpecSnapshotByAtBranchList(
            Table table,
            String version,
            long expectSnapshotId,
            int expectSchemaId,
            String expectRef) throws UserException {
        List<String> list = new ArrayList<>();
        list.add(version);
        TableScanParams tsp = new TableScanParams("branch", null, list);
        IcebergTableQueryInfo queryInfo = IcebergUtils.getQuerySpecSnapshot(table, Optional.empty(), Optional.of(tsp));
        assertQueryInfo(queryInfo, expectSnapshotId, expectSchemaId, expectRef);
    }

    private void assertQueryInfo(
            IcebergTableQueryInfo queryInfo,
            long expectSnapshotId,
            int expectSchemaId,
            String expectRef) {
        Assert.assertEquals(expectSnapshotId, queryInfo.getSnapshotId());
        Assert.assertEquals(expectSchemaId, queryInfo.getSchemaId());
        Assert.assertEquals(expectRef, queryInfo.getRef());
    }
}
