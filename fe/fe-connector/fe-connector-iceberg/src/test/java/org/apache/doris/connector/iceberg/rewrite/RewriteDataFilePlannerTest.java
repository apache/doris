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

package org.apache.doris.connector.iceberg.rewrite;

import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorBetween;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.ConnectorOr;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Offline planning-half tests for {@link RewriteDataFilePlanner} (P6.4-T05). Uses a real
 * {@link InMemoryCatalog} (no Mockito) with multiple data files of controlled sizes/partitions, asserting the
 * SDK planning behaviour ported from fe-core: current-snapshot pin, partition grouping, bin-pack by group
 * size, the file-level and group-level rewrite filters, and the {@code WHERE} conversion through the shared
 * conflict-mode {@link org.apache.doris.connector.iceberg.IcebergPredicateConverter}. The execution half
 * (the distributed INSERT-SELECT) stays in fe-core and is out of scope here.
 */
public class RewriteDataFilePlannerTest {

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));
    private static final PartitionSpec BY_ID = PartitionSpec.builderFor(SCHEMA).identity("id").build();

    // Default knobs: 512MB target, no min/max bounds (every file in range), rewrite-all OFF, huge group cap,
    // delete filters effectively disabled. Individual tests override what they exercise.
    private static final long TARGET = 536870912L;

    private InMemoryCatalog catalog;

    @BeforeEach
    public void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
    }

    // ---- fixtures -------------------------------------------------------------------------------------------

    private Table createUnpartitioned(String name) {
        return catalog.createTable(TableIdentifier.of("db1", name), SCHEMA, PartitionSpec.unpartitioned());
    }

    private Table createPartitioned(String name) {
        return catalog.createTable(TableIdentifier.of("db1", name), SCHEMA, BY_ID);
    }

    private Table createV2Unpartitioned(String name) {
        return catalog.createTable(TableIdentifier.of("db1", name), SCHEMA, PartitionSpec.unpartitioned(),
                Collections.singletonMap(TableProperties.FORMAT_VERSION, "2"));
    }

    private static DataFile unpartFile(String path, long size) {
        return dataFileWithRecords(path, size, 100);
    }

    private static DataFile dataFileWithRecords(String path, long size, long records) {
        return DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("s3://b/db1/" + path)
                .withFileSizeInBytes(size)
                .withRecordCount(records)
                .withFormat(FileFormat.PARQUET)
                .build();
    }

    private static DeleteFile equalityDelete(String path) {
        return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofEqualityDeletes(1)   // field id 1 = "id"
                .withPath("s3://b/db1/" + path)
                .withFileSizeInBytes(128L)
                .withRecordCount(4L)
                .withFormat(FileFormat.PARQUET)
                .build();
    }

    private static DeleteFile positionDelete(String path, String referencedDataFile, long records) {
        return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofPositionDeletes()
                .withPath("s3://b/db1/" + path)
                .withFileSizeInBytes(128L)
                .withRecordCount(records)
                .withReferencedDataFile(referencedDataFile)   // file-scoped -> counts toward the delete ratio
                .withFormat(FileFormat.PARQUET)
                .build();
    }

    private static DataFile partFile(String path, long size, int idValue) {
        return DataFiles.builder(BY_ID)
                .withPath("s3://b/db1/" + path)
                .withFileSizeInBytes(size)
                .withRecordCount(100)
                .withPartitionPath("id=" + idValue)
                .withFormat(FileFormat.PARQUET)
                .build();
    }

    private static void append(Table table, DataFile... files) {
        org.apache.iceberg.AppendFiles append = table.newAppend();
        for (DataFile f : files) {
            append.appendFile(f);
        }
        append.commit();
    }

    private static RewriteDataFilePlanner.Parameters params(long minFileSize, long maxFileSize, int minInputFiles,
            boolean rewriteAll, long maxGroupSize, ConnectorPredicate where) {
        return paramsFull(minFileSize, maxFileSize, minInputFiles, rewriteAll, maxGroupSize,
                Integer.MAX_VALUE, 0.3, where);
    }

    private static RewriteDataFilePlanner.Parameters paramsFull(long minFileSize, long maxFileSize,
            int minInputFiles, boolean rewriteAll, long maxGroupSize, int deleteFileThreshold,
            double deleteRatioThreshold, ConnectorPredicate where) {
        return new RewriteDataFilePlanner.Parameters(TARGET, minFileSize, maxFileSize, minInputFiles, rewriteAll,
                maxGroupSize, deleteFileThreshold, deleteRatioThreshold, /* outputSpecId (dead) */ 2L, where);
    }

    private static RewriteDataFilePlanner.Parameters rewriteAll(long maxGroupSize, ConnectorPredicate where) {
        return params(0L, Long.MAX_VALUE, 5, true, maxGroupSize, where);
    }

    private static List<RewriteDataGroup> plan(Table table, RewriteDataFilePlanner.Parameters p) {
        return new RewriteDataFilePlanner(p, ZoneOffset.UTC).planAndOrganizeTasks(table);
    }

    private static int totalFiles(List<RewriteDataGroup> groups) {
        return groups.stream().mapToInt(RewriteDataGroup::getTaskCount).sum();
    }

    private static Set<Integer> partitionIds(List<RewriteDataGroup> groups) {
        Set<Integer> ids = new HashSet<>();
        for (RewriteDataGroup g : groups) {
            for (DataFile f : g.getDataFiles()) {
                ids.add(f.partition().get(0, Integer.class));
            }
        }
        return ids;
    }

    private static ConnectorPredicate where(ConnectorExpression expr) {
        return new ConnectorPredicate(expr);
    }

    private static ConnectorComparison idEq(int value) {
        return new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("id", ConnectorType.of("INT")),
                new ConnectorLiteral(ConnectorType.of("INT"), (long) value));
    }

    // ---- planning / grouping --------------------------------------------------------------------------------

    @Test
    public void rewriteAllGroupsEveryFile() {
        Table t = createUnpartitioned("t1");
        append(t, unpartFile("a", 100), unpartFile("b", 100), unpartFile("c", 100));

        // maxGroupSize 250 -> bin-pack packs [a,b]=200 then [c]=100 (a 3rd 100 would overflow 250).
        List<RewriteDataGroup> groups = plan(t, rewriteAll(250L, null));

        Assertions.assertEquals(2, groups.size());
        Assertions.assertEquals(3, totalFiles(groups));
        Assertions.assertTrue(groups.stream().allMatch(g -> g.getTotalSize() <= 250L));
    }

    @Test
    public void binPackNeverExceedsMaxGroupSize() {
        Table t = createUnpartitioned("t2");
        append(t, unpartFile("a", 100), unpartFile("b", 100), unpartFile("c", 100), unpartFile("d", 100));

        List<RewriteDataGroup> groups = plan(t, rewriteAll(250L, null));

        Assertions.assertEquals(2, groups.size());
        Assertions.assertEquals(4, totalFiles(groups));
        Assertions.assertTrue(groups.stream().allMatch(g -> g.getTotalSize() == 200L));
    }

    @Test
    public void groupsAreNeverMixedAcrossPartitions() {
        Table t = createPartitioned("t3");
        append(t, partFile("a", 100, 1), partFile("b", 100, 1), partFile("c", 100, 2));

        // Big cap -> each partition collapses into a single bin: id=1 -> 2 files, id=2 -> 1 file.
        List<RewriteDataGroup> groups = plan(t, rewriteAll(1_000_000L, null));

        Assertions.assertEquals(2, groups.size());
        Assertions.assertEquals(3, totalFiles(groups));
        Assertions.assertEquals(new HashSet<>(Arrays.asList(1, 2)), partitionIds(groups));
        // Each group is single-partition.
        for (RewriteDataGroup g : groups) {
            Set<Integer> ids = new HashSet<>();
            g.getDataFiles().forEach(f -> ids.add(f.partition().get(0, Integer.class)));
            Assertions.assertEquals(1, ids.size(), "a rewrite group must not span partitions");
        }
    }

    @Test
    public void usesCurrentSnapshotFileSet() {
        Table t = createUnpartitioned("t4");
        append(t, unpartFile("a", 100));   // snapshot 1
        append(t, unpartFile("b", 100));   // snapshot 2 -> current sees both a and b

        List<RewriteDataGroup> groups = plan(t, rewriteAll(1_000_000L, null));

        Assertions.assertEquals(2, totalFiles(groups));
    }

    @Test
    public void emptyTableYieldsNoGroups() {
        Table t = createUnpartitioned("t5");
        // No append -> no current snapshot. The planner must not NPE on the null snapshot.
        List<RewriteDataGroup> groups = plan(t, rewriteAll(1_000_000L, null));

        Assertions.assertTrue(groups.isEmpty());
    }

    // ---- file-level and group-level filters (rewriteAll = false) --------------------------------------------

    @Test
    public void fileFilterSelectsOutOfRangeFiles() {
        Table t = createUnpartitioned("t6");
        append(t, unpartFile("a", 100), unpartFile("b", 100), unpartFile("c", 100));

        // min-file-size 200 makes every 100-byte file "too small" -> selected; minInputFiles 2 keeps the
        // 3-file group via hasEnoughInputFiles.
        List<RewriteDataGroup> groups = plan(t, params(200L, 1000L, 2, false, 1_000_000L, null));

        Assertions.assertEquals(1, groups.size());
        Assertions.assertEquals(3, totalFiles(groups));
    }

    @Test
    public void fileFilterSkipsInRangeFiles() {
        Table t = createUnpartitioned("t7");
        append(t, unpartFile("a", 100), unpartFile("b", 100), unpartFile("c", 100));

        // 100 is within [50, 200] and there are no deletes -> nothing qualifies for rewrite.
        List<RewriteDataGroup> groups = plan(t, params(50L, 200L, 2, false, 1_000_000L, null));

        Assertions.assertTrue(groups.isEmpty());
    }

    @Test
    public void groupFilterDropsGroupBelowThresholds() {
        Table t = createUnpartitioned("t8");
        append(t, unpartFile("a", 100), unpartFile("b", 100));

        // Files are selected (100 < min 200) but the 2-file group fails every group predicate:
        // minInputFiles 5 (count 2 < 5), content 200 <= target 512MB, 200 <= group cap, no deletes.
        List<RewriteDataGroup> groups = plan(t, params(200L, 1000L, 5, false, 1_000_000L, null));

        Assertions.assertTrue(groups.isEmpty());
    }

    @Test
    public void groupFilterKeepsGroupWithEnoughInputFiles() {
        Table t = createUnpartitioned("t9");
        append(t, unpartFile("a", 100), unpartFile("b", 100));

        // Same as above but minInputFiles 2 -> hasEnoughInputFiles (2 > 1 && 2 >= 2) keeps the group.
        List<RewriteDataGroup> groups = plan(t, params(200L, 1000L, 2, false, 1_000_000L, null));

        Assertions.assertEquals(1, groups.size());
        Assertions.assertEquals(2, totalFiles(groups));
    }

    // ---- WHERE conversion (conflict-mode IcebergPredicateConverter) -----------------------------------------

    @Test
    public void whereOnPartitionColumnPrunesToMatchingPartition() {
        Table t = createPartitioned("t10");
        append(t, partFile("a", 100, 1), partFile("b", 100, 2));

        // WHERE id = 1 -> a convertible single-column comparison -> only the id=1 file survives planning.
        List<RewriteDataGroup> groups = plan(t, rewriteAll(1_000_000L, where(idEq(1))));

        Assertions.assertEquals(1, totalFiles(groups));
        Assertions.assertEquals(new HashSet<>(Collections.singletonList(1)), partitionIds(groups));
    }

    @Test
    public void unconvertibleCrossColumnOrThrows() {
        // User-signed decision (WS-REWRITE R7): a rewrite WHERE is a user-authored data-scope filter, so it must
        // be honored precisely or the rewrite fails. A cross-column OR is unrepresentable in the conflict matrix
        // and cannot be pushed to file pruning; rather than silently widening the scan to the whole table (the
        // earlier DV-T05r-where over-approximation), the planner now THROWS fail-loud.
        Table t = createPartitioned("t11");
        append(t, partFile("a", 100, 1), partFile("b", 100, 2));

        ConnectorExpression crossColumnOr = new ConnectorOr(Arrays.asList(
                idEq(1),
                new ConnectorComparison(ConnectorComparison.Operator.EQ,
                        new ConnectorColumnRef("name", ConnectorType.of("STRING")),
                        new ConnectorLiteral(ConnectorType.of("STRING"), "x"))));

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> plan(t, rewriteAll(1_000_000L, where(crossColumnOr))));
        Assertions.assertTrue(e.getMessage().contains("cannot be pushed down"),
                "an un-pushable rewrite WHERE must fail loud, not silently widen the scan to the whole table");
    }

    @Test
    public void partiallyPushableWhereThrows() {
        // A top-level AND with one pushable conjunct (id=1) and one un-pushable (cross-column OR). Keeping only
        // the pushable arm would widen the rewrite past the user's WHERE, so the planner fails when ANY top-level
        // conjunct cannot be pushed -- not only when nothing pushes. Guards the size-vs-count check (a weaker
        // "is anything pushable?" gate would wrongly let this through).
        Table t = createPartitioned("t19");
        append(t, partFile("a", 100, 1), partFile("b", 100, 2));

        ConnectorExpression crossColumnOr = new ConnectorOr(Arrays.asList(
                idEq(2),
                new ConnectorComparison(ConnectorComparison.Operator.EQ,
                        new ConnectorColumnRef("name", ConnectorType.of("STRING")),
                        new ConnectorLiteral(ConnectorType.of("STRING"), "x"))));
        ConnectorExpression partial = new ConnectorAnd(Arrays.asList(idEq(1), crossColumnOr));

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> plan(t, rewriteAll(1_000_000L, where(partial))));
        Assertions.assertTrue(e.getMessage().contains("cannot be pushed down"));
    }

    @Test
    public void whereBetweenPrunesViaConflictMode() {
        // BETWEEN is in the conflict matrix (-> id>=1 AND id<=1) but NOT the scan-pushdown matrix. This test
        // pins the user-signed Option-A choice: the planner must use IcebergPredicateConverter conflict mode.
        // If it used scan mode, BETWEEN would be dropped and BOTH partitions would survive (totalFiles == 2).
        Table t = createPartitioned("t12");
        append(t, partFile("a", 100, 1), partFile("b", 100, 2));

        ConnectorExpression between = new ConnectorBetween(
                new ConnectorColumnRef("id", ConnectorType.of("INT")),
                new ConnectorLiteral(ConnectorType.of("INT"), 1L),
                new ConnectorLiteral(ConnectorType.of("INT"), 1L));

        List<RewriteDataGroup> groups = plan(t, rewriteAll(1_000_000L, where(between)));

        Assertions.assertEquals(1, totalFiles(groups));
        Assertions.assertEquals(new HashSet<>(Collections.singletonList(1)), partitionIds(groups));
    }

    @Test
    public void whereTopLevelAndAppliesEveryConjunct() {
        // A top-level multi-conjunct ConnectorAnd (id>=1 AND id<=1) is flattened by the planner's
        // per-conjunct scan.filter loop: IcebergPredicateConverter.convert returns both convertible arms and
        // each is applied as a separate iceberg filter (iceberg ANDs them). Only the intersecting partition
        // id=1 survives. If the AND-flatten loop broke or dropped the lower-bound arm, the scan would widen to
        // both partitions (totalFiles == 2) -> red. Distinct from whereBetweenPrunesViaConflictMode, which
        // exercises a single ConnectorBetween node rather than the multi-filter flatten loop.
        Table t = createPartitioned("t18");
        append(t, partFile("a", 100, 1), partFile("b", 100, 2));

        ConnectorColumnRef idRef = new ConnectorColumnRef("id", ConnectorType.of("INT"));
        ConnectorExpression and = new ConnectorAnd(Arrays.asList(
                new ConnectorComparison(ConnectorComparison.Operator.GE, idRef,
                        new ConnectorLiteral(ConnectorType.of("INT"), 1L)),
                new ConnectorComparison(ConnectorComparison.Operator.LE, idRef,
                        new ConnectorLiteral(ConnectorType.of("INT"), 1L))));

        List<RewriteDataGroup> groups = plan(t, rewriteAll(1_000_000L, where(and)));

        Assertions.assertEquals(1, totalFiles(groups));
        Assertions.assertEquals(new HashSet<>(Collections.singletonList(1)), partitionIds(groups));
    }

    // ---- group-predicate OR-arms in isolation (rewriteAll = false) ------------------------------------------

    @Test
    public void groupFilterKeepsViaEnoughContent() {
        Table t = createUnpartitioned("t13");
        append(t, unpartFile("a", 100), unpartFile("b", 100));

        // Files selected (100 < min 200). Group: count 2, size 200. minInputFiles 100 disables hasEnoughInputFiles;
        // hasEnoughContent fires alone (count > 1 && size 200 > target 150); size 200 <= group cap so hasTooMuchContent off.
        List<RewriteDataGroup> groups = new RewriteDataFilePlanner(
                new RewriteDataFilePlanner.Parameters(150L, 200L, 1000L, 100, false, 1_000_000L,
                        Integer.MAX_VALUE, 0.3, 2L, null), ZoneOffset.UTC).planAndOrganizeTasks(t);

        Assertions.assertEquals(1, groups.size());
        Assertions.assertEquals(2, totalFiles(groups));
    }

    @Test
    public void groupFilterKeepsViaTooMuchContent() {
        Table t = createUnpartitioned("t14");
        append(t, unpartFile("a", 2000));

        // One oversized file: selected (2000 > max 1000), packed alone into a 2000-byte group that exceeds the
        // 1500 group cap -> hasTooMuchContent fires (the only true OR-arm; count 1 disables the count/content arms).
        List<RewriteDataGroup> groups = plan(t, params(0L, 1000L, 100, false, 1500L, null));

        Assertions.assertEquals(1, groups.size());
        Assertions.assertEquals(1, totalFiles(groups));
    }

    @Test
    public void fileAtSizeBoundariesIsInRange() {
        Table t = createUnpartitioned("t15");
        append(t, unpartFile("a", 200), unpartFile("b", 1000));

        // outsideDesiredFileSizeRange is strict (< min || > max). A file exactly at min or max is IN range, so
        // with no deletes nothing qualifies. A port using <= / >= would wrongly select both -> red.
        List<RewriteDataGroup> groups = plan(t, params(200L, 1000L, 2, false, 1_000_000L, null));

        Assertions.assertTrue(groups.isEmpty());
    }

    // ---- delete-file filters (restores the legacy testDeleteFileThreshold / testDeleteRatioThreshold parity) -

    @Test
    public void deleteFileThresholdGatesSelection() {
        Table t = createV2Unpartitioned("t16");
        append(t, unpartFile("a", 100));                      // f1 (data sequence 1), size in [50,200] -> never size-selected
        t.newRowDelta()
                .addDeletes(equalityDelete("d1.parquet"))
                .addDeletes(equalityDelete("d2.parquet"))
                .commit();                                     // 2 equality deletes (sequence 2) apply to f1

        // threshold 2 -> 2 >= 2 -> f1 selected via tooManyDeletes; group kept via hasDeleteIssues.
        Assertions.assertEquals(1, plan(t, paramsFull(50L, 200L, 5, false, 1_000_000L, 2, 0.3, null)).size());
        // threshold 3 -> 2 >= 3 false; equality deletes are not file-scoped so the ratio is 0 -> nothing selected.
        Assertions.assertTrue(plan(t, paramsFull(50L, 200L, 5, false, 1_000_000L, 3, 0.3, null)).isEmpty());
    }

    @Test
    public void deleteRatioGatesSelection() {
        Table t = createV2Unpartitioned("t17");
        DataFile f1 = dataFileWithRecords("a", 100, 10);
        t.newAppend().appendFile(f1).commit();                // f1: 10 records
        t.newRowDelta()
                .addDeletes(positionDelete("pos.parquet", f1.path().toString(), 4L))
                .commit();                                     // file-scoped position delete: 4 deleted records -> ratio 0.4

        // ratio threshold 0.3 -> 0.4 >= 0.3 -> f1 selected via tooHighDeleteRatio; threshold disables tooManyDeletes.
        Assertions.assertEquals(1,
                plan(t, paramsFull(50L, 200L, 5, false, 1_000_000L, Integer.MAX_VALUE, 0.3, null)).size());
        // ratio threshold 0.5 -> 0.4 >= 0.5 false -> nothing selected.
        Assertions.assertTrue(
                plan(t, paramsFull(50L, 200L, 5, false, 1_000_000L, Integer.MAX_VALUE, 0.5, null)).isEmpty());
    }
}
