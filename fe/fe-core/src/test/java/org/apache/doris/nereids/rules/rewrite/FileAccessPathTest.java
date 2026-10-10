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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.analysis.ColumnAccessPath;
import org.apache.doris.analysis.ColumnAccessPathType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.rules.rewrite.AccessPathExpressionCollector.CollectAccessPathResult;
import org.apache.doris.nereids.rules.rewrite.NestedColumnPruning.DataTypeAccessTree;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.FileDataSize;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.FileType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class FileAccessPathTest {
    @Test
    public void testStatisticsSizeNeedsAllSixChildren() {
        SlotReference file = slot("f", FileType.INSTANCE, true);
        AccessPathInfo info = prune(file, new FileDataSize(file), field(file, "size"));
        Assertions.assertEquals(ImmutableList.of(data("f")), info.getAllAccessPaths());
        Assertions.assertSame(FileType.INSTANCE, info.getPrunedType());
        Assertions.assertEquals(6, ((FileType) info.getPrunedType()).getFields().size());
    }

    @Test
    public void testCanonicalChildrenAndGetterPaths() {
        SlotReference file = slot("f", FileType.INSTANCE, false);
        for (String name : ImmutableList.of("uri", "offset", "size", "content_type", "checksum", "inline")) {
            AccessPathInfo info = prune(file, field(file, name));
            Assertions.assertEquals(ImmutableList.of(data("f", name)), info.getAllAccessPaths());
            Assertions.assertSame(FileType.INSTANCE, info.getPrunedType());
        }
        DataTypeAccessTree tree = DataTypeAccessTree.of(FileType.INSTANCE, ColumnAccessPathType.DATA);
        tree.setAccessByPath(ImmutableList.of("size"), 0, ColumnAccessPathType.DATA);
        Assertions.assertSame(FileType.INSTANCE, tree.pruneDataType().orElseThrow());
        Assertions.assertEquals(ImmutableList.of("uri", "offset", "size", "content_type", "checksum", "inline"),
                new ArrayList<>(tree.getChildren().keySet()));
        Assertions.assertEquals(6, ((FileType) tree.getType()).getFields().size());
        Assertions.assertTrue(FileType.INSTANCE.getFields().stream().allMatch(StructField::isNullable));
    }

    @Test
    public void testParentAndChildNullPaths() {
        SlotReference file = slot("f", FileType.INSTANCE, true);
        for (Expression expression : ImmutableList.of(new IsNull(file), new Not(new IsNull(file)))) {
            Assertions.assertEquals(ImmutableList.of(meta("f", "NULL")), prune(file, expression).getAllAccessPaths());
        }
        for (Expression expression : ImmutableList.of(new IsNull(field(file, "size")))) {
            Assertions.assertEquals(ImmutableList.of(meta("f", "size", "NULL")),
                    prune(file, expression).getAllAccessPaths());
        }
    }

    @Test
    public void testNoSyntheticPhysicalNullStream() {
        SlotReference file = slot("f", FileType.INSTANCE, false);
        SlotReference outerJoinFile = file.withNullable(true);
        Assertions.assertTrue(collect(outerJoinFile, false, false, new IsNull(outerJoinFile)).isEmpty());
        Assertions.assertEquals(1, collect(outerJoinFile, false, false, new Count(outerJoinFile)).size());
        Assertions.assertEquals(ImmutableList.of(data("f")), prune(file, new Count(file)).getAllAccessPaths());
        Assertions.assertEquals(ImmutableList.of(meta("f", "size", "NULL")),
                prune(file, new IsNull(field(file, "size"))).getAllAccessPaths());
        SlotReference unknown = new SlotReference("unknown", FileType.INSTANCE, true);
        Assertions.assertTrue(collect(unknown, false, false, new IsNull(unknown)).isEmpty());
    }

    @Test
    public void testCountThroughStructFileAlias() {
        assertCountAliasPaths(new StructType(ImmutableList.of(new StructField("f", FileType.INSTANCE, true, ""))),
                new StringLiteral("f"), data("c", "f"));
    }

    @Test
    public void testCountThroughArrayFileAlias() {
        assertCountAliasPaths(ArrayType.of(FileType.INSTANCE), new IntegerLiteral(1), data("c", "*"));
    }

    @Test
    public void testCountThroughMapFileAlias() {
        assertCountAliasPaths(MapType.of(StringType.INSTANCE, FileType.INSTANCE), new StringLiteral("key"),
                data("c", "KEYS"), data("c", "VALUES"));
    }

    private void assertCountAliasPaths(DataType type, Expression selector, ColumnAccessPath... expected) {
        OlapTable table = new OlapTable(100L, "files",
                ImmutableList.of(new Column("c", type.toCatalogDataType(), true)),
                KeysType.DUP_KEYS, new PartitionInfo(), null);
        table.setIndexMeta(-1, "files", table.getFullSchema(), 0, 0, (short) 0,
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        LogicalOlapScan scan = new LogicalOlapScan(RelationId.createGenerator().getNextId(), table);
        SlotReference source = (SlotReference) scan.getOutput().get(0);
        Alias nested = new Alias(new ElementAt(source, selector), "nested_file");
        Alias renamed = new Alias(nested.toSlot(), "renamed_file");
        LogicalProject<?> project = new LogicalProject<>(ImmutableList.of(renamed),
                new LogicalProject<>(ImmutableList.of(nested), scan));
        LogicalAggregate<?> count = new LogicalAggregate<>(ImmutableList.of(),
                ImmutableList.of(new Alias(new Count(renamed.toSlot()))), project);
        Map<Slot, List<CollectAccessPathResult>> paths = new AccessPathPlanCollector().collect(count, null);
        Assertions.assertEquals(new TreeSet<>(ImmutableList.copyOf(expected)),
                new TreeSet<>(prune(source, paths.get(source)).getAllAccessPaths()));

        LogicalProject<?> full = new LogicalProject<>(ImmutableList.of(renamed.toSlot()), project);
        paths = new AccessPathPlanCollector().collect(full, null);
        List<ColumnAccessPath> fullPaths = prune(source, paths.get(source)).getAllAccessPaths();
        for (ColumnAccessPath path : expected) {
            // Metadata may coexist with DATA, but the full-value consumer must demand every
            // FILE child at that path (and MAP keys needed by the lookup).
            ColumnAccessPath fullPath = path.getType() == ColumnAccessPathType.META
                    ? ColumnAccessPath.data(path.getPath().subList(0, path.getPath().size() - 1)) : path;
            Assertions.assertTrue(fullPaths.contains(fullPath), fullPaths.toString());
        }
    }

    @Test
    public void testNestedFilePaths() {
        SlotReference array = slot("a", ArrayType.of(FileType.INSTANCE), true);
        Expression item = new ElementAt(array, new IntegerLiteral(1));
        Assertions.assertEquals(ImmutableList.of(data("a", "*", "uri")),
                prune(array, field(item, "uri")).getAllAccessPaths());
        Assertions.assertSame(FileType.INSTANCE, ((ArrayType) prune(array, field(item, "uri"))
                .getPrunedType()).getItemType());
        Assertions.assertEquals(ImmutableList.of(meta("a", "*", "NULL")),
                prune(array, new IsNull(item)).getAllAccessPaths());

        SlotReference struct = slot("s", new StructType(ImmutableList.of(
                new StructField("f", FileType.INSTANCE, true, ""),
                new StructField("unused", IntegerType.INSTANCE, true, ""))), true);
        Expression nested = field(struct, "f");
        AccessPathInfo info = prune(struct, field(nested, "uri"));
        Assertions.assertEquals(ImmutableList.of(data("s", "f", "uri")), info.getAllAccessPaths());
        Assertions.assertSame(FileType.INSTANCE, ((StructType) info.getPrunedType()).getField("f").getDataType());
        Assertions.assertEquals(ImmutableList.of(meta("s", "f", "NULL")),
                prune(struct, new IsNull(nested)).getAllAccessPaths());
    }

    @Test
    public void testMapLookupKeepsKeysAndRoutesFileValues() {
        SlotReference map = slot("m", MapType.of(StringType.INSTANCE, FileType.INSTANCE), true);
        Expression value = new ElementAt(map, new StringLiteral("key"));
        AccessPathInfo info = prune(map, field(value, "uri"));
        Assertions.assertEquals(new TreeSet<>(ImmutableList.of(data("m", "KEYS"), data("m", "VALUES", "uri"))),
                new TreeSet<>(info.getAllAccessPaths()));
        Assertions.assertSame(FileType.INSTANCE, ((MapType) info.getPrunedType()).getValueType());
        Assertions.assertEquals(new TreeSet<>(ImmutableList.of(data("m", "KEYS"), meta("m", "VALUES", "NULL"))),
                new TreeSet<>(prune(map, new IsNull(value)).getAllAccessPaths()));
    }

    @Test
    public void testFullConsumerDominatesGetterAndMetadata() {
        SlotReference file = slot("f", FileType.INSTANCE, true);
        AccessPathInfo info = prune(file, file, field(file, "uri"), new Count(file));
        Assertions.assertEquals(ImmutableList.of(data("f")), info.getAllAccessPaths());
        Assertions.assertSame(FileType.INSTANCE, info.getPrunedType());

        List<CollectAccessPathResult> paths = collect(file, false, false, file);
        paths.addAll(collect(file, true, false, new IsNull(field(file, "size"))));
        info = prune(file, paths);
        Assertions.assertTrue(info.getAllAccessPaths().contains(data("f")));
        Assertions.assertEquals(ImmutableList.of(meta("f", "size", "NULL")), info.getPredicateAccessPaths());
        Assertions.assertSame(FileType.INSTANCE, info.getPrunedType());
    }

    @Test
    public void testNestedFullConsumerPreservesFileIdentity() {
        SlotReference struct = slot("s", new StructType(ImmutableList.of(
                new StructField("f", FileType.INSTANCE, true, ""))), true);
        Expression file = field(struct, "f");
        AccessPathInfo info = prune(struct, file, field(file, "size"), new IsNull(file));
        Assertions.assertTrue(info.getAllAccessPaths().contains(data("s", "f")));
        Assertions.assertSame(FileType.INSTANCE, ((StructType) info.getPrunedType()).getField("f").getDataType());
        DataTypeAccessTree tree = DataTypeAccessTree.ofRoot(struct, ColumnAccessPathType.DATA);
        for (ColumnAccessPath path : info.getAllAccessPaths()) {
            tree.setAccessByPath(path.getPath(), 0, path.getType());
        }
        Assertions.assertTrue(tree.getChildren().get("s").getChildren().get("f").isAccessAll());
    }

    @Test
    public void testFileCastsDoNotBecomeStructShapePruning() {
        SlotReference file = slot("f", FileType.INSTANCE, true);
        Assertions.assertEquals(ImmutableList.of(data("f", "size")),
                prune(file, field(new Cast(file, FileType.INSTANCE), "size")).getAllAccessPaths());
        Assertions.assertEquals(ImmutableList.of(data("f")),
                prune(file, field(new Cast(file, FileType.INSTANCE.publicStructType(), true), "size"))
                        .getAllAccessPaths());
        for (Expression selector : ImmutableList.of(new StringLiteral("size"), new IntegerLiteral(3))) {
            Expression rewritten = TypeCoercionUtils.processBoundFunction(new ElementAt(
                    new Cast(file, FileType.INSTANCE.publicStructType(), true), selector));
            Assertions.assertEquals(ImmutableList.of(data("f", "size")), prune(file, rewritten).getAllAccessPaths());
            Assertions.assertEquals(ImmutableList.of(meta("f", "size", "NULL")),
                    prune(file, new IsNull(rewritten)).getAllAccessPaths());
        }
        SlotReference struct = slot("s", FileType.INSTANCE.publicStructType(), true);
        Assertions.assertEquals(ImmutableList.of(data("s")),
                prune(struct, field(new Cast(struct, FileType.INSTANCE, true), "size")).getAllAccessPaths());
    }

    @Test
    public void testMvFragmentSkipsCountMetadataAndDistinctNeedsData() {
        SlotReference file = slot("f", FileType.INSTANCE, true);
        List<CollectAccessPathResult> paths = collect(file, false, true, new Count(file));
        Assertions.assertEquals(ImmutableList.of(data("f")), prune(file, paths).getAllAccessPaths());
        // DISTINCT FILE is rejected during analysis; the collector must never treat it as a null-only count.
        Assertions.assertEquals(ImmutableList.of(data("f")), prune(file, new Count(true, file)).getAllAccessPaths());
    }

    private static ElementAt field(Expression value, String name) {
        return new ElementAt(value, new StringLiteral(name));
    }

    private static SlotReference slot(String name, DataType type, boolean nullable) {
        return new SlotReference(name, type, nullable).withColumn(new Column(name, type.toCatalogDataType(), nullable));
    }

    private static List<CollectAccessPathResult> collect(SlotReference slot, boolean predicate,
            boolean skipMeta, Expression... expressions) {
        Multimap<Integer, CollectAccessPathResult> paths = ArrayListMultimap.create();
        AccessPathExpressionCollector collector = new AccessPathExpressionCollector(null, paths, predicate, skipMeta);
        for (Expression expression : expressions) {
            collector.collect(expression);
        }
        return new ArrayList<>(paths.get(slot.getExprId().asInt()));
    }

    private static AccessPathInfo prune(SlotReference slot, Expression... expressions) {
        return prune(slot, collect(slot, false, false, expressions));
    }

    private static AccessPathInfo prune(SlotReference slot, List<CollectAccessPathResult> paths) {
        Map<Slot, List<CollectAccessPathResult>> inputs = ImmutableMap.of(slot, paths);
        Map<Integer, AccessPathInfo> result = Deencapsulation.invoke(
                NestedColumnPruning.class, "pruneDataType", inputs);
        return result.get(slot.getExprId().asInt());
    }

    private static ColumnAccessPath data(String... path) {
        return ColumnAccessPath.data(ImmutableList.copyOf(path));
    }

    private static ColumnAccessPath meta(String... path) {
        return ColumnAccessPath.meta(ImmutableList.copyOf(path));
    }
}
