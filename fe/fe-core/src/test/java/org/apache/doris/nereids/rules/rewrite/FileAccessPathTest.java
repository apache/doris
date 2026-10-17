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
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.rules.rewrite.AccessPathExpressionCollector.CollectAccessPathResult;
import org.apache.doris.nereids.rules.rewrite.NestedColumnPruning.DataTypeAccessTree;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Length;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.FileType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

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
    public void testJsonCastNeedsAllSixChildren() {
        SlotReference file = slot("f", FileType.INSTANCE, true);
        AccessPathInfo info = prune(file, new Cast(file, JsonType.INSTANCE), field(file, "size"));
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
    public void testFileStringFieldMetadataPaths() {
        SlotReference file = slot("f", FileType.INSTANCE, true);
        for (String name : ImmutableList.of("uri", "content_type", "checksum")) {
            Expression getter = field(file, name);
            AccessPathInfo info = prune(file, new Length(getter));
            Assertions.assertEquals(ImmutableList.of(meta("f", name, "OFFSET")), info.getAllAccessPaths());
            Assertions.assertSame(FileType.INSTANCE, info.getPrunedType());
            Assertions.assertEquals(ImmutableList.of(meta("f", name, "NULL")),
                    prune(file, new IsNull(getter)).getAllAccessPaths());
            Assertions.assertEquals(ImmutableList.of(data("f", name)),
                    prune(file, collect(file, false, true, new Length(getter))).getAllAccessPaths());
        }
    }

    @Test
    public void testMetadataDoesNotCrossFileConversionCasts() {
        SlotReference file = slot("f", FileType.INSTANCE, true);
        Expression identityField = field(new Cast(file, FileType.INSTANCE), "uri");
        Assertions.assertEquals(ImmutableList.of(meta("f", "uri", "OFFSET")),
                prune(file, new Length(identityField)).getAllAccessPaths());
        Assertions.assertEquals(ImmutableList.of(meta("f", "uri", "NULL")),
                prune(file, new IsNull(identityField)).getAllAccessPaths());

        Expression structField = field(new Cast(file, FileType.INSTANCE.publicStructType(), true), "uri");
        for (Expression expression : ImmutableList.of(new Length(structField), new IsNull(structField))) {
            AccessPathInfo info = prune(file, expression);
            Assertions.assertEquals(ImmutableList.of(data("f")), info.getAllAccessPaths());
            Assertions.assertSame(FileType.INSTANCE, info.getPrunedType());
        }
        SlotReference struct = slot("s", FileType.INSTANCE.publicStructType(), true);
        Expression fileField = field(new Cast(struct, FileType.INSTANCE, true), "uri");
        for (Expression expression : ImmutableList.of(new Length(fileField), new IsNull(fileField))) {
            AccessPathInfo info = prune(struct, expression);
            Assertions.assertEquals(ImmutableList.of(data("s")), info.getAllAccessPaths());
            Assertions.assertEquals(struct.getDataType(), info.getPrunedType());
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
            Expression accessed = TypeCoercionUtils.processBoundFunction(new ElementAt(
                    new Cast(file, FileType.INSTANCE.publicStructType(), true), selector));
            Assertions.assertTrue(accessed.child(0) instanceof Cast);
            Assertions.assertEquals(ImmutableList.of(data("f")), prune(file, accessed).getAllAccessPaths());
            Assertions.assertEquals(ImmutableList.of(data("f")),
                    prune(file, new IsNull(accessed)).getAllAccessPaths());
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
