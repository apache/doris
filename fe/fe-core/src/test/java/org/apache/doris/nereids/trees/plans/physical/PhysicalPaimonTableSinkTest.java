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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.nereids.properties.DistributionSpecPaimonHashDynamic;
import org.apache.doris.nereids.properties.DistributionSpecPaimonTableSinkHashPartitioned;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.VarCharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class PhysicalPaimonTableSinkTest {

    @Test
    void testFixedAppendCompactionRequiresSingleWriter() {
        Assertions.assertTrue(PhysicalPaimonTableSink.requiresSingleWriter(
                table(BucketMode.HASH_FIXED, Collections.emptyList(), Collections.emptyMap())));
        Assertions.assertFalse(PhysicalPaimonTableSink.requiresSingleWriter(
                table(BucketMode.HASH_FIXED, Collections.emptyList(),
                        Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"))));
    }

    @Test
    void testFixedPrimaryKeyAndDynamicModesRequireSingleWriter() {
        Assertions.assertTrue(PhysicalPaimonTableSink.requiresSingleWriter(
                table(BucketMode.HASH_FIXED, Collections.singletonList("id"),
                        Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"))));
        Assertions.assertTrue(PhysicalPaimonTableSink.requiresSingleWriter(
                table(BucketMode.HASH_DYNAMIC, Collections.singletonList("id"),
                        Collections.emptyMap())));
        Assertions.assertTrue(PhysicalPaimonTableSink.requiresSingleWriter(
                table(BucketMode.KEY_DYNAMIC, Collections.singletonList("id"),
                        Collections.emptyMap())));
        Assertions.assertFalse(PhysicalPaimonTableSink.requiresSingleWriter(
                table(BucketMode.BUCKET_UNAWARE, Collections.emptyList(),
                        Collections.emptyMap())));
    }

    @Test
    void testSupportedFixedBucketBuildsPaimonIdentityDistribution() {
        FileStoreTable table = fixedBucketTable(
                ImmutableList.of(
                        new DataField(0, "id", new IntType()),
                        new DataField(1, "part", new VarCharType())),
                Collections.singletonList("part"), Collections.emptyMap());
        Slot id = new SlotReference("id", IntegerType.INSTANCE);
        Slot part = new SlotReference("part", StringType.INSTANCE);

        DistributionSpecPaimonTableSinkHashPartitioned spec
                = PhysicalPaimonTableSink.buildFixedBucketDistributionSpec(
                        table,
                        ImmutableList.of(
                                new Column("id", PrimitiveType.INT),
                                new Column("part", PrimitiveType.STRING)),
                        ImmutableList.of(id, part));

        Assertions.assertNotNull(spec);
        Assertions.assertEquals(4, spec.getNumBuckets());
        Assertions.assertEquals(ImmutableList.of(part.getExprId(), id.getExprId()),
                spec.getOutputColumnExprIds());
        Assertions.assertEquals(Collections.singletonList(0), spec.getPartitionFieldIndexes());
        Assertions.assertEquals(Collections.singletonList(1), spec.getBucketFieldIndexes());
        Assertions.assertEquals(
                DistributionSpecPaimonTableSinkHashPartitioned.WriterAssignment.IDENTITY,
                spec.getWriterAssignment());
    }

    @Test
    void testUnsupportedFixedBucketRouteFallsBack() {
        FileStoreTable unsupportedType = fixedBucketTable(
                Collections.singletonList(
                        new DataField(0, "id", new ArrayType(new IntType()))),
                Collections.emptyList(), Collections.emptyMap());
        Assertions.assertNull(PhysicalPaimonTableSink.buildFixedBucketDistributionSpec(
                unsupportedType,
                Collections.singletonList(new Column("id", PrimitiveType.INT)),
                Collections.singletonList(new SlotReference("id", IntegerType.INSTANCE))));

        FileStoreTable customBucket = fixedBucketTable(
                Collections.singletonList(new DataField(0, "id", new IntType())),
                Collections.emptyList(),
                Collections.singletonMap(CoreOptions.BUCKET_FUNCTION_TYPE.key(), "mod"));
        Assertions.assertNull(PhysicalPaimonTableSink.buildFixedBucketDistributionSpec(
                customBucket,
                Collections.singletonList(new Column("id", PrimitiveType.INT)),
                Collections.singletonList(new SlotReference("id", IntegerType.INSTANCE))));
    }

    @Test
    void testSupportedHashDynamicBuildsAssignerDistribution() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "-1");
        options.put(CoreOptions.DYNAMIC_BUCKET_ASSIGNER_PARALLELISM.key(), "2");
        TableSchema schema = new TableSchema(
                0L,
                ImmutableList.of(
                        new DataField(0, "id", new IntType()),
                        new DataField(1, "part", new VarCharType())),
                0,
                Collections.singletonList("part"),
                ImmutableList.of("part", "id"),
                options,
                null);
        FileStoreTable table = Mockito.mock(FileStoreTable.class);
        Mockito.when(table.bucketMode()).thenReturn(BucketMode.HASH_DYNAMIC);
        Mockito.when(table.schema()).thenReturn(schema);

        Slot id = new SlotReference("id", IntegerType.INSTANCE);
        Slot part = new SlotReference("part", StringType.INSTANCE);
        DistributionSpecPaimonHashDynamic spec
                = PhysicalPaimonTableSink.buildHashDynamicDistributionSpec(
                        table,
                        ImmutableList.of(
                                new Column("id", PrimitiveType.INT),
                                new Column("part", PrimitiveType.STRING)),
                        ImmutableList.of(id, part));

        Assertions.assertNotNull(spec);
        Assertions.assertEquals(ImmutableList.of(part.getExprId(), id.getExprId()),
                spec.getOutputColumnExprIds());
        Assertions.assertEquals(Collections.singletonList(0), spec.getPartitionFieldIndexes());
        Assertions.assertEquals(Collections.singletonList(1), spec.getPrimaryKeyFieldIndexes());
        Assertions.assertEquals(2, spec.getNumAssigners());
    }

    @Test
    void testHashDynamicWithoutStableAssignerFallsBack() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "-1");
        TableSchema schema = new TableSchema(
                0L,
                Collections.singletonList(new DataField(0, "id", new IntType())),
                0,
                Collections.emptyList(),
                Collections.singletonList("id"),
                options,
                null);
        FileStoreTable table = Mockito.mock(FileStoreTable.class);
        Mockito.when(table.bucketMode()).thenReturn(BucketMode.HASH_DYNAMIC);
        Mockito.when(table.schema()).thenReturn(schema);

        Assertions.assertNull(PhysicalPaimonTableSink.buildHashDynamicDistributionSpec(
                table,
                Collections.singletonList(new Column("id", PrimitiveType.INT)),
                Collections.singletonList(new SlotReference("id", IntegerType.INSTANCE))));
    }

    private static FileStoreTable table(
            BucketMode bucketMode, List<String> primaryKeys, Map<String, String> options) {
        FileStoreTable table = Mockito.mock(FileStoreTable.class);
        Mockito.when(table.bucketMode()).thenReturn(bucketMode);
        Mockito.when(table.primaryKeys()).thenReturn(primaryKeys);
        Mockito.when(table.options()).thenReturn(options);
        return table;
    }

    private static FileStoreTable fixedBucketTable(List<DataField> fields,
            List<String> partitionKeys, Map<String, String> extraOptions) {
        Map<String, String> options = new HashMap<>(extraOptions);
        options.put(CoreOptions.BUCKET.key(), "4");
        options.put(CoreOptions.BUCKET_KEY.key(), "id");
        TableSchema schema = new TableSchema(
                0L, fields, 0, partitionKeys, Collections.singletonList("id"), options, null);
        FileStoreTable table = Mockito.mock(FileStoreTable.class);
        Mockito.when(table.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
        Mockito.when(table.primaryKeys()).thenReturn(Collections.singletonList("id"));
        Mockito.when(table.options()).thenReturn(options);
        Mockito.when(table.schema()).thenReturn(schema);
        return table;
    }
}
