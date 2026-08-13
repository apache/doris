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

package org.apache.doris.datasource.paimon;

import org.apache.doris.datasource.ExternalWriteDistributionPlan;
import org.apache.doris.datasource.ExternalWriteDistributionPlan.RouteKind;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.PaimonBinaryRowHash;
import org.apache.doris.nereids.trees.expressions.functions.scalar.PaimonFixedBucket;
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

class PaimonWriteDistributionProviderTest {
    private final PaimonWriteDistributionProvider provider =
            new PaimonWriteDistributionProvider();

    @Test
    void testSupportedPartitionedFixedBucketUsesTwoHiddenExpressions() {
        FileStoreTable table = table(
                BucketMode.HASH_FIXED,
                ImmutableList.of(
                        new DataField(0, "id", new IntType()),
                        new DataField(1, "part", new VarCharType())),
                Collections.singletonList("part"),
                false,
                Collections.emptyMap());
        Slot id = new SlotReference("id", IntegerType.INSTANCE);
        Slot part = new SlotReference("PART", StringType.INSTANCE);

        ExternalWriteDistributionPlan plan = provider.plan(
                table, ImmutableList.of(id, part));

        Assertions.assertEquals(RouteKind.STATELESS_HASH, plan.getRouteKind());
        Assertions.assertEquals(2, plan.getRoutingExpressions().size());
        PaimonBinaryRowHash partitionHash = (PaimonBinaryRowHash)
                ((Alias) plan.getRoutingExpressions().get(0)).child();
        Assertions.assertSame(part, partitionHash.child(0));
        PaimonFixedBucket bucket = (PaimonFixedBucket)
                ((Alias) plan.getRoutingExpressions().get(1)).child();
        Assertions.assertEquals(2, bucket.children().size());
        Assertions.assertSame(id, bucket.child(1));
        Assertions.assertEquals(4,
                plan.getRoutingCardinalityCap(
                        plan.getRoutingExpressions().get(1).getExprId()).getAsLong());
    }

    @Test
    void testUnpartitionedFixedBucketOnlyNeedsBucketExpression() {
        FileStoreTable table = table(
                BucketMode.HASH_FIXED,
                Collections.singletonList(new DataField(0, "id", new IntType())),
                Collections.emptyList(),
                false,
                Collections.emptyMap());

        ExternalWriteDistributionPlan plan = provider.plan(
                table,
                Collections.singletonList(new SlotReference("id", IntegerType.INSTANCE)));

        Assertions.assertEquals(RouteKind.STATELESS_HASH, plan.getRouteKind());
        Assertions.assertEquals(1, plan.getRoutingExpressions().size());
        Assertions.assertTrue(((Alias) plan.getRoutingExpressions().get(0)).child()
                instanceof PaimonFixedBucket);
    }

    @Test
    void testUnsupportedFixedBucketTypeAlwaysUsesSingleWriter() {
        Map<String, String> options = Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true");
        FileStoreTable table = table(
                BucketMode.HASH_FIXED,
                Collections.singletonList(
                        new DataField(0, "id", new ArrayType(new IntType()))),
                Collections.emptyList(),
                true,
                options);

        ExternalWriteDistributionPlan plan = provider.plan(
                table,
                Collections.singletonList(new SlotReference("id", IntegerType.INSTANCE)));

        Assertions.assertEquals(RouteKind.SINGLE_WRITER, plan.getRouteKind());
    }

    @Test
    void testStatefulBucketModesUseSingleWriter() {
        for (BucketMode mode : ImmutableList.of(
                BucketMode.HASH_DYNAMIC, BucketMode.KEY_DYNAMIC, BucketMode.POSTPONE_MODE)) {
            ExternalWriteDistributionPlan plan = provider.plan(
                    table(mode,
                            Collections.singletonList(new DataField(0, "id", new IntType())),
                            Collections.emptyList(), false, Collections.emptyMap()),
                    Collections.singletonList(new SlotReference("id", IntegerType.INSTANCE)));
            Assertions.assertEquals(RouteKind.SINGLE_WRITER, plan.getRouteKind());
        }
    }

    @Test
    void testCustomBucketFunctionUsesSingleWriter() {
        Map<String, String> options = Collections.singletonMap(
                CoreOptions.BUCKET_FUNCTION_TYPE.key(), "mod");
        ExternalWriteDistributionPlan plan = provider.plan(
                table(BucketMode.HASH_FIXED,
                        Collections.singletonList(new DataField(0, "id", new IntType())),
                        Collections.emptyList(), false, options),
                Collections.singletonList(new SlotReference("id", IntegerType.INSTANCE)));

        Assertions.assertEquals(RouteKind.SINGLE_WRITER, plan.getRouteKind());
    }

    @Test
    void testMissingBucketColumnUsesSingleWriter() {
        ExternalWriteDistributionPlan plan = provider.plan(
                table(BucketMode.HASH_FIXED,
                        Collections.singletonList(new DataField(0, "id", new IntType())),
                        Collections.emptyList(), false, Collections.emptyMap()),
                Collections.singletonList(new SlotReference("other", IntegerType.INSTANCE)));

        Assertions.assertEquals(RouteKind.SINGLE_WRITER, plan.getRouteKind());
    }

    private static FileStoreTable table(BucketMode mode, List<DataField> fields,
            List<String> partitionKeys, boolean appendOnly, Map<String, String> extraOptions) {
        Map<String, String> options = new HashMap<>(extraOptions);
        options.put(CoreOptions.BUCKET.key(), "4");
        options.put(CoreOptions.BUCKET_KEY.key(), "id");
        List<String> primaryKeys = appendOnly
                ? Collections.emptyList() : Collections.singletonList("id");
        TableSchema schema = new TableSchema(
                0L, fields, 0, partitionKeys, primaryKeys, options, null);
        FileStoreTable table = Mockito.mock(FileStoreTable.class);
        Mockito.when(table.bucketMode()).thenReturn(mode);
        Mockito.when(table.primaryKeys()).thenReturn(primaryKeys);
        Mockito.when(table.options()).thenReturn(options);
        Mockito.when(table.schema()).thenReturn(schema);
        return table;
    }
}
