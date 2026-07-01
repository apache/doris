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
import org.apache.doris.datasource.paimon.PaimonExternalDatabase;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.DistributionSpecExternalTableSinkHashPartitioned;
import org.apache.doris.nereids.properties.DistributionSpecExternalTableSinkHashPartitioned.PaimonFixedBucketRouteInfo;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;

import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class PhysicalPaimonTableSinkTest {

    @Test
    public void testBucketUnawareWithoutDistributionColumnsUsesRandomPartitioned() {
        SlotReference valueSlot = new SlotReference("v", StringType.INSTANCE);
        PhysicalPaimonTableSink<Plan> sink = newSink(
                BucketMode.BUCKET_UNAWARE,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(new Column("v", PrimitiveType.STRING)),
                Collections.singletonList(valueSlot));

        Assert.assertSame(PhysicalProperties.SINK_RANDOM_PARTITIONED, sink.getRequirePhysicalProperties());
    }

    @Test
    public void testFixedBucketUsesPartitionAndRouteBucketExprIds() {
        SlotReference partitionSlot = new SlotReference("pt", IntegerType.INSTANCE);
        SlotReference bucketSlot = new SlotReference("bucket_key", IntegerType.INSTANCE);
        SlotReference valueSlot = new SlotReference("v", StringType.INSTANCE);
        PhysicalPaimonTableSink<Plan> sink = newSink(
                BucketMode.HASH_FIXED,
                Collections.singletonList("bucket_key"),
                Collections.singletonList(new Column("pt", PrimitiveType.INT)),
                Arrays.asList(
                        new Column("pt", PrimitiveType.INT),
                        new Column("bucket_key", PrimitiveType.INT),
                        new Column("v", PrimitiveType.STRING)),
                Arrays.asList(partitionSlot, bucketSlot, valueSlot));

        PhysicalProperties properties = sink.getRequirePhysicalProperties();

        Assert.assertTrue(properties.getDistributionSpec()
                instanceof DistributionSpecExternalTableSinkHashPartitioned);
        DistributionSpecExternalTableSinkHashPartitioned distribution =
                (DistributionSpecExternalTableSinkHashPartitioned) properties.getDistributionSpec();
        Assert.assertEquals(Collections.singletonList(partitionSlot.getExprId()), distribution.getOutputColExprIds());
        Assert.assertEquals(DistributionSpecExternalTableSinkHashPartitioned.ExternalSinkHashMode.STRICT_HASH,
                distribution.getExternalSinkHashMode());
        PaimonFixedBucketRouteInfo routeInfo = distribution.getPaimonFixedBucketRouteInfo();
        Assert.assertEquals(8, routeInfo.getBucketNum());
        Assert.assertEquals(PaimonFixedBucketRouteInfo.BucketFunctionType.DEFAULT,
                routeInfo.getBucketFunctionType());
        Assert.assertEquals(Collections.singletonList(bucketSlot.getExprId()), routeInfo.getBucketKeyExprIds());
    }

    @Test
    public void testFixedBucketRequiresBucketKeyInSinkChildOutput() {
        SlotReference partitionSlot = new SlotReference("pt", IntegerType.INSTANCE);
        SlotReference valueSlot = new SlotReference("v", StringType.INSTANCE);
        PhysicalPaimonTableSink<Plan> sink = newSink(
                BucketMode.HASH_FIXED,
                Collections.singletonList("bucket_key"),
                Collections.singletonList(new Column("pt", PrimitiveType.INT)),
                Arrays.asList(new Column("pt", PrimitiveType.INT), new Column("v", PrimitiveType.STRING)),
                Arrays.asList(partitionSlot, valueSlot));

        UnsupportedOperationException exception = Assert.assertThrows(UnsupportedOperationException.class,
                sink::getRequirePhysicalProperties);

        Assert.assertTrue(exception.getMessage().contains("requires bucket key in sink output"));
        Assert.assertTrue(exception.getMessage().contains("bucket_key"));
    }

    @Test
    public void testUnsupportedBucketModeFailsFast() {
        SlotReference valueSlot = new SlotReference("v", StringType.INSTANCE);
        PhysicalPaimonTableSink<Plan> sink = newSink(
                BucketMode.HASH_DYNAMIC,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList(new Column("v", PrimitiveType.STRING)),
                Collections.singletonList(valueSlot));

        UnsupportedOperationException exception = Assert.assertThrows(UnsupportedOperationException.class,
                sink::getRequirePhysicalProperties);

        Assert.assertTrue(exception.getMessage().contains("Unsupported Paimon bucket mode"));
    }

    private static PhysicalPaimonTableSink<Plan> newSink(BucketMode bucketMode, List<String> bucketKeys,
            List<Column> partitionColumns, List<Column> columns, List<SlotReference> outputSlots) {
        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        FileStoreTable fileStoreTable = Mockito.mock(FileStoreTable.class);
        TableSchema schema = Mockito.mock(TableSchema.class);
        Mockito.when(table.getPaimonTable(Mockito.any(Optional.class))).thenReturn(fileStoreTable);
        Mockito.when(table.getPartitionColumns(Mockito.any(Optional.class))).thenReturn(partitionColumns);
        Mockito.when(fileStoreTable.bucketMode()).thenReturn(bucketMode);
        Mockito.when(fileStoreTable.schema()).thenReturn(schema);
        Mockito.when(schema.bucketKeys()).thenReturn(bucketKeys);
        Mockito.when(schema.numBuckets()).thenReturn(8);
        Mockito.when(schema.options()).thenReturn(Collections.emptyMap());

        Plan child = Mockito.mock(Plan.class);
        Mockito.when(child.getAllChildrenTypes()).thenReturn(new BitSet());
        Mockito.when(child.depth()).thenReturn(1);
        Mockito.when(child.getOutput()).thenReturn(outputSlots.stream()
                .map(Slot.class::cast)
                .collect(Collectors.toList()));
        List<Slot> output = outputSlots.stream()
                .map(Slot.class::cast)
                .collect(Collectors.toList());
        List<NamedExpression> outputExprs = outputSlots.stream()
                .map(NamedExpression.class::cast)
                .collect(Collectors.toList());

        return new PhysicalPaimonTableSink<>(
                Mockito.mock(PaimonExternalDatabase.class),
                table,
                columns,
                outputExprs,
                Optional.empty(),
                new LogicalProperties(() -> output, () -> DataTrait.EMPTY_TRAIT),
                child);
    }
}
