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

import org.apache.doris.datasource.ExternalWriteDistributionPlan;
import org.apache.doris.datasource.ExternalWriteDistributionPlan.RouteKind;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.IcebergPartitionTransform;
import org.apache.doris.nereids.trees.expressions.functions.scalar.IcebergPartitionTransform.Transform;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

public class IcebergWriteDistributionProviderTest {
    private final IcebergWriteDistributionProvider provider =
            new IcebergWriteDistributionProvider();

    @Test
    public void testPlansTransformResultTuple() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "ts", Types.TimestampType.withoutZone()),
                Types.NestedField.required(3, "name", Types.StringType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .bucket("id", 16)
                .day("ts")
                .truncate("name", 4)
                .build();
        List<Slot> output = ImmutableList.of(
                new SlotReference("id", IntegerType.INSTANCE),
                new SlotReference("ts", DateTimeV2Type.SYSTEM_DEFAULT),
                new SlotReference("name", StringType.INSTANCE));

        ExternalWriteDistributionPlan plan = provider.plan(table(schema, spec), output);

        Assert.assertEquals(RouteKind.ADAPTIVE_HASH, plan.getRouteKind());
        Assert.assertEquals(3, plan.getRoutingExpressions().size());
        assertTransform(plan.getRoutingExpressions().get(0), Transform.BUCKET, output.get(0));
        assertTransform(plan.getRoutingExpressions().get(1), Transform.DAY, output.get(1));
        assertTransform(plan.getRoutingExpressions().get(2), Transform.TRUNCATE, output.get(2));
        Assert.assertEquals(plan.getRoutingExpressions().size(), plan.getRoutingExprIds().size());
        Assert.assertEquals(16,
                plan.getRoutingCardinalityCap(
                        plan.getRoutingExpressions().get(0).getExprId()).getAsLong());
    }

    @Test
    public void testIdentityStillGetsHiddenRoutingSlot() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "Part", Types.StringType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("Part").build();
        Slot output = new SlotReference("part", StringType.INSTANCE);

        ExternalWriteDistributionPlan plan = provider.plan(
                table(schema, spec), ImmutableList.of(output));

        Assert.assertEquals(RouteKind.ADAPTIVE_HASH, plan.getRouteKind());
        Assert.assertEquals(1, plan.getRoutingExpressions().size());
        Alias route = (Alias) plan.getRoutingExpressions().get(0);
        Assert.assertSame(output, route.child());
    }

    @Test
    public void testUnpartitionedTableUsesRandomWriters() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()));

        ExternalWriteDistributionPlan plan = provider.plan(
                table(schema, PartitionSpec.unpartitioned()),
                ImmutableList.of(new SlotReference("id", IntegerType.INSTANCE)));

        Assert.assertEquals(RouteKind.RANDOM, plan.getRouteKind());
        Assert.assertFalse(plan.getFallbackReason().isPresent());
    }

    @Test
    public void testMissingPartitionSourceUsesSingleWriter() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "partition_id", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("partition_id", 8).build();

        ExternalWriteDistributionPlan plan = provider.plan(
                table(schema, spec),
                ImmutableList.of(new SlotReference("other_id", IntegerType.INSTANCE)));

        Assert.assertEquals(RouteKind.SINGLE_WRITER, plan.getRouteKind());
        Assert.assertTrue(plan.getFallbackReason().orElse("").contains("partition_id"));
    }

    private static void assertTransform(
            NamedExpression route, Transform expectedTransform, Slot expectedSource) {
        Expression expression = ((Alias) route).child();
        Assert.assertTrue(expression instanceof IcebergPartitionTransform);
        IcebergPartitionTransform transform = (IcebergPartitionTransform) expression;
        Assert.assertEquals(expectedTransform, transform.getTransform());
        Assert.assertSame(expectedSource, transform.child(0));
    }

    private static Table table(Schema schema, PartitionSpec spec) {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.spec()).thenReturn(spec);
        return table;
    }
}
