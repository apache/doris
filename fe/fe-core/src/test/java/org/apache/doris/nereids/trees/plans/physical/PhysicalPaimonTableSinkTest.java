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

import org.apache.doris.datasource.paimon.PaimonExternalDatabase;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.nereids.properties.DistributionSpecPaimonBucketShuffle;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableSet;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class PhysicalPaimonTableSinkTest {

    @Test
    public void testRequirePhysicalPropertiesUsesPartitionShuffle(@Mocked PaimonExternalDatabase database,
                                                                  @Mocked PaimonExternalTable table,
                                                                  @Mocked PhysicalPlan child,
                                                                  @Mocked LogicalProperties logicalProperties)
            throws Exception {
        List<Slot> output = Collections.singletonList(new SlotReference("pt", IntegerType.INSTANCE));
        new Expectations() {{
                table.getPartitionColumnNames((Optional) any);
                result = ImmutableSet.of("pt");
                child.getOutput();
                result = output;
                table.getDbName();
                result = "db1";
                table.getName();
                result = "tbl1";
            }};

        PhysicalPaimonTableSink<PhysicalPlan> sink = new PhysicalPaimonTableSink<>(database, table,
                Collections.emptyList(), Collections.emptyList(), Optional.empty(), logicalProperties, child);

        PhysicalProperties required = sink.getRequirePhysicalProperties();
        Assert.assertTrue(required.getDistributionSpec() instanceof DistributionSpecPaimonBucketShuffle);
    }

    @Test
    public void testRequirePhysicalPropertiesFallsBackWhenPartitionSlotsMissing(
            @Mocked PaimonExternalDatabase database,
            @Mocked PaimonExternalTable table,
            @Mocked PhysicalPlan child,
            @Mocked LogicalProperties logicalProperties) throws Exception {
        List<Slot> output = Collections.singletonList(new SlotReference("k1", IntegerType.INSTANCE));
        new Expectations() {{
                table.getPartitionColumnNames((Optional) any);
                result = ImmutableSet.of("pt");
                child.getOutput();
                result = output;
                table.getDbName();
                result = "db1";
                table.getName();
                result = "tbl1";
            }};

        PhysicalPaimonTableSink<PhysicalPlan> sink = new PhysicalPaimonTableSink<>(database, table,
                Collections.emptyList(), Collections.emptyList(), Optional.empty(), logicalProperties, child);

        Assert.assertSame(PhysicalProperties.SINK_RANDOM_PARTITIONED, sink.getRequirePhysicalProperties());
    }
}
