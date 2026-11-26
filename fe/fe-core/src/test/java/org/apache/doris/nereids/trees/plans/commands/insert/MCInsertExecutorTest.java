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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalDatabase;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalTable;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecMaxComputeTableSinkHashPartitioned;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalMaxComputeTableSink;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.planner.MaxComputeTableSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.thrift.TQueryOptions;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

class MCInsertExecutorTest {
    @ParameterizedTest
    @CsvSource({"13, 12", "12, 13"})
    void testConfigChangeBetweenPlanningAndDispatch(int plannedVersion, int updatedVersion) {
        int previousVersion = Config.be_exec_version;
        try {
            Config.be_exec_version = plannedVersion;
            PhysicalMaxComputeTableSink<Plan> plan = createDynamicSink();
            Class<?> expected = plannedVersion == 13
                    ? DistributionSpecMaxComputeTableSinkHashPartitioned.class : DistributionSpecHash.class;
            Assertions.assertInstanceOf(expected, plan.getRequirePhysicalProperties().getDistributionSpec());

            Config.be_exec_version = updatedVersion;
            PhysicalMaxComputeTableSink<?> selected = (PhysicalMaxComputeTableSink<?>)
                    plan.withPhysicalPropertiesAndStats(PhysicalProperties.GATHER, null);
            Assertions.assertInstanceOf(expected, selected.getRequirePhysicalProperties().getDistributionSpec());

            TQueryOptions dispatchOptions = new TQueryOptions();
            dispatchOptions.setBeExecVersion(updatedVersion);
            Coordinator coordinator = Mockito.mock(Coordinator.class);
            Mockito.when(coordinator.getQueryOptions()).thenReturn(dispatchOptions);
            MCInsertExecutor executor = Mockito.mock(MCInsertExecutor.class, Mockito.CALLS_REAL_METHODS);
            Mockito.doReturn(coordinator).when(executor).getCoordinator();
            executor.finalizeSink(Mockito.mock(PlanFragment.class), Mockito.mock(MaxComputeTableSink.class), selected);

            Assertions.assertEquals(plannedVersion, dispatchOptions.getBeExecVersion());
            Assertions.assertEquals(updatedVersion, createDynamicSink().getBeExecVersion());
        } finally {
            Config.be_exec_version = previousVersion;
        }
    }

    private static PhysicalMaxComputeTableSink<Plan> createDynamicSink() {
        Column part = new Column("part", PrimitiveType.INT);
        List<Column> columns = ImmutableList.of(new Column("data", PrimitiveType.INT), part);
        MaxComputeExternalTable table = Mockito.mock(MaxComputeExternalTable.class);
        Mockito.when(table.getPartitionColumns()).thenReturn(ImmutableList.of(part));
        Mockito.when(table.getFullSchema()).thenReturn(columns);
        List<Slot> output = ImmutableList.of(new SlotReference("data", IntegerType.INSTANCE),
                new SlotReference("part", IntegerType.INSTANCE));
        Plan child = Mockito.mock(Plan.class);
        Mockito.when(child.getOutput()).thenReturn(output);
        return new PhysicalMaxComputeTableSink<>(Mockito.mock(MaxComputeExternalDatabase.class), table, columns,
                ImmutableList.copyOf(output), Optional.empty(), Mockito.mock(LogicalProperties.class), child);
    }
}
