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

package org.apache.doris.nereids.plan;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.operators.plans.logical.LogicalRelation;
import org.apache.doris.nereids.operators.plans.physical.PhysicalScan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plans;
import org.apache.doris.nereids.trees.plans.logical.LogicalLeafPlan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.List;

public class TestPlanOutput implements Plans {
    @Test
    public void testComputeOutput() {
        Table table = new Table(0L, "a", Table.TableType.OLAP, ImmutableList.<Column>of(
            new Column("id", Type.INT, true, AggregateType.NONE, "0", ""),
            new Column("name", Type.STRING, true, AggregateType.NONE, "", "")
        ));
        LogicalLeafPlan<LogicalRelation> relationPlan = plan(
            new LogicalRelation(table, ImmutableList.of("a"))
        );
        List<Slot> output = relationPlan.getOutput();
        Assertions.assertTrue(output.size() == 2);
        Assertions.assertEquals(output.get(0).getName(), "id");
        Assertions.assertEquals(output.get(0).getQualifiedName(), "a.id");
        Assertions.assertEquals(output.get(0).getDataType(), IntegerType.INSTANCE);

        Assertions.assertEquals(output.get(1).getName(), "name");
        Assertions.assertEquals(output.get(1).getQualifiedName(), "a.name");
        Assertions.assertEquals(output.get(1).getDataType(), StringType.INSTANCE);
    }

    @Test
    public void testLazyComputeOutput() {
        // not throw exception when create new UnboundRelation
        LogicalLeafPlan<UnboundRelation> relationPlan = plan(
            new UnboundRelation(ImmutableList.of("a"))
        );

        try {
            // throw exception when getOutput
            relationPlan.getOutput();
            throw new IllegalStateException("Expect an UnboundException but no exception");
        } catch (UnboundException e) {
            // correct exception
        }
    }

    @Test
    public void testWithOutput() {
        Table table = new Table(0L, "a", Table.TableType.OLAP, ImmutableList.of(
            new Column("id", Type.INT, true, AggregateType.NONE, "0", ""),
            new Column("name", Type.STRING, true, AggregateType.NONE, "", "")
        ));
        LogicalLeafPlan<LogicalRelation> relationPlan = plan(
            new LogicalRelation(table, ImmutableList.of("a"))
        );

        List<Slot> output = relationPlan.getOutput();
        // column prune
        LogicalLeafPlan<LogicalRelation> newPlan = relationPlan.withOutput(ImmutableList.of(output.get(0)));
        output = newPlan.getOutput();
        Assertions.assertTrue(output.size() == 1);
        Assertions.assertEquals(output.get(0).getName(), "id");
        Assertions.assertEquals(output.get(0).getQualifiedName(), "a.id");
        Assertions.assertEquals(output.get(0).getDataType(), IntegerType.INSTANCE);
    }

    @Test(expected = NullPointerException.class)
    public void testPhysicalPlanMustHaveLogicalProperties() {
        plan(new PhysicalScan(OperatorType.PHYSICAL_OLAP_SCAN, ImmutableList.of("tbl")) {}, null);
    }
}
