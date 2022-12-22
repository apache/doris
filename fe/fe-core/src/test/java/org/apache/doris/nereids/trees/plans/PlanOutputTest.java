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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.catalog.Table;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.RelationUtil;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

public class PlanOutputTest {
    @Test
    public void testComputeOutput() {
        LogicalOlapScan relationPlan = PlanConstructor.newLogicalOlapScan(0, "a", 0);
        List<Slot> output = relationPlan.getOutput();
        Assertions.assertEquals(2, output.size());
        Assertions.assertEquals(output.get(0).getName(), "id");
        Assertions.assertEquals(output.get(0).getQualifiedName(), "db.a.id");
        Assertions.assertEquals(output.get(0).getDataType(), IntegerType.INSTANCE);

        Assertions.assertEquals(output.get(1).getName(), "name");
        Assertions.assertEquals(output.get(1).getQualifiedName(), "db.a.name");
        Assertions.assertEquals(output.get(1).getDataType(), StringType.INSTANCE);
    }

    @Test
    public void testLazyComputeOutput() {
        // not throw exception when create new UnboundRelation
        UnboundRelation relationPlan = new UnboundRelation(RelationUtil.newRelationId(), ImmutableList.of("a"));

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
        LogicalOlapScan relationPlan = PlanConstructor.newLogicalOlapScan(0, "a", 0);

        List<Slot> output = relationPlan.getOutput();
        // column prune
        Plan newPlan = relationPlan.withOutput(ImmutableList.of(output.get(0)));
        output = newPlan.getOutput();
        Assertions.assertEquals(1, output.size());
        Assertions.assertEquals(output.get(0).getName(), "id");
        Assertions.assertEquals(output.get(0).getQualifiedName(), "db.a.id");
        Assertions.assertEquals(output.get(0).getDataType(), IntegerType.INSTANCE);
    }

    @Test
    public void testPhysicalPlanMustHaveLogicalProperties() {
        Assertions.assertThrows(NullPointerException.class, () ->
                new PhysicalRelation(RelationUtil.newRelationId(),
                        PlanType.PHYSICAL_OLAP_SCAN, ImmutableList.of("db"), Optional.empty(), null) {
                    @Override
                    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
                        return null;
                    }

                    @Override
                    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
                        return null;
                    }

                    @Override
                    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
                            StatsDeriveResult statsDeriveResult) {
                        return null;
                    }

                    @Override
                    public Table getTable() {
                        return null;
                    }
                }
        );
    }
}
