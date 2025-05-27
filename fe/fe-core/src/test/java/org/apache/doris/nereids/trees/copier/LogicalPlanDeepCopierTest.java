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

package org.apache.doris.nereids.trees.copier;

import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.PlanConstructor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LogicalPlanDeepCopierTest {

    @Test
    public void testDeepCopyOlapScan() {
        LogicalOlapScan relationPlan = PlanConstructor.newLogicalOlapScan(0, "a", 0);
        relationPlan = (LogicalOlapScan) relationPlan.withOperativeSlots(relationPlan.getOutput());
        LogicalOlapScan aCopy = (LogicalOlapScan) relationPlan.accept(LogicalPlanDeepCopier.INSTANCE, new DeepCopierContext());
        for (Slot opSlot : aCopy.getOperativeSlots()) {
            Assertions.assertTrue(aCopy.getOutputSet().contains(opSlot));
        }
    }
}
