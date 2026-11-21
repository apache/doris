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

import org.apache.doris.nereids.rules.implementation.LogicalOlapScanToPhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

/**
 * The state of the operative slots derivation has to survive the copies of a relation and the
 * conversion to a physical relation, otherwise the statistics derivation of the copy treats a
 * derived empty list as "not derived" and fetches the statistics of every column again.
 */
public class OperativeSlotsDerivedStateTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test.wide (\n"
                + "a int not null, b int, c int)\n"
                + "distributed by hash(a) buckets 10\n"
                + "properties('replication_num' = '1');");
        connectContext.setDatabase("test");
    }

    @Test
    public void testCopyOfARelationKeepsTheDerivedEmptyState() {
        // a query that needs no column of the table, so the derivation result is an empty list
        Plan analyzed = PlanChecker.from(connectContext)
                .analyze("select count(*) from wide")
                .getCascadesContext()
                .getRewritePlan();
        Plan derived = analyzed.accept(new OperativeColumnDerive(), new OperativeColumnDerive.DeriveContext());
        LogicalOlapScan scan = derived.<LogicalOlapScan>collectToList(LogicalOlapScan.class::isInstance).get(0);
        Assertions.assertTrue(scan.getOperativeSlots().isEmpty());
        Assertions.assertTrue(scan.isOperativeSlotsDerived());

        // the memo builds a group expression for every leaf with withGroupExpression
        LogicalOlapScan memoCopy = scan.withGroupExpression(Optional.empty());
        Assertions.assertTrue(memoCopy.getOperativeSlots().isEmpty());
        Assertions.assertTrue(memoCopy.isOperativeSlotsDerived(),
                "a copy has to carry the derivation state of the relation it was built from");

        // a copy built for another purpose, e.g. by SetPreAggStatus before the join reorder
        LogicalOlapScan preAggCopy = scan.withPreAggStatus(scan.getPreAggStatus());
        Assertions.assertTrue(preAggCopy.isOperativeSlotsDerived());
    }

    @Test
    public void testPhysicalScanKeepsTheDerivedStateOfItsLogicalScan() {
        PlanChecker checker = PlanChecker.from(connectContext)
                .analyze("select 66 from wide")
                .customRewrite(new OperativeColumnDerive());
        LogicalOlapScan logicalScan = checker.getCascadesContext().getRewritePlan()
                .<LogicalOlapScan>collectToList(LogicalOlapScan.class::isInstance).get(0);
        Assertions.assertTrue(logicalScan.getOperativeSlots().isEmpty());
        Assertions.assertTrue(logicalScan.isOperativeSlotsDerived());

        // the implementation rule converts the derived relation into the physical relation that the
        // memo estimates, e.g. as the child of a PhysicalStorageLayerAggregate
        Plan physical = new LogicalOlapScanToPhysicalOlapScan().build()
                .transform(logicalScan, checker.getCascadesContext()).get(0);
        Assertions.assertTrue(physical instanceof PhysicalOlapScan);
        PhysicalOlapScan physicalScan = (PhysicalOlapScan) physical;
        Assertions.assertTrue(physicalScan.getOperativeSlots().isEmpty());
        Assertions.assertTrue(physicalScan.isOperativeSlotsDerived(),
                "the physical relation, which is estimated on its own, has to carry the state");
    }
}
