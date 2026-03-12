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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalHudiScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;

import mockit.Expectations;
import mockit.Injectable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for Hudi MOR table count push down disabled.
 * This test verifies that count push down is disabled for Hudi MOR (Merge on Read) tables
 * but allowed for Hudi COW (Copy on Write) tables.
 */
public class HudiMorCountPushDownTest {

    private final AggregateStrategies aggregateStrategies = new AggregateStrategies();

    /**
     * Test that canPushDownCountForHudiScan returns false for Hudi MOR table.
     * When isHoodieCowTable() returns false (MOR table), count push down should be disabled.
     */
    @Test
    public void testCanPushDownCountForHudiMorTableReturnsFalse(
            @Injectable LogicalHudiScan hudiScan,
            @Injectable HMSExternalTable hmsTable) {

        new Expectations() {
            {
                hudiScan.getTable();
                result = hmsTable;

                hmsTable.isHoodieCowTable();
                result = false;  // MOR table
            }
        };

        boolean canPushDown = aggregateStrategies.canPushDownCountForHudiScan(hudiScan);
        Assertions.assertFalse(canPushDown, "Count push down should be disabled for Hudi MOR table");
    }

    /**
     * Test that canPushDownCountForHudiScan returns true for Hudi COW table.
     * When isHoodieCowTable() returns true (COW table), count push down should be allowed.
     */
    @Test
    public void testCanPushDownCountForHudiCowTableReturnsTrue(
            @Injectable LogicalHudiScan hudiScan,
            @Injectable HMSExternalTable hmsTable) {

        new Expectations() {
            {
                hudiScan.getTable();
                result = hmsTable;

                hmsTable.isHoodieCowTable();
                result = true;  // COW table
            }
        };

        boolean canPushDown = aggregateStrategies.canPushDownCountForHudiScan(hudiScan);
        Assertions.assertTrue(canPushDown, "Count push down should be enabled for Hudi COW table");
    }

    /**
     * Test that canPushDownCountForHudiScan returns true for non-Hudi file scan.
     * For regular file scans (not LogicalHudiScan), count push down should be allowed.
     */
    @Test
    public void testCanPushDownCountForNonHudiFileScanReturnsTrue(
            @Injectable LogicalFileScan fileScan) {

        boolean canPushDown = aggregateStrategies.canPushDownCountForHudiScan(fileScan);
        Assertions.assertTrue(canPushDown, "Count push down should be enabled for non-Hudi file scan");
    }

    /**
     * Test that canPushDownCountForHudiScan returns true for OLAP scan.
     * For OLAP scans, count push down should be allowed (handled elsewhere).
     */
    @Test
    public void testCanPushDownCountForOlapScanReturnsTrue(
            @Injectable LogicalOlapScan olapScan) {

        boolean canPushDown = aggregateStrategies.canPushDownCountForHudiScan(olapScan);
        Assertions.assertTrue(canPushDown, "Count push down should be enabled for OLAP scan");
    }

    /**
     * Test that canPushDownCountForHudiScan returns true when Hudi table is not HMSExternalTable.
     * This covers the edge case where the table type is different.
     */
    @Test
    public void testCanPushDownCountForHudiScanWithNonHmsTableReturnsTrue(
            @Injectable LogicalHudiScan hudiScan,
            @Injectable org.apache.doris.datasource.ExternalTable externalTable) {

        new Expectations() {
            {
                hudiScan.getTable();
                result = externalTable;
            }
        };

        boolean canPushDown = aggregateStrategies.canPushDownCountForHudiScan(hudiScan);
        Assertions.assertTrue(canPushDown,
                "Count push down should be enabled when Hudi table is not HMSExternalTable");
    }
}
