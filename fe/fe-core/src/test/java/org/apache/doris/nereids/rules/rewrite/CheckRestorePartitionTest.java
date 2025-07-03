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

import org.apache.doris.catalog.Partition;
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CheckRestorePartitionTest extends TestWithFeService implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test_restore_partition");
        useDatabase("test_restore_partition");

        createTable("create table test_restore(id int, part int) "
                + "partition by range(part) ("
                + "  partition p1 values[('1'), ('2')),"
                + "  partition p2 values[('2'), ('3'))"
                + ") "
                + "distributed by hash(id) "
                + "properties ('replication_num'='1')");

        FeConstants.runningUnitTest = true;
    }

    @Test
    void testOlapScanPartitionWithSingleColumnCase() {
        String sql = "select * from test_restore";
        Plan plan = PlanChecker.from(createCascadesContext(sql))
                .analyze(sql)
                .getPlan();

        LogicalOlapScan scan = (LogicalOlapScan) plan.child(0).child(0);
        for (Partition partition : scan.getTable().getPartitions()) {
            partition.setState(Partition.PartitionState.RESTORE);
        }
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(MemoTestUtils.createConnectContext(), scan)
                .applyBottomUp(new CheckRestorePartition()));
        Assertions.assertTrue(exception.getMessage().contains("Partition state is not NORMAL:"));
    }

}
