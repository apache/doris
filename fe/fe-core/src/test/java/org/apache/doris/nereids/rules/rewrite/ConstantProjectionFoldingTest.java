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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

/**
 * Test that a project expression whose input slots are all uniform constants (e.g.
 * `date_sub(dt, INTERVAL 1 DAY)` where `dt` is a uniform constant slot) is folded and the
 * projected slot is registered as a uniform constant, so downstream constant propagation can
 * fold predicates over the projected slot and partition pruning on the referenced tables works.
 *
 * <p>The uniform constant source here is a filter predicate (`WHERE dt = '2026-07-28'`), which
 * is independent of constant CTEs.
 */
class ConstantProjectionFoldingTest extends TestWithFeService implements MemoPatternMatchSupported {

    // the subquery projects `date_sub(dt, INTERVAL 1 DAY)`; `dt` is a uniform constant there
    // (from `WHERE dt = '2026-07-28'`), so the projected slot must fold to a uniform constant
    // and the outer join predicate `t1.dt = prev_dt` folds to `t1.dt = '2026-07-27'`, letting
    // the outer scan prune to a single partition.
    private static final String SQL = "SELECT * FROM cte_prune_t t1\n"
            + "JOIN (\n"
            + "    SELECT dt, date_sub(dt, INTERVAL 1 DAY) AS prev_dt\n"
            + "    FROM cte_prune_t\n"
            + "    WHERE dt = '2026-07-28'\n"
            + ") p ON t1.dt = p.prev_dt";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        createTable("CREATE TABLE `test`.`cte_prune_t` (\n"
                + "  `dt` DATE NULL COMMENT \"\",\n"
                + "  `sn` VARCHAR(50) NULL COMMENT \"\",\n"
                + "  `v` DOUBLE NULL COMMENT \"\"\n"
                + ") DUPLICATE KEY(`dt`, `sn`)\n"
                + "PARTITION BY RANGE(`dt`)\n"
                + "(PARTITION p20260101 VALUES [(\"2026-01-01\"), (\"2026-01-02\")),\n"
                + " PARTITION p20260726 VALUES [(\"2026-07-26\"), (\"2026-07-27\")),\n"
                + " PARTITION p20260727 VALUES [(\"2026-07-27\"), (\"2026-07-28\")),\n"
                + " PARTITION p20260728 VALUES [(\"2026-07-28\"), (\"2026-07-29\")),\n"
                + " PARTITION p20260729 VALUES [(\"2026-07-29\"), (\"2026-07-30\")),\n"
                + " PARTITION p20260901 VALUES [(\"2026-09-01\"), (\"2026-09-02\")))\n"
                + "DISTRIBUTED BY HASH(`sn`) BUCKETS 3\n"
                + "PROPERTIES('replication_num' = '1');");
        FeConstants.runningUnitTest = true;
    }

    @Test
    void testUniformConstantFoldThroughFunctionProjection() {
        PlanChecker planChecker = PlanChecker.from(connectContext)
                .analyze(SQL)
                .rewrite();
        Plan plan = planChecker.getCascadesContext().getRewritePlan();
        String planString = plan.treeString();

        List<LogicalOlapScan> scans = plan.collectToList(LogicalOlapScan.class::isInstance);
        Assertions.assertEquals(2, scans.size(),
                "both the outer table and the subquery should scan cte_prune_t, plan: " + planString);
        Set<String> selectedPartitions = Sets.newHashSet();
        for (LogicalOlapScan scan : scans) {
            // the outer scan (t1) should prune to p20260727, the subquery scan to p20260728
            Assertions.assertEquals(1, scan.getSelectedPartitionIds().size(),
                    "scan on cte_prune_t should prune to exactly one partition, plan: " + planString);
            selectedPartitions.add(((OlapTable) scan.getTable())
                    .getPartition(scan.getSelectedPartitionIds().get(0)).getName());
        }
        Assertions.assertEquals(Sets.newHashSet("p20260727", "p20260728"), selectedPartitions,
                "outer scan should prune to p20260727 and subquery scan to p20260728, plan: "
                        + planString);
    }
}
