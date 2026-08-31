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
 * Test that a CTE defining a constant single row (e.g. `params`) propagates the constants
 * out, so that downstream predicates over the CTE columns can be constant-folded and the
 * partition pruning on the referenced tables works.
 *
 * <p>After the constant CTE is inlined, the first consumer predicate
 * `dt BETWEEN params.begin_time AND params.end_time` is folded and the scan prunes to the
 * day partition; the second consumer predicate
 * `dt BETWEEN DATE_SUB(params.begin_time, INTERVAL params.period_days DAY)
 *        AND DATE_SUB(params.begin_time, INTERVAL 1 DAY)`
 * was not folded, so the scan read all partitions.
 */
class ConstantCteTest extends TestWithFeService implements MemoPatternMatchSupported {

    // the constant CTE `params` participates in two joins; the second join uses
    // DATE_SUB() over the CTE columns, which needs constant folding to prune partitions.
    private static final String SQL = "WITH params AS (\n"
            + "    SELECT\n"
            + "        CAST('2026-07-28 00:00:00' AS DATETIME) AS begin_time,\n"
            + "        CAST('2026-07-28 23:59:59' AS DATETIME) AS end_time,\n"
            + "        DATEDIFF(CAST('2026-07-28 23:59:59' AS DATETIME), "
            + "CAST('2026-07-28 00:00:00' AS DATETIME)) + 1 AS period_days\n"
            + "),\n"
            + "current_data AS (\n"
            + "    SELECT SUM(v) AS total_value\n"
            + "    FROM cte_prune_t\n"
            + "    JOIN params ON 1=1\n"
            + "    WHERE dt BETWEEN params.begin_time AND params.end_time\n"
            + "),\n"
            + "last_period_data AS (\n"
            + "    SELECT SUM(v) AS total_value\n"
            + "    FROM cte_prune_t\n"
            + "    JOIN params ON 1=1\n"
            + "    WHERE dt BETWEEN DATE_SUB(params.begin_time, INTERVAL params.period_days DAY)\n"
            + "               AND DATE_SUB(params.begin_time, INTERVAL 1 DAY)\n"
            + ")\n"
            + "SELECT * FROM current_data, last_period_data";

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
    void testConstantCteFoldJoinPredicateAndPrunePartition() {
        // params has 2 consumers; force inline to match the reported scenario
        connectContext.getSessionVariable().inlineCTEReferencedThreshold = 2;

        PlanChecker planChecker = PlanChecker.from(connectContext)
                .analyze(SQL)
                .rewrite();
        Plan plan = planChecker.getCascadesContext().getRewritePlan();
        String planString = plan.treeString();

        List<LogicalOlapScan> scans = plan.collectToList(LogicalOlapScan.class::isInstance);
        Assertions.assertEquals(2, scans.size(),
                "both current_data and last_period_data should scan cte_prune_t, plan: " + planString);
        Set<String> selectedPartitions = Sets.newHashSet();
        for (LogicalOlapScan scan : scans) {
            // current_data only needs p20260728, last_period_data only needs p20260727;
            // both must prune to exactly one partition
            Assertions.assertEquals(1, scan.getSelectedPartitionIds().size(),
                    "scan on cte_prune_t should prune to exactly one partition, plan: " + planString);
            selectedPartitions.add(((OlapTable) scan.getTable())
                    .getPartition(scan.getSelectedPartitionIds().get(0)).getName());
        }
        Assertions.assertEquals(Sets.newHashSet("p20260727", "p20260728"), selectedPartitions,
                "current_data should prune to p20260728 and last_period_data to p20260727, plan: "
                        + planString);
    }
}
