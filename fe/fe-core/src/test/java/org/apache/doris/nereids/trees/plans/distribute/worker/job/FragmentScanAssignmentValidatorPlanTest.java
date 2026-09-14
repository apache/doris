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

package org.apache.doris.nereids.trees.plans.distribute.worker.job;

import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.Planner;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Verifies the planning-time scan assignment check against plans of real queries.
 *
 * <p>The first test is the guard against false positives: the shapes that the translator produces
 * for joins, set operations, recursive CTEs, CTEs and window functions must all stay accepted. The
 * second one reconstructs the symptom the check exists for - an exchange that an optimization elided
 * out of the plan tree, which leaves two olap scans in one fragment - and asserts it is rejected.
 */
public class FragmentScanAssignmentValidatorPlanTest extends TestWithFeService {

    private static final String T1 = "fragment_scan_assignment_test.t1";
    private static final String T2 = "fragment_scan_assignment_test.t2";

    @Override
    protected void runBeforeAll() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        createDatabase("fragment_scan_assignment_test");
        createTable("CREATE TABLE " + T1 + " (id INT NOT NULL, k INT NOT NULL, v INT NOT NULL) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 4 "
                + "PROPERTIES('replication_num' = '1')");
        createTable("CREATE TABLE " + T2 + " (id INT NOT NULL, k INT NOT NULL, v INT NOT NULL) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 4 "
                + "PROPERTIES('replication_num' = '1')");
    }

    @Test
    public void testTranslatedPlanShapesStayAssignable() throws Exception {
        enableBucketedAggregation();
        String[] queries = {
                "SELECT k, SUM(v) AS sv FROM " + T1 + " GROUP BY k",
                "SELECT * FROM " + T1 + " JOIN " + T2 + " ON " + T1 + ".k = " + T2 + ".k",
                "SELECT * FROM " + T1 + " LEFT JOIN " + T2 + " ON " + T1 + ".k = " + T2 + ".k",
                "SELECT * FROM " + T1 + " JOIN [shuffle] " + T2 + " ON " + T1 + ".k = " + T2 + ".k",
                "SELECT k FROM " + T1 + " UNION ALL SELECT k FROM " + T2,
                "SELECT k FROM " + T1 + " INTERSECT SELECT k FROM " + T2,
                "SELECT k, SUM(v) AS sv FROM " + T1 + " GROUP BY k"
                        + " UNION ALL SELECT k, SUM(v) AS sv FROM " + T2 + " GROUP BY k",
                "WITH c AS (SELECT k, SUM(v) AS sv FROM " + T1 + " GROUP BY k)"
                        + " SELECT c.k, t2.v FROM c JOIN " + T2 + " t2 ON c.k = t2.k",
                "WITH RECURSIVE cte AS ("
                        + " SELECT k, SUM(v) AS sv, CAST(1 AS INT) AS lvl FROM " + T1 + " GROUP BY k"
                        + " UNION ALL"
                        + " SELECT k, sv, CAST(lvl + 1 AS INT) AS lvl FROM cte WHERE lvl < 3)"
                        + " SELECT k, MAX(sv) AS msv FROM cte GROUP BY k",
                "SELECT k, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) AS rn FROM " + T1,
                "SELECT k, COUNT(DISTINCT v) FROM " + T1 + " GROUP BY k",
                "SELECT * FROM " + T1 + " ORDER BY v LIMIT 10",
        };
        for (String query : queries) {
            Planner planner = getSQLPlanner("EXPLAIN " + query);
            Assertions.assertFalse(planner.getFragments().isEmpty(), query);
        }
    }

    @Test
    public void testElidedExchangeBoundaryIsRejected() throws Exception {
        Planner planner = getSQLPlanner("EXPLAIN SELECT * FROM " + T1 + " JOIN " + T2
                + " ON " + T1 + ".k = " + T2 + ".k");
        List<PlanFragment> fragments = Lists.newArrayList(planner.getFragments());
        PlanFragment splicedFragment = null;
        for (PlanFragment fragment : fragments) {
            PlanNode root = fragment.getPlanRoot();
            if (olapScansInFragment(root).size() != 1
                    || !spliceOutExchangeStreamingOneOlapScan(root)) {
                continue;
            }
            // Let the fragment take ownership of the spliced subtree, the way the translator does
            // when it merges a child (PlanFragment#setPlanRoot rewrites the ownership until an
            // exchange).
            fragment.setPlanRoot(root);
            if (olapScansInFragment(root).size() > 1) {
                splicedFragment = fragment;
                break;
            }
        }
        Assertions.assertNotNull(splicedFragment,
                "no fragment of the join plan could be turned into the illegal shape,"
                        + " the test needs to be adapted to the new plan structure");
        Assertions.assertEquals(2, olapScansInFragment(splicedFragment.getPlanRoot()).size());
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class,
                () -> FragmentScanAssignmentValidator.validate(fragments));
        Assertions.assertTrue(exception.getMessage().contains("Not supported multiple scan multiple OlapTable"),
                exception.getMessage());
    }

    /**
     * Connects the input of an exchange of this fragment directly below its parent, which is what an
     * exchange-eliding translation step does to the plan tree. Only exchanges that stream exactly
     * one olap scan are considered, and exchanges of other fragments are left untouched.
     */
    private boolean spliceOutExchangeStreamingOneOlapScan(PlanNode node) {
        for (int i = 0; i < node.getChildren().size(); i++) {
            PlanNode child = node.getChild(i);
            if (child instanceof ExchangeNode) {
                if (child.getChildren().size() == 1 && olapScans(child.getChild(0)).size() == 1) {
                    node.setChild(i, child.getChild(0));
                    return true;
                }
                continue;
            }
            if (spliceOutExchangeStreamingOneOlapScan(child)) {
                return true;
            }
        }
        return false;
    }

    private void enableBucketedAggregation() {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sessionVariable.aggPhase = 1;
        sessionVariable.setBeNumberForTest(1);
        sessionVariable.bucketedAggMinInputRows = 0;
        sessionVariable.bucketedAggMaxGroupKeys = 0;
        sessionVariable.bucketedAggHighCardThreshold = 1.0;
        sessionVariable.enableBucketedHashAgg = true;
        sessionVariable.enableBucketShuffleJoin = false;
        sessionVariable.parallelPipelineTaskNum = 1;
    }

    private List<OlapScanNode> olapScansInFragment(PlanNode root) {
        return root.collectInCurrentFragment(OlapScanNode.class::isInstance);
    }

    private List<OlapScanNode> olapScans(PlanNode node) {
        List<OlapScanNode> scans = Lists.newArrayList();
        node.collect(OlapScanNode.class, scans);
        return scans;
    }
}
