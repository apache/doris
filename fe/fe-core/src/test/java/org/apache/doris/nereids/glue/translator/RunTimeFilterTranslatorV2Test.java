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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.RuntimeFilter;
import org.apache.doris.planner.RuntimeFilter.RuntimeFilterTarget;
import org.apache.doris.planner.SetOperationNode;
import org.apache.doris.thrift.TRuntimeFilterDesc;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;

/**
 * Tests for RunTimeFilterTranslatorV2, specifically verifying that runtime filter
 * locality (isLocalTarget) is correctly computed for INTERSECT/EXCEPT operations.
 *
 * Before the fix, the V2 translator used a 2-arg RuntimeFilterTarget constructor
 * which hard-coded isLocalTarget=false, causing all V2 filters to be incorrectly
 * marked as remote-only (hasRemoteTargets=true, hasLocalTargets=false) regardless
 * of actual fragment placement.
 */
public class RunTimeFilterTranslatorV2Test extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test_rf_v2");
        connectContext.setDatabase("test_rf_v2");
        connectContext.getSessionVariable().enableFallbackToOriginalPlanner = false;
        connectContext.getSessionVariable().setRuntimeFilterType(8);
        connectContext.getSessionVariable().setEnableRuntimeFilterPrune(false);
        // Prevent the optimizer from eliminating set operations on empty tables
        connectContext.getSessionVariable().setDisableNereidsRules(
                "PRUNE_EMPTY_PARTITION,"
                + "ELIMINATE_INTERSECTION_ON_EMPTYRELATION,"
                + "ELIMINATE_EXCEPT_ON_EMPTYRELATION");

        createTable("create table test_rf_v2.t1 ("
                + "k1 int, "
                + "v1 int"
                + ") distributed by hash(k1) buckets 3 "
                + "properties('replication_num' = '1');");

        createTable("create table test_rf_v2.t2 ("
                + "k1 int, "
                + "v1 int"
                + ") distributed by hash(k1) buckets 3 "
                + "properties('replication_num' = '1');");

        createTable("create table test_rf_v2.t3 ("
                + "k1 int, "
                + "v1 int"
                + ") distributed by hash(k1) buckets 3 "
                + "properties('replication_num' = '1');");
    }

    /**
     * Verify that runtime filters produced by INTERSECT correctly compute
     * locality by comparing builder and target fragment IDs.
     */
    @Test
    public void testIntersectRuntimeFilterLocality() throws Exception {
        String sql = "select k1 from test_rf_v2.t1 intersect select k1 from test_rf_v2.t2";
        verifySetOpRuntimeFilterLocality(sql);
    }

    /**
     * Verify that runtime filters produced by EXCEPT correctly compute locality.
     */
    @Test
    public void testExceptRuntimeFilterLocality() throws Exception {
        String sql = "select k1 from test_rf_v2.t1 except select k1 from test_rf_v2.t2";
        verifySetOpRuntimeFilterLocality(sql);
    }

    /**
     * Multi-child INTERSECT: verify runtime filter locality with 3 branches.
     */
    @Test
    public void testMultiChildIntersectRuntimeFilterLocality() throws Exception {
        String sql = "select k1 from test_rf_v2.t1 "
                + "intersect select k1 from test_rf_v2.t2 "
                + "intersect select k1 from test_rf_v2.t3";
        verifySetOpRuntimeFilterLocality(sql);
    }

    /**
     * Run the full Nereids planner pipeline (including CBO for stats computation)
     * and verify that each SetOperationNode-built runtime filter has locality flags
     * consistent with the fragment placement of its builder and targets.
     *
     * Before the fix, isLocalTarget was always false (hard-coded), so
     * hasRemoteTargets was always true and hasLocalTargets was always false.
     * After the fix, locality is computed by comparing fragment IDs.
     */
    @SuppressWarnings("unchecked")
    private void verifySetOpRuntimeFilterLocality(String sql) throws Exception {
        Planner planner = getSQLPlanner(sql);
        Assertions.assertNotNull(planner);

        List<RuntimeFilter> runtimeFilters = planner.getRuntimeFilters();

        int checkedCount = 0;
        for (RuntimeFilter rf : runtimeFilters) {
            PlanNode builderNode = rf.getBuilderNode();
            if (!(builderNode instanceof SetOperationNode)) {
                continue;
            }

            // Access private 'targets' field via reflection
            Field targetsField = RuntimeFilter.class.getDeclaredField("targets");
            targetsField.setAccessible(true);
            List<RuntimeFilterTarget> targets = (List<RuntimeFilterTarget>) targetsField.get(rf);
            Assertions.assertFalse(targets.isEmpty(),
                    "RuntimeFilter " + rf.getFilterId().asInt() + " has no targets");

            // Compute expected locality: all targets must be in the same fragment
            // as builder (mirrors V1 allMatch logic)
            boolean expectedLocal = true;
            for (RuntimeFilterTarget target : targets) {
                if (!target.node.getFragmentId().equals(builderNode.getFragmentId())) {
                    expectedLocal = false;
                    break;
                }
            }

            // Verify the actual locality matches expectation
            TRuntimeFilterDesc desc = rf.toThrift();
            boolean actualLocal = desc.has_local_targets;
            boolean actualRemote = desc.has_remote_targets;

            // BE requires exactly one of (hasLocal, hasRemote) to be true
            Assertions.assertNotEquals(actualLocal, actualRemote,
                    String.format("RF %d: hasLocal=%s and hasRemote=%s must differ",
                            rf.getFilterId().asInt(), actualLocal, actualRemote));

            // Verify the direction matches expected fragment-based locality
            Assertions.assertEquals(expectedLocal, actualLocal,
                    String.format("RF %d: expected hasLocalTargets=%s (builder frag=%s) but got %s",
                            rf.getFilterId().asInt(), expectedLocal,
                            builderNode.getFragmentId(), actualLocal));

            Assertions.assertEquals(!expectedLocal, actualRemote,
                    String.format("RF %d: expected hasRemoteTargets=%s but got %s",
                            rf.getFilterId().asInt(), !expectedLocal, actualRemote));

            checkedCount++;
        }

        Assertions.assertTrue(checkedCount > 0,
                "Expected at least one SetOperationNode-built runtime filter, "
                        + "but found none. Total RFs: " + runtimeFilters.size());
    }
}
