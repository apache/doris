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

package org.apache.doris.qe;

import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.physical.TopnFilter;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineInstanceParams;
import org.apache.doris.thrift.TTopnFilterDesc;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class CoordinatorTest extends TestWithFeService {

    @BeforeAll
    public void init() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase("test");
        useDatabase("test");
        createTable("create table tbl(id int, v int) distributed by hash(id)"
                + " buckets 3 properties('replication_num' = '1');");
    }

    /**
     * Verifies that FragmentExecParams.toThrift() pre-computes topnFilterDescs once and shares
     * the same list object across all TPipelineInstanceParams when instanceExecParams.size() > 1.
     *
     * Before the fix, a new List was created per instance inside the loop.
     * After the fix, one List is created outside the loop and shared by all instances.
     */
    @Test
    public void testTopnFilterDescsSharedAmongInstances() throws Exception {
        NereidsPlanner planner = plan("select * from test.tbl order by id limit 5");
        List<TopnFilter> topnFilters = planner.getTopnFilters();

        Assumptions.assumeTrue(!topnFilters.isEmpty(),
                "Query did not generate topn filters; test skipped");

        Coordinator coordinator = (Coordinator) EnvFactory.getInstance()
                .createCoordinator(connectContext, planner, null);

        // prepare() populates fragmentExecParamsMap; it is protected and accessible
        // from this package.
        coordinator.prepare();

        // Access private fragmentExecParamsMap via reflection.
        Field mapField = Coordinator.class.getDeclaredField("fragmentExecParamsMap");
        mapField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<PlanFragmentId, Coordinator.FragmentExecParams> fragMap =
                (Map<PlanFragmentId, Coordinator.FragmentExecParams>) mapField.get(coordinator);

        Assertions.assertFalse(fragMap.isEmpty());

        // Pick any fragment and inject 3 fake instances on the same host so that
        // toThrift() groups them into one TPipelineFragmentParams with 3 local entries.
        Coordinator.FragmentExecParams fragParams = fragMap.values().iterator().next();
        fragParams.instanceExecParams.clear();

        TNetworkAddress host = new TNetworkAddress("127.0.0.1", 9060);
        fragParams.instanceExecParams.add(
                new Coordinator.FInstanceExecParam(new TUniqueId(1L, 1L), host, fragParams));
        fragParams.instanceExecParams.add(
                new Coordinator.FInstanceExecParam(new TUniqueId(1L, 2L), host, fragParams));
        fragParams.instanceExecParams.add(
                new Coordinator.FInstanceExecParam(new TUniqueId(1L, 3L), host, fragParams));

        // toThrift() is package-private and accessible from this package.
        Map<TNetworkAddress, TPipelineFragmentParams> result = fragParams.toThrift(0);

        TPipelineFragmentParams pipelineParams = result.get(host);
        Assertions.assertNotNull(pipelineParams);

        List<TPipelineInstanceParams> localParamsList = pipelineParams.getLocalParams();
        Assertions.assertEquals(3, localParamsList.size());

        // Every instance must have topn_filter_descs set and non-empty.
        for (TPipelineInstanceParams lp : localParamsList) {
            Assertions.assertNotNull(lp.getTopnFilterDescs());
            Assertions.assertFalse(lp.getTopnFilterDescs().isEmpty());
        }

        // All instances must reference the SAME list object (not merely equal copies).
        // This is the invariant introduced by the commit: the list is built once outside
        // the instance loop and shared across all instances.
        List<TTopnFilterDesc> shared = localParamsList.get(0).getTopnFilterDescs();
        for (int i = 1; i < localParamsList.size(); i++) {
            Assertions.assertSame(shared, localParamsList.get(i).getTopnFilterDescs(),
                    "instance " + i + " must share the same topnFilterDescs list object");
        }
    }

    private NereidsPlanner plan(String sql) throws IOException {
        connectContext.getSessionVariable().setDisableNereidsRules(
                "PRUNE_EMPTY_PARTITION,OLAP_SCAN_TABLET_PRUNE");
        connectContext.getSessionVariable().topNLazyMaterializationThreshold = -1;
        // The test table is empty (0 rows). The topn-filter condition is:
        //   Math.max(rowCount, 1) * topnFilterRatio > limit
        // With default ratio=0.5: Math.max(0,1)*0.5=0.5 which is NOT > 5, so no filter.
        // Set ratio > 5 so that even an empty table (rowCount=0) passes the check.
        connectContext.getSessionVariable().topnFilterRatio = 10.0;
        connectContext.setThreadLocalInfo();
        UUID uuid = UUID.randomUUID();
        connectContext.setQueryId(
                new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));
        return PlanChecker.from(connectContext).plan(sql);
    }
}
