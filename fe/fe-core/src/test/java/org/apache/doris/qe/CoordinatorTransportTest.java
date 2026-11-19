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
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.runtime.ThriftPlansBuilder;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public class CoordinatorTransportTest extends TestWithFeService {

    @BeforeAll
    public void init() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase("test");
        useDatabase("test");
        createTable("create table tbl(id int, v int) distributed by hash(id)"
                + " buckets 3 properties('replication_num' = '1');");
    }

    @Test
    public void testTopnFiltersInNereidsTransport() throws Exception {
        NereidsPlanner planner = plan("select * from test.tbl order by id limit 5");
        Assertions.assertFalse(planner.getTopnFilters().isEmpty());
        Coordinator coordinator = EnvFactory.getInstance().createCoordinator(connectContext, planner, null);
        Map<DistributedPlanWorker, TPipelineFragmentParamsList> params = ThriftPlansBuilder.plansToThrift(
                coordinator.getCoordinatorContext());

        Assertions.assertFalse(params.isEmpty());
        for (TPipelineFragmentParamsList backendParams : params.values()) {
            Assertions.assertFalse(backendParams.getRuntimeFilterInfo().getTopnFilterDescs().isEmpty());
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
