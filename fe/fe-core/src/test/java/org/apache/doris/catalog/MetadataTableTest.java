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

package org.apache.doris.catalog;

import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.planner.BackendPartitionedSchemaScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class MetadataTableTest extends TestWithFeService {

    @Override
    protected int backendNum() {
        return 2;
    }

    @Test
    public void testScanBackendActiveTasks() throws Exception {
        useDatabase("information_schema");

        LogicalPlan parsed = new NereidsParser().parseSingle(
                "select  sum(SCAN_ROWS),  sum(SCAN_BYTES)  from  backend_active_tasks  where  QUERY_ID  =  'd299cb2156ef4870-aea578938f703503'");
        StatementContext statementContext = new StatementContext();
        NereidsPlanner nereidsPlanner = new NereidsPlanner(statementContext);
        nereidsPlanner.plan(new LogicalPlanAdapter(parsed, statementContext));
        List<PlanFragment> fragments = nereidsPlanner.getFragments();
        PlanNode planRoot = fragments.get(fragments.size() - 1).getPlanRoot();
        List<BackendPartitionedSchemaScanNode> scanNodes = new ArrayList<>();
        planRoot.collect(BackendPartitionedSchemaScanNode.class, scanNodes);
        BackendPartitionedSchemaScanNode scanNode = scanNodes.get(0);
        List<TScanRangeLocations> scanRangeLocations = scanNode.getScanRangeLocations(0);
        Assertions.assertEquals(backendNum(), scanRangeLocations.size());
    }
}
