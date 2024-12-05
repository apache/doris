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
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;

public class NereidsCoordinatorTest extends TestWithFeService {
    @BeforeAll
    public void init() throws Exception {
        FeConstants.runningUnitTest = true;

        createDatabase("test");
        useDatabase("test");

        createTable("create table tbl(id int) distributed by hash(id) buckets 10 properties('replication_num' = '1');");
    }

    @Test
    public void testNereidsCoordinatorScanRangeNum() throws IOException {
        NereidsPlanner planner = plan("select * from test.tbl");
        NereidsCoordinator coordinator = (NereidsCoordinator) EnvFactory.getInstance()
                .createCoordinator(connectContext, null, planner, null);
        int scanRangeNum = coordinator.getScanRangeNum();
        Assertions.assertEquals(10, scanRangeNum);
    }

    @Test
    public void testNereidsCoordinatorScanRangeNum2() throws IOException {
        NereidsPlanner planner = plan("select * from information_schema.columns");
        NereidsCoordinator coordinator = (NereidsCoordinator) EnvFactory.getInstance()
                .createCoordinator(connectContext, null, planner, null);
        int scanRangeNum = coordinator.getScanRangeNum();
        Assertions.assertEquals(0, scanRangeNum);
    }

    private NereidsPlanner plan(String sql) throws IOException {
        ConnectContext connectContext = createDefaultCtx();
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION,OLAP_SCAN_TABLET_PRUNE");
        connectContext.setThreadLocalInfo();

        UUID uuid = UUID.randomUUID();
        connectContext.setQueryId(new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));
        NereidsPlanner planner = PlanChecker.from(connectContext).plan(sql);
        return planner;
    }
}
