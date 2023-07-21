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

package org.apache.doris.nereids.rules.rewrite.mv;

import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.function.Consumer;

/**
 * Base class to test selecting materialized index.
 */
public abstract class BaseMaterializedIndexSelectTest extends TestWithFeService {
    protected void singleTableTest(String sql, String indexName, boolean preAgg) {
        singleTableTest(sql, scan -> {
            Assertions.assertEquals(preAgg, scan.isPreAggregation());
            Assertions.assertEquals(indexName, scan.getSelectedIndexName());
        });
    }

    protected void singleTableTest(String sql, Consumer<OlapScanNode> scanConsumer) {
        PlanChecker.from(connectContext).checkPlannerResult(sql, planner -> {
            List<ScanNode> scans = planner.getScanNodes();
            Assertions.assertEquals(1, scans.size());
            ScanNode scanNode = scans.get(0);
            Assertions.assertTrue(scanNode instanceof OlapScanNode);
            OlapScanNode olapScan = (OlapScanNode) scanNode;
            scanConsumer.accept(olapScan);
        });
    }
}
