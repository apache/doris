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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;

import java.util.Collections;
import java.util.List;

/**
 * Transforms a normalized MV plan into delta INSERT commands.
 *
 * <p>Supported patterns:
 * <ul>
 *   <li>SCAN_ONLY:    ResultSink → Project → OlapScan</li>
 *   <li>PROJECT_SCAN: ResultSink → Project → Project → OlapScan</li>
 * </ul>
 *
 * <p>Aggregate plans are not yet supported and will be routed to
 * {@code AggDeltaStrategy} once strategy routing is implemented.
 */
public class IvmDeltaRewriter {

    /**
     * Rewrites the normalized plan into a list of delta command bundles.
     * Currently produces exactly one INSERT bundle for the single base table scan.
     */
    public List<DeltaCommandBundle> rewrite(Plan normalizedPlan, IvmDeltaRewriteContext ctx) {
        Plan queryPlan = AbstractDeltaStrategy.stripResultSink(normalizedPlan);
        rejectAggPlan(queryPlan);
        LogicalOlapScan scan = AbstractDeltaStrategy.extractScan(queryPlan);
        BaseTableInfo baseTableInfo = AbstractDeltaStrategy.extractBaseTableInfo(scan);
        Command insertCommand = AbstractDeltaStrategy.buildInsertCommand(queryPlan, ctx);
        return Collections.singletonList(new DeltaCommandBundle(baseTableInfo, insertCommand));
    }

    /** Guard: reject aggregate plans until AggDeltaStrategy routing is wired in. */
    private void rejectAggPlan(Plan plan) {
        if (plan.containsType(LogicalAggregate.class)) {
            throw new AnalysisException(
                    "IVM delta rewrite does not yet support aggregate plans; "
                            + "AggDeltaStrategy routing is not yet implemented");
        }
    }
}
