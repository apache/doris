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

package org.apache.doris.nereids.rules.mv;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;

import com.google.common.collect.ImmutableList;

/**
 * Select rollup index when aggregate is not present.
 * <p>
 * Scan OLAP table with aggregate is handled in {@link SelectRollupWithAggregate}. This rule is to disable
 * pre-aggregation for OLAP scan when there is no aggregate plan.
 * <p>
 * Note that we should first apply {@link SelectRollupWithAggregate} and then {@link SelectRollupWithoutAggregate}.
 * Besides, these two rules should run in isolated batches, thus when enter this rule, it's guaranteed that there is
 * no aggregation on top of the scan.
 */
public class SelectRollupWithoutAggregate extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalOlapScan()
                .whenNot(LogicalOlapScan::isRollupSelected)
                .then(this::scanWithoutAggregate)
                .toRule(RuleType.ROLLUP_WITH_OUT_AGG);
    }

    private LogicalOlapScan scanWithoutAggregate(LogicalOlapScan scan) {
        switch (scan.getTable().getKeysType()) {
            case AGG_KEYS:
            case UNIQUE_KEYS:
                return scan.withMaterializedIndexSelected(PreAggStatus.off("No aggregate on scan."),
                        ImmutableList.of(scan.getTable().getBaseIndexId()));
            default:
                // Set pre-aggregation to `on` to keep consistency with legacy logic.
                return scan.withMaterializedIndexSelected(PreAggStatus.on(),
                        ImmutableList.of(scan.getTable().getBaseIndexId()));
        }
    }
}
