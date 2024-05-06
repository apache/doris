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
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.qe.ConnectContext;

import java.util.List;

/**
 * Used to prune empty partition.
 */
public class PruneEmptyPartition extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalOlapScan().thenApply(ctx -> {
            LogicalOlapScan scan = ctx.root;
            OlapTable table = scan.getTable();
            List<Long> ids = table.selectNonEmptyPartitionIds(scan.getSelectedPartitionIds());
            if (ids.isEmpty()) {
                return new LogicalEmptyRelation(ConnectContext.get().getStatementContext().getNextRelationId(),
                        scan.getOutput());
            }
            return scan.withSelectedPartitionIds(ids);
        }).toRule(RuleType.PRUNE_EMPTY_PARTITION);
    }
}
