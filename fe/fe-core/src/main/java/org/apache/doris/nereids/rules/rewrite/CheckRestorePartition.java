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

import org.apache.doris.catalog.Partition;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;

/**
 * If there are restore partition after rewrite throw an exception because it is not readable or writable
 */
public class CheckRestorePartition extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalOlapScan().then(this::checkRestorePartition).toRule(RuleType.CHECK_RESTORE_PARTITION);
    }

    private LogicalOlapScan checkRestorePartition(LogicalOlapScan scan) {
        if (scan.getSelectedPartitionIds() != null) {
            for (long id : scan.getSelectedPartitionIds()) {
                Partition partition = scan.getTable().getPartition(id);
                if (partition.getState() == Partition.PartitionState.RESTORE) {
                    throw new AnalysisException("Partition state is not NORMAL: "
                            + partition.getName() + ":" + "RESTORING");
                }
            }
        }
        return scan;
    }
}
