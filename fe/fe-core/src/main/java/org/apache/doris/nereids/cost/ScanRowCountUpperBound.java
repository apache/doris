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

package org.apache.doris.nereids.cost;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccSnapshot;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;

import java.util.Optional;
import java.util.OptionalLong;

/** Prove cost-only row bounds without changing cardinality or selecting a physical alternative. */
final class ScanRowCountUpperBound {
    private ScanRowCountUpperBound() {
    }

    static OptionalLong forGroup(Group group) {
        if (group.getLogicalExpressions().isEmpty()) {
            // Implementation-created groups may have no logical expression from which to prove a bound.
            return OptionalLong.empty();
        }
        // The logical expression describes every equivalent physical candidate in this group.
        // Its bound keeps exchange costs independent of which child is cheapest or ranked next.
        GroupExpression expression = group.getFirstLogicalExpression();
        Plan plan = expression.getPlan();
        if (plan instanceof LogicalFileScan) {
            LogicalFileScan scan = (LogicalFileScan) plan;
            if (scan.getTableSnapshot().isPresent() || scan.getScanParams().isPresent()) {
                // Explicit selectors may name moving references instead of a fixed scan snapshot.
                return OptionalLong.empty();
            }
            return forScan(scan.getTable(), scan.getSelectedPartitions(),
                    scan.getTableSnapshot(), scan.getScanParams());
        }
        if (plan instanceof LogicalFilter || plan instanceof LogicalProject || plan instanceof LogicalLimit
                || plan instanceof LogicalSort || plan instanceof LogicalTopN) {
            return forGroup(expression.child(0));
        }
        // Joins, global aggregates on empty input, generators and set operations need their own proofs.
        return OptionalLong.empty();
    }

    static OptionalLong forScan(PhysicalFileScan scan) {
        return forScan(scan.getTable(), scan.getSelectedPartitions(),
                scan.getTableSnapshot(), scan.getScanParams());
    }

    private static OptionalLong forScan(TableIf table, SelectedPartitions selected,
            Optional<TableSnapshot> tableSnapshot, Optional<TableScanParams> scanParams) {
        if (selected.isPruned && selected.state == SelectedPartitions.State.MATERIALIZED) {
            Optional<MvccSnapshot> snapshot = MvccUtil.getSnapshotFromContext(table, tableSnapshot, scanParams);
            if (snapshot.isPresent() && snapshot.get() instanceof PluginDrivenMvccSnapshot) {
                // Only inspect already-materialized metadata from this statement's read snapshot.
                return ((PluginDrivenMvccSnapshot) snapshot.get())
                        .getSelectedPartitionRowCount(selected.selectedPartitions.keySet());
            }
        }
        return OptionalLong.empty();
    }
}
