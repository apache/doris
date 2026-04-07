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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Detect DISTINCT queries where the distinct columns exactly match the key column prefix
 * of the underlying OlapScan table, and mark the aggregate to use sorted streaming distinct.
 *
 * This optimization avoids hash table usage by leveraging the fact that data from
 * AGG_KEYS / UNIQUE_KEYS (MOR) tables is already sorted by key columns per tablet.
 *
 * Only applies when:
 * 1. The aggregate is a pure DISTINCT (no aggregate functions, isDistinct() == true)
 * 2. The table is AGG_KEYS or UNIQUE_KEYS MOR (scan output is sorted per tablet)
 * 3. The table is not ZORDER sorted
 * 4. The DISTINCT columns exactly match a prefix of the table's key columns
 */
public class UseDistinctInOrder extends PlanPostProcessor {
    private static final Logger LOG = LogManager.getLogger(UseDistinctInOrder.class);

    @Override
    public Plan visitPhysicalHashAggregate(PhysicalHashAggregate<? extends Plan> agg, CascadesContext ctx) {
        // First, recursively visit children
        agg.child().accept(this, ctx);

        // Check session variable
        if (!ConnectContext.get().getSessionVariable().enableSortedDistinct) {
            return agg;
        }

        // Only apply to pure DISTINCT aggregations (no aggregate functions)
        if (!agg.isDistinct()) {
            return agg;
        }

        // Find the OlapScan under this agg (allow Filter/Project in between)
        PhysicalOlapScan scan = findOlapScan(agg.child());
        if (scan == null) {
            return agg;
        }

        OlapTable table = scan.getTable();

        // Only AGG_KEYS and UNIQUE_KEYS MOR tables have sorted scan output per tablet
        KeysType keysType = table.getKeysType();
        if (keysType != KeysType.AGG_KEYS && !table.isMorTable()) {
            return agg;
        }

        // ZORDER sort is not simple lexicographic order
        if (table.isZOrderSort()) {
            return agg;
        }

        // Get key columns for the selected index
        List<Column> keyColumns = table.getKeyColumnsByIndexId(scan.getSelectedIndexId());
        if (keyColumns.isEmpty()) {
            return agg;
        }

        // Collect DISTINCT column names (= group by expressions, which should all be Slots)
        List<Expression> groupByExprs = agg.getGroupByExpressions();
        Set<String> distinctColumnNames = new HashSet<>();
        for (Expression expr : groupByExprs) {
            if (!(expr instanceof Slot)) {
                return agg;  // non-slot expression, can't match key columns
            }
            distinctColumnNames.add(((Slot) expr).getName());
        }

        // Check full coverage: DISTINCT columns == key prefix
        // The key columns (in order) must form a prefix that exactly equals the distinct column set
        if (distinctColumnNames.size() > keyColumns.size()) {
            return agg;  // more distinct columns than key columns, impossible to fully cover
        }

        // Check that the first N key columns (N = distinct column count) ARE exactly the distinct columns
        for (int i = 0; i < distinctColumnNames.size(); i++) {
            if (!distinctColumnNames.contains(keyColumns.get(i).getName())) {
                return agg;  // key column prefix doesn't match
            }
        }

        // Full coverage confirmed! Mark the aggregate.
        if (LOG.isDebugEnabled()) {
            LOG.debug("UseDistinctInOrder: marking aggregate {} for sorted distinct on table {}",
                    agg.getId(), table.getName());
        }
        agg.setUseSortedDistinct(true);
        return agg;
    }

    /**
     * Walk down through Project/Filter nodes to find the OlapScan.
     */
    private PhysicalOlapScan findOlapScan(Plan plan) {
        if (plan instanceof PhysicalOlapScan) {
            return (PhysicalOlapScan) plan;
        }
        if (plan instanceof PhysicalProject || plan instanceof PhysicalFilter) {
            if (plan.children().size() == 1) {
                return findOlapScan(plan.child(0));
            }
        }
        return null;
    }
}
