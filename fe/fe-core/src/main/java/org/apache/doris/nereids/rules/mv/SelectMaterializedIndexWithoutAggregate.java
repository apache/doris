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

import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Select materialized index, i.e., both for rollup and materialized view when aggregate is not present.
 * <p>
 * Scan OLAP table with aggregate is handled in {@link SelectMaterializedIndexWithAggregate}.
 * <p>
 * Note that we should first apply {@link SelectMaterializedIndexWithAggregate} and then
 * {@link SelectMaterializedIndexWithoutAggregate}.
 * Besides, these two rules should run in isolated batches, thus when enter this rule, it's guaranteed that there is
 * no aggregation on top of the scan.
 */
public class SelectMaterializedIndexWithoutAggregate extends AbstractSelectMaterializedIndexRule
        implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // project with pushdown filter.
                // Project(Filter(Scan))
                logicalProject(logicalFilter(logicalOlapScan().when(this::shouldSelectIndex)))
                        .then(project -> {
                            LogicalFilter<LogicalOlapScan> filter = project.child();
                            LogicalOlapScan scan = filter.child();
                            return project.withChildren(filter.withChildren(
                                    select(scan, project::getInputSlots, filter::getConjuncts)));
                        }).toRule(RuleType.MATERIALIZED_INDEX_PROJECT_FILTER_SCAN),

                // project with filter that cannot be pushdown.
                // Filter(Project(Scan))
                logicalFilter(logicalProject(logicalOlapScan().when(this::shouldSelectIndex)))
                        .then(filter -> {
                            LogicalProject<LogicalOlapScan> project = filter.child();
                            LogicalOlapScan scan = project.child();
                            return filter.withChildren(project.withChildren(
                                    select(scan, project::getInputSlots, ImmutableList::of)
                            ));
                        }).toRule(RuleType.MATERIALIZED_INDEX_FILTER_PROJECT_SCAN),

                // scan with filters could be pushdown.
                // Filter(Scan)
                logicalFilter(logicalOlapScan().when(this::shouldSelectIndex))
                        .then(filter -> {
                            LogicalOlapScan scan = filter.child();
                            return filter.withChildren(select(scan, ImmutableSet::of, filter::getConjuncts));
                        })
                        .toRule(RuleType.MATERIALIZED_INDEX_FILTER_SCAN),

                // project and scan.
                // Project(Scan)
                logicalProject(logicalOlapScan().when(this::shouldSelectIndex))
                        .then(project -> {
                            LogicalOlapScan scan = project.child();
                            return project.withChildren(
                                    select(scan, project::getInputSlots, ImmutableList::of));
                        })
                        .toRule(RuleType.MATERIALIZED_INDEX_PROJECT_SCAN),

                // only scan.
                logicalOlapScan()
                        .when(this::shouldSelectIndex)
                        .then(scan -> select(scan, scan::getOutputSet, ImmutableList::of))
                        .toRule(RuleType.MATERIALIZED_INDEX_SCAN)
        );
    }

    /**
     * Select materialized index when aggregate node is not present.
     *
     * @param scan Scan node.
     * @param requiredScanOutputSupplier Supplier to get the required scan output.
     * @param predicatesSupplier Supplier to get pushdown predicates.
     * @return Result scan node.
     */
    private LogicalOlapScan select(
            LogicalOlapScan scan,
            Supplier<Set<Slot>> requiredScanOutputSupplier,
            Supplier<List<Expression>> predicatesSupplier) {
        switch (scan.getTable().getKeysType()) {
            case AGG_KEYS:
            case UNIQUE_KEYS:
                OlapTable table = scan.getTable();
                long baseIndexId = table.getBaseIndexId();
                int baseIndexKeySize = table.getKeyColumnsByIndexId(table.getBaseIndexId()).size();
                // No aggregate on scan.
                // So only base index and indexes that have all the keys could be used.
                List<MaterializedIndex> candidates = table.getVisibleIndex().stream()
                        .filter(index -> index.getId() == baseIndexId
                                || table.getKeyColumnsByIndexId(index.getId()).size() == baseIndexKeySize)
                        .collect(Collectors.toList());
                PreAggStatus preAgg = PreAggStatus.off("No aggregate on scan.");
                if (candidates.size() == 1) {
                    // `candidates` only have base index.
                    return scan.withMaterializedIndexSelected(preAgg, ImmutableList.of(baseIndexId));
                } else {
                    return scan.withMaterializedIndexSelected(preAgg,
                            filterAndOrder(candidates.stream(), scan, requiredScanOutputSupplier.get(),
                                    predicatesSupplier.get()));
                }
            case DUP_KEYS:
                // Set pre-aggregation to `on` to keep consistency with legacy logic.
                return scan.withMaterializedIndexSelected(PreAggStatus.on(),
                        filterAndOrder(scan.getTable().getVisibleIndex().stream(), scan,
                                requiredScanOutputSupplier.get(),
                                predicatesSupplier.get()));
            default:
                throw new RuntimeException("Not supported keys type: " + scan.getTable().getKeysType());
        }
    }
}
