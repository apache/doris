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

import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.expression.rules.InferPredicateFromMonotonicFunction;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.table.File;
import org.apache.doris.nereids.trees.expressions.functions.table.Hdfs;
import org.apache.doris.nereids.trees.expressions.functions.table.Http;
import org.apache.doris.nereids.trees.expressions.functions.table.Local;
import org.apache.doris.nereids.trees.expressions.functions.table.S3;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Add bare-column predicates that storage min/max indexes can consume for OLAP scans, external
 * catalog file scans, and external-file table-valued functions. This post-processor runs after CBO
 * and partition-predicate removal, so the extra predicates improve scan pruning without changing
 * cardinality estimation, join planning, or materialized-view matching.
 */
public class AddMonotonicFunctionPruningPredicates extends PlanPostProcessor {

    @Override
    public Plan visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, CascadesContext context) {
        filter = (PhysicalFilter<? extends Plan>) super.visit(filter, context);
        Plan child = filter.child();
        if (!supportsStoragePruning(child)) {
            return filter;
        }

        Set<Expression> rewrittenConjuncts = new LinkedHashSet<>();
        for (Expression conjunct : filter.getConjuncts()) {
            Expression rewritten = InferPredicateFromMonotonicFunction.inferForPruning(conjunct);
            rewrittenConjuncts.addAll(ExpressionUtils.extractConjunction(rewritten));
        }
        if (rewrittenConjuncts.equals(filter.getConjuncts())) {
            return filter;
        }
        return filter.withConjunctsAndChild(rewrittenConjuncts, child)
                .copyStatsAndGroupIdFrom((AbstractPhysicalPlan) filter);
    }

    private static boolean supportsStoragePruning(Plan plan) {
        if (plan instanceof PhysicalStorageLayerAggregate) {
            plan = ((PhysicalStorageLayerAggregate) plan).getRelation();
        }
        if (plan instanceof PhysicalOlapScan) {
            return true;
        }
        if (plan instanceof PhysicalFileScan) {
            ExternalTable table = ((PhysicalFileScan) plan).getTable();
            return table instanceof PluginDrivenExternalTable
                    && ((PluginDrivenExternalTable) table).supportsStoragePredicatePruning();
        }
        if (plan instanceof PhysicalTVFRelation) {
            // Do not call getCatalogFunction() here: constructing an external-file TVF may list remote
            // files. Match the already-bound Nereids function type without adding post-processing I/O.
            TableValuedFunction function = ((PhysicalTVFRelation) plan).getFunction();
            return function instanceof File || function instanceof Hdfs || function instanceof Http
                    || function instanceof Local || function instanceof S3;
        }
        return false;
    }
}
