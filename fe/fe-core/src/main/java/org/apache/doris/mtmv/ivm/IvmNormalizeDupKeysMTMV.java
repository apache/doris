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

import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import java.util.Set;

/**
 * Normalizes a query for a DUP detail IVM MV by checking it, and answers with the query itself.
 *
 * <p>A detail MV holds the query's own rows: nothing can ever reach a row it already holds, so its
 * refresh only appends what its base tables appended, and there is nothing about the query to
 * rewrite. What is left to do is check that the query is one such an MV can maintain -- that no node
 * of it is a shape whose maintenance would have to correct a row already materialized -- and this
 * rewriter is that check, node by node, together with the traversal that reaches every node to check
 * it.
 *
 * <p>Every node type this rewriter does not accept lands in {@link #visit}, which rejects it exactly
 * as the unique-key normalize does. That is what keeps the two in step: a shape the normalize does
 * not support is rejected here as well, and a shape it supports only through the row-id path is
 * rejected by name below.
 *
 * <p>The rewriter carries no state of its own: what the traversal needs is the statement's excluded
 * trigger tables, which is also all the scan check below reads, so they are the rewriter's context.
 */
public class IvmNormalizeDupKeysMTMV extends DefaultPlanRewriter<Set<TableNameInfo>> {
    /**
     * Checks that {@code plan} is a query a DUP detail MV can maintain.
     *
     * @return {@code plan} itself, which is what such an MV's normalized plan is
     */
    public Plan normalize(Plan plan, Set<TableNameInfo> excludedTriggerTables) {
        return plan.accept(this, excludedTriggerTables);
    }

    @Override
    public Plan visit(Plan plan, Set<TableNameInfo> excludedTriggerTables) {
        // Every node type not accepted below lands here, which is where the unique-key normalize
        // rejects it too: a detail MV's query may hold no other shape.
        throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                "IVM does not support plan node: " + plan.getClass().getSimpleName());
    }

    @Override
    public Plan visitLogicalOlapScan(LogicalOlapScan scan, Set<TableNameInfo> excludedTriggerTables) {
        OlapTable table = scan.getTable();
        if (table.getKeysType() != KeysType.DUP_KEYS) {
            // An AnalysisException rather than an IvmException: a refresh turns every IvmException into a
            // fallback, which would express "the premise of this MV is broken" as "try a complete refresh",
            // even though that refresh runs this same normalize and fails the same way. The premise being
            // broken is not a retry.
            throw new AnalysisException(
                    "IVM detail MV error: table " + table.getName() + " is " + table.getKeysType()
                            + ", but a detail MV can only be built over duplicate-key detail tables");
        }
        IvmUtil.validateBinlogEnabled(excludedTriggerTables, scan);
        return scan;
    }

    @Override
    public Plan visitLogicalOneRowRelation(LogicalOneRowRelation oneRowRelation,
            Set<TableNameInfo> excludedTriggerTables) {
        return visitChildren(this, oneRowRelation, excludedTriggerTables);
    }

    @Override
    public Plan visitLogicalProject(LogicalProject<? extends Plan> project,
            Set<TableNameInfo> excludedTriggerTables) {
        if (project.isDistinct()) {
            throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                    "IVM does not support DISTINCT project.");
        }
        return visitChildren(this, project, excludedTriggerTables);
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter,
            Set<TableNameInfo> excludedTriggerTables) {
        return visitChildren(this, filter, excludedTriggerTables);
    }

    @Override
    public Plan visitLogicalSubQueryAlias(LogicalSubQueryAlias<? extends Plan> alias,
            Set<TableNameInfo> excludedTriggerTables) {
        return visitChildren(this, alias, excludedTriggerTables);
    }

    @Override
    public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join,
            Set<TableNameInfo> excludedTriggerTables) {
        if (!join.getJoinType().isInnerOrCrossJoin()) {
            // An outer join retracts the rows its null side stopped matching, which means reaching
            // rows the MV already holds.
            throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                    "IVM detail MV error: a detail MV can only maintain INNER_JOIN and CROSS_JOIN, "
                            + "but the query has " + join.getJoinType());
        }
        return visitChildren(this, join, excludedTriggerTables);
    }

    @Override
    public Plan visitLogicalUnion(LogicalUnion union, Set<TableNameInfo> excludedTriggerTables) {
        if (union.getQualifier() != Qualifier.ALL) {
            throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                    "IVM does not support UNION DISTINCT. Only UNION ALL is supported.");
        }
        if (!union.getConstantExprsList().isEmpty()) {
            throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                    "IVM does not support UNION ALL with constant expressions.");
        }
        return visitChildren(this, union, excludedTriggerTables);
    }

    @Override
    public Plan visitLogicalResultSink(LogicalResultSink<? extends Plan> sink,
            Set<TableNameInfo> excludedTriggerTables) {
        return visitChildren(this, sink, excludedTriggerTables);
    }

    @Override
    public Plan visitLogicalOlapTableSink(LogicalOlapTableSink<? extends Plan> sink,
            Set<TableNameInfo> excludedTriggerTables) {
        return visitChildren(this, sink, excludedTriggerTables);
    }

    @Override
    public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate,
            Set<TableNameInfo> excludedTriggerTables) {
        // An aggregate's delta changes the row of a group, and DISTINCT folds rows together: both
        // mean reaching and correcting rows the MV already holds.
        throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                "IVM detail MV error: a detail MV holds no aggregate, so an aggregate cannot be "
                        + "maintained in one");
    }

    @Override
    public Plan visitLogicalRepeat(LogicalRepeat<? extends Plan> repeat,
            Set<TableNameInfo> excludedTriggerTables) {
        // GROUPING SETS is a repeat of aggregates, so it needs each group row corrected as well.
        throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                "IVM detail MV error: a detail MV holds no GROUPING SETS, so GROUPING SETS cannot be "
                        + "maintained in one");
    }
}
