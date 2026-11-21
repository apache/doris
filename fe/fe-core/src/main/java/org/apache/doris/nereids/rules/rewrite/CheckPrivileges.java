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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.analysis.UserAuthentication;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalView;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.roaringbitmap.RoaringBitmap;

import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * CheckPrivileges
 *
 * The privChecked flag is on StatementContext to support view permission passthrough:
 * when InlineLogicalView expands a view, the outer CheckPrivileges has already verified
 * the view-level permission, so the inner tables should not be re-checked.
 *
 * For CTEs, since LogicalCTEConsumer is a leaf node and the producer subtree is not
 * traversed by ColumnPruning, we override visitLogicalCTEConsumer to explicitly
 * traverse the producer plan and check privileges on its tables.
 *
 * A statement that reads rows of a table and none of its columns - COUNT(*), COUNT(1),
 * SELECT 1 FROM t - can run for a user who holds SELECT on some column of the table and not
 * on the table, as it can in MySQL and PostgreSQL. The checks above do not see it that way:
 * a bare table nothing is read from is asked for SELECT on the table, and a table written
 * with an alias, a view and a subquery sit under a project in which ColumnPruning keeps one
 * column of its own choosing so that the plan still has a column to produce, so they are
 * asked for that column. When such a check is refused and the user holds some column of the
 * table, the plan is walked a second time without that kept column; a table none of whose
 * columns is then needed passes. A column that an expression is computed from stays needed,
 * read or not. The second walk is used only for a table reached through the plan nodes
 * listed in isPlainNode; anywhere else the refusal stands. A statement that passes the first
 * check never takes the second walk.
 */
public class CheckPrivileges extends ColumnPruning {
    private JobContext jobContext;
    private final Set<CTEId> checkedCteIds = new HashSet<>();

    // The plan being checked, kept for the second walk.
    private Plan root;
    // The second walk: on while it runs, and what it found. A relation or view is "unread" if it was
    // reached with no visible column needed, and "read" if it was reached with one; a node reached both
    // ways counts as read. All null until the first refusal that needs them.
    private boolean collectingUnread;
    private Set<Plan> unread;
    private Set<Plan> read;
    private Set<CTEId> collectedCteIds;
    // Relations and views by what lies between them and the root: only plain nodes, or something else.
    private Set<Plan> plainlyReached;
    private Set<Plan> otherwiseReached;

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        StatementContext stmtCtx = jobContext.getCascadesContext().getStatementContext();
        if (stmtCtx.isPrivChecked()) {
            return plan;
        }
        this.jobContext = jobContext;
        this.root = plan;
        super.rewriteRoot(plan, jobContext);
        stmtCtx.setPrivChecked(true);
        // don't rewrite plan, because Reorder expect no LogicalProject on LogicalJoin
        return plan;
    }

    @Override
    public Plan visitLogicalView(LogicalView<? extends Plan> view, PruneContext context) {
        Set<String> usedColumns = computeUsedColumns(view, context.requiredSlotsIds);
        if (collectingUnread) {
            recordUnreadOrRead(view, usedColumns);
        } else {
            checkColumnPrivileges(view, view.getView(), usedColumns);
        }

        // stop check privilege in the view
        return view;
    }

    @Override
    public Plan visitLogicalTVFRelation(LogicalTVFRelation tvfRelation, PruneContext context) {
        if (!collectingUnread) {
            TableValuedFunction tvf = tvfRelation.getFunction();
            tvf.checkAuth(jobContext.getCascadesContext().getConnectContext());
        }
        return super.visitLogicalTVFRelation(tvfRelation, context);
    }

    @Override
    public Plan visitLogicalRelation(LogicalRelation relation, PruneContext context) {
        if (relation instanceof LogicalCatalogRelation) {
            TableIf table = ((LogicalCatalogRelation) relation).getTable();
            Set<String> usedColumns = computeUsedColumns(relation, context.requiredSlotsIds);
            if (collectingUnread) {
                recordUnreadOrRead(relation, usedColumns);
            } else {
                checkColumnPrivileges(relation, table, usedColumns);
            }
        }
        return super.visitLogicalRelation(relation, context);
    }

    /**
     * On the second walk, a project nothing is read from asks its child only for what its own expressions
     * are computed from, instead of for the one column ColumnPruning would keep in it. A column the project
     * merely passes through is not read. A column an expression is computed from is: the statement may
     * still evaluate that expression when it runs, and an expression can fail. Only a project is treated
     * this way: an aggregate that is not read still groups by its keys, and those are read.
     */
    @Override
    public Plan visitLogicalProject(LogicalProject<? extends Plan> project, PruneContext context) {
        if (collectingUnread && context.needPrune && !requiresAnyOf(project.getOutputs(), context)) {
            RoaringBitmap computedFromIds = new RoaringBitmap();
            ImmutableList.Builder<Slot> computedFrom = ImmutableList.builder();
            for (NamedExpression output : project.getProjects()) {
                Expression computed = output instanceof Alias ? ((Alias) output).child() : output;
                if (computed instanceof SlotReference) {
                    continue;
                }
                for (Slot slot : computed.getInputSlots()) {
                    if (!computedFromIds.contains(slot.getExprId().asInt())) {
                        computedFromIds.add(slot.getExprId().asInt());
                        computedFrom.add(slot);
                    }
                }
            }
            project.child().accept(this, new PruneContext(project, computedFromIds, computedFrom.build(), true));
            // the plan this walk returns is thrown away, like the first one's
            return project;
        }
        return super.visitLogicalProject(project, context);
    }

    private static boolean requiresAnyOf(List<NamedExpression> outputs, PruneContext context) {
        for (NamedExpression output : outputs) {
            if (context.requiredSlotsIds.contains(output.getExprId().asInt())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Plan visitLogicalCTEConsumer(LogicalCTEConsumer consumer, PruneContext context) {
        CTEId cteId = consumer.getCteId();
        StatementContext stmtCtx = jobContext.getCascadesContext().getStatementContext();
        Plan producerPlan = stmtCtx.getCteProducerByCteId(cteId);
        if (producerPlan != null && (collectingUnread ? collectedCteIds : checkedCteIds).add(cteId)) {
            PruneContext producerContext = new PruneContext(
                    null, producerPlan.getOutputExprIdBitSet(), ImmutableList.of(), true);
            producerPlan.accept(this, producerContext);
        }
        return super.visitLogicalCTEConsumer(consumer, context);
    }

    private Set<String> computeUsedColumns(Plan plan, RoaringBitmap requiredSlotIds) {
        List<Slot> outputs = plan.getOutput();
        Map<Integer, Slot> idToSlot = new LinkedHashMap<>(outputs.size());
        for (Slot output : outputs) {
            idToSlot.putIfAbsent(output.getExprId().asInt(), output);
        }

        Set<String> usedColumns = Sets.newLinkedHashSetWithExpectedSize(idToSlot.size());
        for (Integer requiredSlotId : requiredSlotIds) {
            Slot slot = idToSlot.get(requiredSlotId);
            if (slot != null) {
                // don't check privilege for hidden column, e.g. __DORIS_DELETE_SIGN__
                if (slot instanceof SlotReference && ((SlotReference) slot).getOriginalColumn().isPresent()
                        && !((SlotReference) slot).getOriginalColumn().get().isVisible()) {
                    continue;
                }
                usedColumns.add(slot.getName());
            }
        }
        return usedColumns;
    }

    private void checkColumnPrivileges(Plan relationOrView, TableIf table, Set<String> usedColumns) {
        CascadesContext cascadesContext = jobContext.getCascadesContext();
        ConnectContext connectContext = cascadesContext.getConnectContext();
        try {
            UserAuthentication.checkPermission(table, connectContext, usedColumns);
        } catch (UserException e) {
            // The refusal stands unless the statement reads no column of the table and the user holds one
            // of its columns. Asked in this order so that a user who holds no column of the table, and a
            // table whose authorization source is not the built-in one, are refused as before without the
            // plan being walked again.
            Optional<String> heldColumn = UserAuthentication.findSelectableColumn(table, connectContext);
            if (!heldColumn.isPresent() || !readsNoColumnOf(relationOrView)) {
                throw new AnalysisException(e.getMessage(), e);
            }
            // What the SQL cache replays for this table is then the grant that let the statement through.
            usedColumns = ImmutableSet.of(heldColumn.get());
        }
        StatementContext statementContext = cascadesContext.getStatementContext();
        Optional<SqlCacheContext> sqlCacheContext = statementContext.getSqlCacheContext();
        if (sqlCacheContext.isPresent()) {
            sqlCacheContext.get().addCheckPrivilegeTablesOrViews(table, usedColumns);
        }
    }

    /** Whether the statement needs no visible column of {@code relationOrView}; walks the plan on first use. */
    private boolean readsNoColumnOf(Plan relationOrView) {
        if (unread == null) {
            unread = Collections.newSetFromMap(new IdentityHashMap<>());
            read = Collections.newSetFromMap(new IdentityHashMap<>());
            plainlyReached = Collections.newSetFromMap(new IdentityHashMap<>());
            otherwiseReached = Collections.newSetFromMap(new IdentityHashMap<>());
            collectedCteIds = new HashSet<>();
            collectHowReached(root, true);
            collectingUnread = true;
            try {
                root.accept(this, new PruneContext(null, root.getOutputExprIdBitSet(), ImmutableList.of(), true));
            } finally {
                collectingUnread = false;
            }
        }
        return unread.contains(relationOrView) && !read.contains(relationOrView)
                && plainlyReached.contains(relationOrView) && !otherwiseReached.contains(relationOrView);
    }

    private void recordUnreadOrRead(Plan relationOrView, Set<String> usedColumns) {
        (usedColumns.isEmpty() ? unread : read).add(relationOrView);
    }

    private void collectHowReached(Plan plan, boolean plainSoFar) {
        if (plan instanceof LogicalCatalogRelation || plan instanceof LogicalView) {
            (plainSoFar ? plainlyReached : otherwiseReached).add(plan);
            return;
        }
        boolean plain = plainSoFar && isPlainNode(plan);
        for (Plan child : plan.children()) {
            collectHowReached(child, plain);
        }
    }

    /**
     * The plan nodes the second walk was written for and is tested under: each states every column it reads
     * from its children in its own expressions, so "nothing is asked of the child" means nothing is read
     * from it. Under any other node a refusal stays as it was. Listing a node here is a claim about that
     * node; an unlisted one costs only a refusal.
     */
    private static boolean isPlainNode(Plan plan) {
        return plan instanceof LogicalProject || plan instanceof LogicalAggregate || plan instanceof LogicalFilter
                || plan instanceof LogicalJoin || plan instanceof LogicalSort || plan instanceof LogicalTopN
                || plan instanceof LogicalLimit || plan instanceof LogicalSink;
    }
}
