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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.constraint.TableIdentifier;
import org.apache.doris.common.Id;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.StructInfoNode;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.rules.exploration.mv.mapping.ExpressionMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.RelationMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Abstract context for query rewrite by materialized view, this context is different by statement context
 * which means should be instanced by new query
 */
public abstract class MaterializationContext {
    private static final Logger LOG = LogManager.getLogger(MaterializationContext.class);
    public final Map<RelationMapping, SlotMapping> queryToMaterializationSlotMappingCache = new HashMap<>();
    protected List<Table> baseTables;
    protected List<Table> baseViews;
    // The plan of materialization def sql
    protected final Plan plan;
    // The original plan of materialization sql
    protected final Plan originalPlan;
    // Should regenerate when materialization is already rewritten successfully because one query may hit repeatedly
    // make sure output is different in multi using
    protected Plan scanPlan;
    // The materialization plan output shuttled expression, this is used by generate field
    // exprToScanExprMapping
    protected List<? extends Expression> planOutputShuttledExpressions;
    // Generated mapping from materialization plan out expr to materialization scan plan out slot mapping,
    // this is used for later
    protected Map<Expression, Expression> exprToScanExprMapping = new HashMap<>();
    // Generated mapping from materialization plan out shuttled expr to materialization scan plan out slot mapping,
    // this is used for expression rewrite
    protected ExpressionMapping shuttledExprToScanExprMapping;
    // This mark the materialization context is available or not,
    // will not be used in query transparent rewritten if false
    protected boolean available = true;
    // Mark the materialization plan in the context is already rewritten successfully or not
    protected boolean success = false;
    // Mark enable record failure detail info or not, because record failure detail info is performance-depleting
    protected final boolean enableRecordFailureDetail;
    // The materialization plan struct info, construct struct info is expensive,
    // this should be constructed once for all query for performance
    protected final StructInfo structInfo;
    // Group id set that are rewritten unsuccessfully by this materialization for reducing rewrite times
    protected final Set<GroupId> matchedFailGroups = new HashSet<>();
    // Group id set that are rewritten successfully by this materialization for reducing rewrite times
    protected final Set<GroupId> matchedSuccessGroups = new HashSet<>();
    // Record the reason, if rewrite by materialization fail. The failReason should be empty if success.
    // The key is the query belonged group expression objectId, the value is the fail reasons because
    // for one materialization query may be multi when nested materialized view.
    protected final Multimap<ObjectId, Pair<String, String>> failReason = HashMultimap.create();
    protected List<String> identifier;
    // The common table id set which is used in materialization, added for performance consideration
    private BitSet commonTableIdSet;

    /**
     * MaterializationContext, this contains necessary info for query rewriting by materialization
     */
    public MaterializationContext(Plan plan, Plan originalPlan,
            CascadesContext cascadesContext, StructInfo structInfo) {
        this.plan = plan;
        this.originalPlan = originalPlan;
        StatementBase parsedStatement = cascadesContext.getStatementContext().getParsedStatement();
        this.enableRecordFailureDetail = parsedStatement != null && parsedStatement.isExplain()
                && ExplainLevel.MEMO_PLAN == parsedStatement.getExplainOptions().getExplainLevel();
        // Construct materialization struct info, catch exception which may cause planner roll back
        this.structInfo = structInfo == null
                ? constructStructInfo(plan, originalPlan, cascadesContext).orElseGet(() -> null)
                : structInfo;
        this.available = this.structInfo != null;
        if (available) {
            this.planOutputShuttledExpressions = this.structInfo.getPlanOutputShuttledExpressions();
        }
    }

    /**
     * Construct materialized view Struct info
     * @param plan maybe remove unnecessary plan node, and the logical output maybe wrong
     * @param originalPlan original plan, the output is right
     */
    public static Optional<StructInfo> constructStructInfo(Plan plan, Plan originalPlan,
                                                           CascadesContext cascadesContext) {
        try {
            return Optional.of(StructInfo.of(plan, originalPlan, cascadesContext));
        } catch (Exception exception) {
            LOG.warn(String.format("construct materialization struct info fail, materialization plan is %s",
                    plan.treeString()), exception);
            return Optional.empty();
        }
    }

    public boolean alreadyRewrite(GroupId groupId) {
        return this.matchedFailGroups.contains(groupId) || this.matchedSuccessGroups.contains(groupId);
    }

    public void addMatchedGroup(GroupId groupId, boolean rewriteSuccess) {
        if (rewriteSuccess) {
            this.matchedSuccessGroups.add(groupId);
        } else {
            this.matchedFailGroups.add(groupId);
        }
    }

    /**
     * Try to generate scan plan for materialization
     * if MaterializationContext is already rewritten successfully, then should generate new scan plan in later
     * query rewrite, because one plan may hit the materialized view repeatedly and the materialization scan output
     * should be different.
     */
    private void tryGenerateScanPlan(StructInfo queryStructInfo, CascadesContext cascadesContext) {
        if (!this.isAvailable()) {
            return;
        }
        LogicalOlapScan olapScan = doGenerateOlapScanPlan(cascadesContext);
        List<Slot> scanPlanOutput = olapScan.getOutput();
        ExpressionMapping newShuttledExprToScanExprMapping = ExpressionMapping.generate(
                this.planOutputShuttledExpressions, scanPlanOutput);
        Map<Expression, Expression> newExprToScanExprMapping = new HashMap<>();
        List<Slot> originalPlanOutput = originalPlan.getOutput();
        if (originalPlanOutput.size() == scanPlanOutput.size()) {
            for (int slotIndex = 0; slotIndex < originalPlanOutput.size(); slotIndex++) {
                newExprToScanExprMapping.put(originalPlanOutput.get(slotIndex), scanPlanOutput.get(slotIndex));
            }
        }

        List<Map<Expression, Expression>> flattenExpressionMap = newShuttledExprToScanExprMapping.flattenMap();
        Map<Expression, Expression> lineageExpressionToScanExpressionMap = flattenExpressionMap.isEmpty()
                ? ImmutableMap.of()
                : flattenExpressionMap.get(0);
        Set<Expression> relationImpliedPredicates = tryCollectRelationImpliedPredicates(
                structInfo, lineageExpressionToScanExpressionMap);
        if (relationImpliedPredicates == null) {
            if (queryStructInfo != null) {
                recordFailReason(queryStructInfo, "Rewrite relation implied predicate to mv scan fail",
                        () -> String.format("lineageExpressionToScanExpressionMap = %s",
                                lineageExpressionToScanExpressionMap));
            }
            return;
        }
        olapScan = olapScan.withRelationImpliedPredicates(relationImpliedPredicates);
        Plan newScanPlan = BindRelation.checkAndAddDeleteSignFilter(olapScan, cascadesContext.getConnectContext(),
                olapScan.getTable());

        this.shuttledExprToScanExprMapping = newShuttledExprToScanExprMapping;
        this.exprToScanExprMapping = newExprToScanExprMapping;
        this.scanPlan = newScanPlan;
    }

    public static @Nullable Set<Expression> tryCollectRelationImpliedPredicates(
            StructInfo structInfo, List<? extends Expression> planOutputShuttledExpressions, List<Slot> scanOutput) {
        ExpressionMapping shuttledExprToScanExprMapping =
                ExpressionMapping.generate(planOutputShuttledExpressions, scanOutput);
        List<Map<Expression, Expression>> flattenExpressionMap = shuttledExprToScanExprMapping.flattenMap();
        Map<Expression, Expression> lineageExpressionToScanExpressionMap = flattenExpressionMap.isEmpty()
                ? ImmutableMap.of()
                : flattenExpressionMap.get(0);
        return tryCollectRelationImpliedPredicates(structInfo, lineageExpressionToScanExpressionMap);
    }

    private static @Nullable Set<Expression> tryCollectRelationImpliedPredicates(StructInfo structInfo,
            Map<Expression, Expression> lineageExpressionToScanExpressionMap) {
        Predicates viewPredicates = Objects.requireNonNull(structInfo.getPredicates(),
                "view predicates can not be null");
        Set<Expression> viewSemanticPredicates = viewPredicates.getSemanticPredicates();
        Set<Expression> relationImpliedPredicates = new HashSet<>();
        if (viewSemanticPredicates.isEmpty()) {
            return relationImpliedPredicates;
        }

        List<? extends Expression> viewLineagePredicates = ExpressionUtils.shuttleExpressionWithLineage(
                new ArrayList<>(viewSemanticPredicates), structInfo.getTopPlan());

        Map<RelationImpliedSlotKey, SlotReference> hiddenSlots = new HashMap<>();
        for (Expression viewPredicate : viewLineagePredicates) {
            if (ExpressionUtils.isInferred(viewPredicate) || BooleanLiteral.TRUE.equals(viewPredicate)) {
                continue;
            }
            Expression rewrittenPredicate = toRelationImpliedPredicate(viewPredicate,
                    lineageExpressionToScanExpressionMap, hiddenSlots);
            if (rewrittenPredicate == null) {
                return null;
            }
            relationImpliedPredicates.add(rewrittenPredicate);
        }
        return relationImpliedPredicates;
    }

    // Rewrite a semantic predicate from the MV definition side into a relation implied predicate
    // that can be carried by the MV scan. For example, if the MV outputs `a + 1 AS x`,
    // lineageExpressionToScanExpressionMap contains `a + 1 -> mv.x`, so `a + 1 > 10`
    // is rewritten to `mv.x > 10`. If the predicate references a non-output column like `c > 10`,
    // create a hidden slot for `c`, and reuse the same hidden slot through hiddenSlots.
    private static Expression toRelationImpliedPredicate(Expression predicate,
            Map<Expression, Expression> lineageExpressionToScanExpressionMap,
            Map<RelationImpliedSlotKey, SlotReference> hiddenSlots) {
        AtomicBoolean invalid = new AtomicBoolean(false);
        Expression rewrittenPredicate = predicate.rewriteDownShortCircuit(expression -> {
            if (invalid.get()) {
                return expression;
            }
            Expression scanExpression = lineageExpressionToScanExpressionMap.get(expression);
            if (scanExpression != null) {
                // When an MV output expression is matched, replace it with the MV scan output slot,
                // for example `a + 1` -> `mv.x`.
                return scanExpression;
            }
            if (expression instanceof SlotReference) {
                SlotReference slot = (SlotReference) expression;
                if (!slot.getOriginalTable().isPresent() || !slot.getOriginalColumn().isPresent()) {
                    invalid.set(true);
                    return expression;
                }
                RelationImpliedSlotKey key = RelationImpliedSlotKey.of(slot);
                // When a non-output base-table column is still identifiable, create a hidden slot for it.
                return hiddenSlots.computeIfAbsent(key,
                        ignored -> slot.withExprId(StatementScopeIdGenerator.newExprId()));
            }
            return expression;
        });
        return invalid.get() ? null : rewrittenPredicate;
    }

    private static class RelationImpliedSlotKey {
        private final TableIdentifier tableIdentifier;
        private final String columnName;
        private final List<String> subPath;

        private RelationImpliedSlotKey(TableIdentifier tableIdentifier, String columnName, List<String> subPath) {
            this.tableIdentifier = tableIdentifier;
            this.columnName = columnName;
            this.subPath = subPath;
        }

        private static RelationImpliedSlotKey of(SlotReference slot) {
            return new RelationImpliedSlotKey(new TableIdentifier(slot.getOriginalTable().get()),
                    slot.getOriginalColumn().get().getName(), slot.getSubPath());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof RelationImpliedSlotKey)) {
                return false;
            }
            RelationImpliedSlotKey that = (RelationImpliedSlotKey) o;
            return Objects.equals(tableIdentifier, that.tableIdentifier)
                    && Objects.equals(columnName, that.columnName)
                    && Objects.equals(subPath, that.subPath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableIdentifier, columnName, subPath);
        }
    }

    /**
     * Should clear scan plan after materializationContext is already rewritten successfully,
     * Because one plan may hit the materialized view repeatedly and the materialization scan output
     * should be different.
     */
    public void clearScanPlan(CascadesContext cascadesContext) {
        this.scanPlan = null;
        this.shuttledExprToScanExprMapping = null;
        this.exprToScanExprMapping = null;
    }

    public void addSlotMappingToCache(RelationMapping relationMapping, SlotMapping slotMapping) {
        queryToMaterializationSlotMappingCache.put(relationMapping, slotMapping);
    }

    public SlotMapping getSlotMappingFromCache(RelationMapping relationMapping) {
        return queryToMaterializationSlotMappingCache.get(relationMapping);
    }

    /**
     * Try to generate scan plan for materialization
     * if MaterializationContext is already rewritten successfully, then should generate new scan plan in later
     * query rewrite, because one plan may hit the materialized view repeatedly and the materialization scan output
     * should be different
     */
    abstract LogicalOlapScan doGenerateOlapScanPlan(CascadesContext cascadesContext);

    /**
     * Get materialization unique identifier which identify it
     */
    public abstract List<String> generateMaterializationIdentifier();

    /**
     * Common method for generating materialization identifier by index name
     */
    public static List<String> generateMaterializationIdentifier(OlapTable olapTable, String indexName) {
        return indexName == null
                ? ImmutableList.of(olapTable.getDatabase().getCatalog().getName(),
                        olapTable.getDatabase().getFullName(),
                        olapTable.getName())
                : ImmutableList.of(olapTable.getDatabase().getCatalog().getName(),
                        olapTable.getDatabase().getFullName(),
                        olapTable.getName(), indexName);
    }

    /**
     * Common method for generating materialization identifier by index id
     */
    public static List<String> generateMaterializationIdentifierByIndexId(OlapTable olapTable, Long indexId) {
        return indexId == null
                ? ImmutableList.of(olapTable.getDatabase().getCatalog().getName(),
                olapTable.getDatabase().getFullName(),
                olapTable.getName())
                : ImmutableList.of(olapTable.getDatabase().getCatalog().getName(),
                        olapTable.getDatabase().getFullName(),
                        olapTable.getName(), olapTable.getIndexNameById(indexId));
    }

    /**
     * Get String info which is used for to string
     */
    abstract String getStringInfo();

    /**
     * Get materialization plan statistics,
     * the key is the identifier of statistics which is usual the scan plan relationId or something similar
     * the value is original plan statistics.
     * the statistics is used by cost estimation when the materialization is used
     * Which should be the materialization origin plan statistics
     */
    abstract Optional<Pair<Id, Statistics>> getPlanStatistics(CascadesContext cascadesContext);

    // original plan statistics is generated by origin plan, and the column expression in statistics
    // should be keep consistent to mv scan plan
    protected Statistics normalizeStatisticsColumnExpression(Statistics originalPlanStatistics) {
        Map<Expression, ColumnStatistic> normalizedExpressionMap = new HashMap<>();
        // this statistics column expression is materialization origin plan, should normalize it to
        // materialization scan plan
        for (Map.Entry<Expression, ColumnStatistic> entry : originalPlanStatistics.columnStatistics().entrySet()) {
            Expression targetExpression = entry.getKey();
            Expression sourceExpression = this.getExprToScanExprMapping().get(targetExpression);
            if (sourceExpression != null && targetExpression instanceof NamedExpression
                    && sourceExpression instanceof NamedExpression) {
                normalizedExpressionMap.put(MaterializedViewUtils.normalizeExpression(
                        (NamedExpression) sourceExpression, (NamedExpression) targetExpression, false).toSlot(),
                        entry.getValue());
            }
        }
        return originalPlanStatistics.withExpressionToColumnStats(normalizedExpressionMap);
    }

    /**
     * Calc the relation is chosen finally or not
     */
    abstract boolean isFinalChosen(Relation relation);

    public Plan getPlan() {
        return plan;
    }

    public Plan getOriginalPlan() {
        return originalPlan;
    }

    public @Nullable Plan getScanPlan(StructInfo queryStructInfo, CascadesContext cascadesContext) {
        if (this.scanPlan == null || this.shuttledExprToScanExprMapping == null
                || this.exprToScanExprMapping == null) {
            tryGenerateScanPlan(queryStructInfo, cascadesContext);
        }
        return scanPlan;
    }

    public List<Table> getBaseTables() {
        return baseTables;
    }

    public List<Table> getBaseViews() {
        return baseViews;
    }

    public Map<Expression, Expression> getExprToScanExprMapping() {
        return exprToScanExprMapping;
    }

    public ExpressionMapping getShuttledExprToScanExprMapping() {
        return shuttledExprToScanExprMapping;
    }

    public boolean isAvailable() {
        return available;
    }

    public Multimap<ObjectId, Pair<String, String>> getFailReason() {
        return failReason;
    }

    public boolean isEnableRecordFailureDetail() {
        return enableRecordFailureDetail;
    }

    public void setSuccess(boolean success) {
        this.success = success;
        // TODO clear the fail message by according planId ?
        this.failReason.clear();
    }

    public StructInfo getStructInfo() {
        return structInfo;
    }

    public boolean isSuccess() {
        return success;
    }

    /**
     * Record fail reason when in rewriting by struct info
     */
    public void recordFailReason(StructInfo structInfo, String summary, Supplier<String> failureReasonSupplier) {
        // record it's rewritten
        if (structInfo.getTopPlan().getGroupExpression().isPresent()) {
            this.addMatchedGroup(structInfo.getTopPlan().getGroupExpression().get().getOwnerGroup().getGroupId(),
                    false);
        }
        // once success, do not record the fail reason
        if (this.success) {
            return;
        }
        this.failReason.put(structInfo.getOriginalPlanId(),
                Pair.of(summary, this.isEnableRecordFailureDetail() ? failureReasonSupplier.get() : ""));
    }

    /**
     * Record fail reason when in rewriting by queryGroupPlan
     */
    public void recordFailReason(Plan queryGroupPlan, String summary, Supplier<String> failureReasonSupplier) {
        // record it's rewritten
        if (queryGroupPlan.getGroupExpression().isPresent()) {
            this.addMatchedGroup(queryGroupPlan.getGroupExpression().get().getOwnerGroup().getGroupId(),
                    false);
        }
        // once success, do not record the fail reason
        if (this.success) {
            return;
        }
        this.failReason.put(queryGroupPlan.getGroupExpression()
                        .map(GroupExpression::getId).orElseGet(() -> new ObjectId(-1)),
                Pair.of(summary, this.isEnableRecordFailureDetail() ? failureReasonSupplier.get() : ""));
    }

    /**
     * get materialization context common table id by current currentQueryStatementContext
     */
    public BitSet getCommonTableIdSet(StatementContext currentQueryStatementContext) {
        if (commonTableIdSet != null) {
            return commonTableIdSet;
        }
        commonTableIdSet = new BitSet();
        for (StructInfoNode node : structInfo.getRelationIdStructInfoNodeMap().values()) {
            for (CatalogRelation catalogRelation : node.getCatalogRelation()) {
                commonTableIdSet.set(currentQueryStatementContext.getTableId(catalogRelation.getTable()).asInt());
            }
        }
        return commonTableIdSet;
    }

    @Override
    public String toString() {
        return getStringInfo();
    }

    /**
     * get qualifiers for all mvs rewrite success and chosen by current query.
     *
     * @param materializationContexts all mv candidates context for current query
     * @param physicalPlan the chosen plan for current query
     * @return chosen mvs' qualifier set
     */
    public static Set<List<String>> getChosenMvsQualifiers(
            List<MaterializationContext> materializationContexts, Plan physicalPlan) {
        Set<MaterializationContext> rewrittenSuccessMaterializationSet = materializationContexts.stream()
                .filter(MaterializationContext::isSuccess)
                .collect(Collectors.toSet());
        Set<List<String>> chosenMaterializationQualifiers = new HashSet<>();
        physicalPlan.accept(new DefaultPlanVisitor<Void, Void>() {
            @Override
            public Void visitPhysicalRelation(PhysicalRelation physicalRelation, Void context) {
                for (MaterializationContext rewrittenContext : rewrittenSuccessMaterializationSet) {
                    if (rewrittenContext.isFinalChosen(physicalRelation)) {
                        chosenMaterializationQualifiers.add(rewrittenContext.generateMaterializationIdentifier());
                    }
                }
                return null;
            }

            @Override
            public Void visitPhysicalLazyMaterializeOlapScan(PhysicalLazyMaterializeOlapScan lazyScan, Void context) {
                PhysicalOlapScan physicalRelation = lazyScan.getScan();
                for (MaterializationContext rewrittenContext : rewrittenSuccessMaterializationSet) {
                    if (rewrittenContext.isFinalChosen(physicalRelation)) {
                        chosenMaterializationQualifiers.add(rewrittenContext.generateMaterializationIdentifier());
                    }
                }
                return null;
            }
        }, null);
        return chosenMaterializationQualifiers;
    }

    /**
     * ToSummaryString, this contains only summary info.
     */
    public static String toSummaryString(CascadesContext cascadesContext,
            Plan physicalPlan) {
        StatementContext statementContext = cascadesContext.getStatementContext();
        List<MaterializationContext> materializationContexts = cascadesContext.getMaterializationContexts();
        if (materializationContexts.isEmpty()) {
            return "";
        }
        Set<List<String>> chosenMaterializationQualifiers = getChosenMvsQualifiers(
                materializationContexts, physicalPlan);

        StringBuilder builder = new StringBuilder();
        builder.append("\nMaterializedView");
        // rewrite success and chosen
        builder.append("\nMaterializedViewRewriteSuccessAndChose:\n");
        if (!chosenMaterializationQualifiers.isEmpty()) {
            chosenMaterializationQualifiers.forEach(materializationQualifier ->
                    builder.append(statementContext.isPreMvRewritten() ? " RBO." : " CBO.")
                            .append(generateIdentifierName(materializationQualifier)).append(" chose\n"));
        }
        // rewrite success but not chosen
        builder.append("\nMaterializedViewRewriteSuccessButNotChose:\n");
        Set<List<String>> rewriteSuccessButNotChoseQualifiers = materializationContexts.stream()
                .filter(MaterializationContext::isSuccess)
                .map(MaterializationContext::generateMaterializationIdentifier)
                .filter(materializationQualifier -> !chosenMaterializationQualifiers.contains(materializationQualifier))
                .collect(Collectors.toSet());
        if (!rewriteSuccessButNotChoseQualifiers.isEmpty()) {
            rewriteSuccessButNotChoseQualifiers.forEach(materializationQualifier ->
                    builder.append(statementContext.isPreMvRewritten() ? " RBO." : " CBO.")
                            .append(generateIdentifierName(materializationQualifier)).append(" not chose\n"));
        }
        // rewrite fail
        builder.append("\nMaterializedViewRewriteFail:");
        for (MaterializationContext ctx : materializationContexts) {
            if (!ctx.isSuccess()) {
                Set<String> failReasonSet =
                        ctx.getFailReason().values().stream().map(Pair::key).collect(ImmutableSet.toImmutableSet());
                if (ctx.isEnableRecordFailureDetail()) {
                    failReasonSet = ctx.getFailReason().values().stream()
                            .map(Pair::toString)
                            .collect(ImmutableSet.toImmutableSet());
                }
                builder.append("\n")
                        .append(statementContext.isPreMvRewritten() ? " RBO." : " CBO.")
                        .append(generateIdentifierName(ctx.generateMaterializationIdentifier())).append(" fail\n")
                        .append("  FailInfo: ").append(String.join(", ", failReasonSet));
            }
        }
        return builder.toString();
    }

    /**
     * If materialized view rewrite duration is exceeded, make all materializationContexts with reason
     * materialized view rewrite duration is exceeded
     * */
    public static void makeFailWithDurationExceeded(Plan queryPlan,
            List<MaterializationContext> materializationContexts, long duration) {
        for (MaterializationContext context : materializationContexts) {
            if (context.isSuccess()) {
                continue;
            }
            context.recordFailReason(queryPlan,
                    "materialized view rewrite duration is exceeded, the duration is " + duration,
                    () -> "materialized view rewrite duration is exceeded, the duration is " + duration);
        }
    }

    private static String generateIdentifierName(List<String> qualifiers) {
        return String.join(".", qualifiers);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MaterializationContext context = (MaterializationContext) o;
        return generateMaterializationIdentifier().equals(context.generateMaterializationIdentifier());
    }

    @Override
    public int hashCode() {
        return Objects.hash(generateMaterializationIdentifier());
    }
}
