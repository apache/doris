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

package org.apache.doris.qe;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.NereidsException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalDeferMaterializeResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.statistics.ColumnStatistic;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.jd.mpc.hive.encsdk.SandboxApi;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * MaskingOptimizer applies mask_udf1/mask_udf2 to output expressions.
 * It works on Doris 4.0 Nereids planner as a CustomRewriter.
 *
 * <p>This class does not reference the Java UDF classes directly; it builds {@link Expr} / Nereids
 * expressions using the <b>function names</b> below. In production, these names must be bound to
 * the correct implementations via CREATE FUNCTION (or equivalent registration):
 * <ul>
 *   <li><b>mask_udf1</b> -> {@code org.apache.doris.udf.mask.MaskUdf1}
 *       (fe/be-java-extensions/java-udf/.../MaskUdf1.java) - non-aggregate column masking</li>
 *   <li><b>mask_udf2</b> -> {@code org.apache.doris.udf.mask.MaskUdf2}
 *       (fe/be-java-extensions/java-udf/.../MaskUdf2.java) - differential-privacy masking for aggregate outputs</li>
 * </ul>
 * FE resolves calls by name from the catalog; BE executes the class named by the function's
 * symbol (set at CREATE FUNCTION time). Ensuring the same names and correct symbol is how we
 * confirm the runtime invokes these two UDFs.
 */
public class MaskingOptimizer implements CustomRewriter {
    private static final Logger LOG = LogManager.getLogger(MaskingOptimizer.class);
    /** SQL function name for non-aggregate masking; must match CREATE FUNCTION and MaskUdf1. */
    private static final String MASK_UDF1 = "mask_udf1";
    /** SQL function name for aggregate masking; must match CREATE FUNCTION and MaskUdf2. */
    private static final String MASK_UDF2 = "mask_udf2";
    private static final ConcurrentHashMap<String, String> POLICY_CACHE = new ConcurrentHashMap<>();
    private final NereidsRewriter nereidsRewriter = new NereidsRewriter();

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        ConnectContext ctx = jobContext.getCascadesContext().getConnectContext();
        if (!isMaskingEnabled(ctx)) {
            return plan;
        }
        MaskingContext context = new MaskingContext(ctx, plan);
        return plan.accept(nereidsRewriter, context);
    }

    private boolean isMaskingEnabled(ConnectContext ctx) {
        return ctx != null && ctx.getSessionVariable() != null && ctx.getSessionVariable().maskingEnabled;
    }

    private class MaskingContext {
        private final ConnectContext ctx;
        private final Plan rootPlan;
        private final String policy;
        private final String env;
        private final Set<org.apache.doris.nereids.trees.expressions.ExprId> maskedAggExprIds;
        private final boolean disableInputMasking;

        MaskingContext(ConnectContext ctx, Plan rootPlan) {
            this(ctx, rootPlan, resolvePolicy(ctx, rootPlan),
                    Strings.nullToEmpty(ctx.getSessionVariable().maskingSandboxEnv),
                    new HashSet<>(), false);
        }

        private MaskingContext(ConnectContext ctx, Plan rootPlan, String policy, String env,
                Set<org.apache.doris.nereids.trees.expressions.ExprId> maskedAggExprIds,
                boolean disableInputMasking) {
            this.ctx = ctx;
            this.rootPlan = rootPlan;
            this.policy = policy;
            this.env = env;
            this.maskedAggExprIds = maskedAggExprIds;
            this.disableInputMasking = disableInputMasking;
        }

        private MaskingContext withInputMaskingDisabled() {
            return new MaskingContext(ctx, rootPlan, policy, env, maskedAggExprIds, true);
        }
    }

    /**
     * Resolves the masking policy string from DMS (Data Management Service), aligned with Presto ide branch.
     *
     * <p>Collects base tables from the current Nereids plan, builds
     * {@link SandboxApi.TableDescription} list with dbName, tbName, userErp, projectId, env, dataVerify, msCluster,
     * and calls {@link SandboxApi#getTablePolicy(List, String)}. Policy is cached by
     * (url, sorted table set, userErp, projectId, env, dataVerify).
     */
    private String resolvePolicy(ConnectContext ctx, Plan rootPlan) {
        if (ctx == null || ctx.getSessionVariable() == null) {
            return "";
        }
        SessionVariable session = ctx.getSessionVariable();
        String url = Strings.nullToEmpty(session.maskingDmsUrl);
        if (Strings.isNullOrEmpty(url)) {
            // Refuse to return unmasked data when DMS url is not configured.
            AnalysisException ae = new AnalysisException("masking_dms_url is empty");
            throw new NereidsException(ae);
        }
        String userErp = ctx.getQualifiedUser() != null ? ctx.getQualifiedUser() : "";
        String projectId = Strings.nullToEmpty(session.maskingSandboxProjectId);
        if (Strings.isNullOrEmpty(projectId)) {
            AnalysisException ae = new AnalysisException("masking_sandbox_project_id is empty");
            throw new NereidsException(ae);
        }

        String env = Strings.nullToEmpty(session.maskingSandboxEnv);
        if (Strings.isNullOrEmpty(env)) {
            AnalysisException ae = new AnalysisException("masking_sandbox_env is empty");
            throw new NereidsException(ae);
        }
        int dataVerify = session.maskingSandboxDataVerify ? 1 : 0;

        List<SandboxApi.TableDescription> tableList = rootPlan != null
                ? collectTableDescriptionsFromPlan(ctx, rootPlan, userErp, projectId, dataVerify)
                : new ArrayList<>();

        List<String> tableKeys = tableList.stream()
                .map(t -> (t.getDbName() != null ? t.getDbName() : "")
                        + "."
                        + (t.getTbName() != null ? t.getTbName() : ""))
                .sorted()
                .collect(Collectors.toList());
        String tableKey = String.join(",", tableKeys);
        String cacheKey = String.join("|", url, tableKey, userErp, projectId, env, String.valueOf(dataVerify));
        String cached = POLICY_CACHE.get(cacheKey);
        if (cached != null) {
            return cached;
        }
        try {
            List<SandboxApi.SandboxTablePolicy> tablePolicy = SandboxApi.getTablePolicy(tableList, url);
            String policyJson = JSONObject.toJSONString(tablePolicy);
            POLICY_CACHE.put(cacheKey, policyJson);
            return policyJson;
        } catch (Exception e) {
            LOG.warn("Failed to fetch masking policy from DMS url={}", url, e);
            AnalysisException ae =
                    new AnalysisException("Failed to fetch masking policy from DMS: " + url, e);
            throw new NereidsException(ae);
        }
    }

    /** Collect base tables from Nereids plan and build TableDescription list for DMS. */
    private List<SandboxApi.TableDescription> collectTableDescriptionsFromPlan(
            ConnectContext ctx, Plan rootPlan, String userErp, String projectId, int dataVerify) {
        if (!(rootPlan instanceof org.apache.doris.nereids.trees.plans.logical.LogicalPlan)) {
            return new ArrayList<>();
        }
        Set<TableIf> tables = PlanUtils.getTableSet(
                (org.apache.doris.nereids.trees.plans.logical.LogicalPlan) rootPlan);
        List<SandboxApi.TableDescription> list = new ArrayList<>();
        for (TableIf table : tables) {
            try {
                String dbName = table.getDatabase() != null
                        ? ClusterNamespace.getNameFromFullName(table.getDatabase().getFullName()) : "";
                String tbName = table.getName() != null ? table.getName() : "";
                if (Strings.isNullOrEmpty(dbName) && Strings.isNullOrEmpty(tbName)) {
                    continue;
                }
                SandboxApi.TableDescription desc = new SandboxApi.TableDescription();
                desc.setDbName(dbName);
                desc.setTbName(tbName);
                desc.setMsCluster("");
                desc.setUserErp(userErp);
                desc.setProjectId(projectId);
                desc.setDataVerify(dataVerify);
                list.add(desc);
            } catch (Exception e) {
                LOG.warn("Skip table for masking policy: {}", table.getName(), e);
            }
        }
        return list;
    }


    private class NereidsRewriter extends DefaultPlanRewriter<MaskingContext> {
        @Override
        public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> agg, MaskingContext context) {
            Plan newChild = agg.child().accept(this, context.withInputMaskingDisabled());
            List<NamedExpression> outputs = agg.getOutputExpressions();
            for (NamedExpression output : outputs) {
                if (containsMaskUdf1(output) || containsMaskUdf2(output)) {
                    continue;
                }
                Optional<Expression> aggExpr = extractAggregateOutputExpr(agg, output);
                if (aggExpr.isPresent()) {
                    context.maskedAggExprIds.add(output.getExprId());
                }
            }
            return newChild == agg.child() ? agg : agg.withChildren(ImmutableList.of(newChild));
        }

        @Override
        public Plan visitLogicalProject(LogicalProject<? extends Plan> project, MaskingContext context) {
            Plan newChild = project.child().accept(this, context);
            if (context.disableInputMasking) {
                return newChild == project.child()
                        ? project
                        : project.withProjectsAndChild(project.getProjects(), newChild);
            }
            boolean changed = newChild != project.child();
            List<NamedExpression> projects = project.getProjects();
            List<NamedExpression> newProjects = Lists.newArrayListWithCapacity(projects.size());
            for (NamedExpression expr : projects) {
                if (containsMaskUdf1(expr)) {
                    newProjects.add(expr);
                    continue;
                }
                if (expr instanceof SlotReference) {
                    SlotReference slot = (SlotReference) expr;
                    Optional<Expression> aggExpr = resolveAggExprFromChild(newChild, slot);
                    if (aggExpr.isPresent() && aggExpr.get() instanceof AggregateFunction) {
                        AggregateFunction aggregateFunction = (AggregateFunction) aggExpr.get();
                        if (shouldMaskAggregateExpression(aggregateFunction, context)) {
                            String aggOpName = ((AggregateFunction) aggExpr.get()).getName();
                            Expression masked = buildNereidsUdf1(
                                    buildNereidsUdf2ForExpr(slot, aggOpName, context), context);
                            newProjects.add(wrapNamedExpression(expr, masked));
                            changed = true;
                            continue;
                        }
                    }
                }
                Expression childExpr = unwrapNamedExpression(expr);
                AggregateFunction directAggregate = extractFirstAggregate(childExpr);
                if (directAggregate != null && shouldMaskAggregateExpression(directAggregate, context)) {
                    Expression masked = buildNereidsUdf1(
                            buildNereidsUdf2ForExpr(childExpr, directAggregate.getName(), context), context);
                    NamedExpression replaced = wrapNamedExpression(expr, masked);
                    newProjects.add(replaced);
                    if (replaced != expr) {
                        changed = true;
                    }
                    continue;
                }
                if (shouldMaskExpression(childExpr, context)) {
                    Expression masked = buildNereidsUdf1(childExpr, context);
                    NamedExpression replaced = wrapNamedExpression(expr, masked);
                    newProjects.add(replaced);
                    if (replaced != expr) {
                        changed = true;
                    }
                } else {
                    newProjects.add(expr);
                }
            }
            if (!changed) {
                return project;
            }
            return project.withProjectsAndChild(newProjects, newChild);
        }

        @Override
        public Plan visitLogicalResultSink(LogicalResultSink<? extends Plan> sink, MaskingContext context) {
            Plan newChild = sink.child().accept(this, context);
            List<NamedExpression> outputs = sink.getOutputExprs();
            List<NamedExpression> newOutputs = rewriteOutputExpressions(outputs, context, newChild);
            Plan projectedChild = ensureProjectedChild(newChild, newOutputs);
            boolean changed = projectedChild != sink.child() || !outputs.equals(newOutputs);
            if (!changed) {
                return sink;
            }
            LogicalResultSink<? extends Plan> newSink = sink.withOutputExprs(newOutputs);
            return newSink.withChildren(ImmutableList.of(projectedChild));
        }

        @Override
        public Plan visitLogicalDeferMaterializeResultSink(LogicalDeferMaterializeResultSink<? extends Plan> sink,
                MaskingContext context) {
            Plan newChild = sink.child().accept(this, context);
            List<NamedExpression> outputs = sink.getOutputExprs();
            List<NamedExpression> newOutputs = rewriteOutputExpressions(outputs, context, newChild);
            Plan projectedChild = ensureProjectedChild(newChild, newOutputs);
            LogicalResultSink<? extends Plan> baseSink = sink.getLogicalResultSink();
            LogicalResultSink<? extends Plan> newBaseSink = baseSink.withOutputExprs(newOutputs);
            return new LogicalDeferMaterializeResultSink<>(newBaseSink,
                    sink.getOlapTable(), sink.getSelectedIndexId(), Optional.empty(),
                    Optional.empty(), projectedChild);
        }

        private List<NamedExpression> rewriteOutputExpressions(List<NamedExpression> outputs, MaskingContext context,
                Plan childPlan) {
            List<NamedExpression> childOutputs = (List<NamedExpression>) (List<?>) childPlan.getOutput();
            boolean canAlignByOrdinal = childOutputs != null && childOutputs.size() == outputs.size();
            List<NamedExpression> newOutputs = Lists.newArrayListWithCapacity(outputs.size());
            for (int i = 0; i < outputs.size(); i++) {
                NamedExpression expr = outputs.get(i);
                NamedExpression aligned = canAlignByOrdinal ? childOutputs.get(i) : expr;

                if (containsMaskUdf1(expr)) {
                    newOutputs.add(expr);
                    continue;
                }

                Expression alignedChildExpr = unwrapNamedExpression(aligned);
                if (alignedChildExpr instanceof SlotReference) {
                    SlotReference slot = (SlotReference) alignedChildExpr;
                    Optional<Expression> aggExpr = resolveAggExprFromChild(childPlan, slot);
                    if (aggExpr.isPresent() && aggExpr.get() instanceof AggregateFunction) {
                        AggregateFunction aggregateFunction = (AggregateFunction) aggExpr.get();
                        if (shouldMaskAggregateExpression(aggregateFunction, context)) {
                            String aggOpName = ((AggregateFunction) aggExpr.get()).getName();
                            Expression masked = buildNereidsUdf1(
                                    buildNereidsUdf2ForExpr(slot, aggOpName, context), context);
                            newOutputs.add(wrapNamedExpression(aligned, masked));
                            continue;
                        }
                    }
                }

                AggregateFunction directAggregate = extractFirstAggregate(alignedChildExpr);
                if (directAggregate != null && shouldMaskAggregateExpression(directAggregate, context)) {
                    Expression masked = buildNereidsUdf1(
                            buildNereidsUdf2ForExpr(alignedChildExpr, directAggregate.getName(), context), context);
                    newOutputs.add(wrapNamedExpression(aligned, masked));
                    continue;
                }

                if (shouldMaskExpression(alignedChildExpr, context)) {
                    Expression masked = buildNereidsUdf1(alignedChildExpr, context);
                    newOutputs.add(wrapNamedExpression(aligned, masked));
                } else {
                    newOutputs.add(aligned);
                }
            }
            return newOutputs;
        }

        private Plan ensureProjectedChild(Plan childPlan, List<NamedExpression> outputs) {
            List<NamedExpression> childOutputs = (List<NamedExpression>) (List<?>) childPlan.getOutput();
            if (childOutputs.equals(outputs)) {
                return childPlan;
            }
            return new LogicalProject<>(outputs, childPlan);
        }

    }

    private NamedExpression wrapNamedExpression(NamedExpression original, Expression newChild) {
        try {
            return new Alias(original.getExprId(), ImmutableList.of(newChild),
                    original.getName(), original.getQualifier(), false);
        } catch (Exception e) {
            return new Alias(newChild);
        }
    }


    private Expression unwrapNamedExpression(NamedExpression expr) {
        if (expr.children().isEmpty()) {
            return expr;
        }
        return expr.child(0);
    }

    private Optional<Expression> resolveAggExprFromChild(Plan childPlan, SlotReference slot) {
        if (!(childPlan instanceof LogicalAggregate)) {
            return Optional.empty();
        }
        LogicalAggregate<?> agg = (LogicalAggregate<?>) childPlan;
        for (NamedExpression output : agg.getOutputExpressions()) {
            if (output.getExprId().equals(slot.getExprId())) {
                return extractAggregateOutputExpr(agg, output);
            }
        }
        if (agg.getGroupByExpressions().isEmpty() && agg.getAggregateFunctions().size() == 1) {
            AggregateFunction func = agg.getAggregateFunctions().iterator().next();
            return Optional.of(func);
        }
        return Optional.empty();
    }

    private Optional<Expression> extractAggregateOutputExpr(LogicalAggregate<?> agg, NamedExpression output) {
        Expression candidate = unwrapNamedExpression(output);
        if (containsAggregate(candidate)) {
            return Optional.of(candidate);
        }
        if (!(output instanceof SlotReference)) {
            return Optional.empty();
        }
        SlotReference outputSlot = (SlotReference) output;
        int groupBySize = agg.getGroupByExpressions().size();
        List<NamedExpression> outputs = agg.getOutputExpressions();
        for (int i = 0; i < outputs.size(); i++) {
            if (!outputs.get(i).getExprId().equals(outputSlot.getExprId())) {
                continue;
            }
            if (i < groupBySize) {
                return Optional.empty();
            }
            AggregateFunction func = getAggregateFunctionAtOutputIndex(agg, i);
            if (func != null) {
                return Optional.of(func);
            }
            if (agg.getGroupByExpressions().isEmpty() && agg.getAggregateFunctions().size() == 1) {
                return Optional.of(agg.getAggregateFunctions().iterator().next());
            }
            return Optional.empty();
        }
        if (agg.getGroupByExpressions().isEmpty() && agg.getAggregateFunctions().size() == 1) {
            return Optional.of(agg.getAggregateFunctions().iterator().next());
        }
        return Optional.empty();
    }

    private AggregateFunction getAggregateFunctionAtOutputIndex(LogicalAggregate<?> agg, int outputIndex) {
        int groupBySize = agg.getGroupByExpressions().size();
        if (outputIndex < groupBySize) {
            return null;
        }
        int aggIndex = outputIndex - groupBySize;
        int idx = 0;
        for (NamedExpression ne : agg.getOutputExpressions()) {
            Expression e = unwrapNamedExpression(ne);
            if (e instanceof AggregateFunction) {
                if (idx == aggIndex) {
                    return (AggregateFunction) e;
                }
                idx++;
            }
        }
        return idx == 0 && agg.getAggregateFunctions().size() == 1
                ? agg.getAggregateFunctions().iterator().next()
                : null;
    }

    private boolean containsMaskUdf1(Expression expr) {
        List<Expression> functions = expr.collectToList(
                new java.util.function.Predicate<org.apache.doris.nereids.trees.TreeNode<Expression>>() {
                    @Override
                    public boolean test(org.apache.doris.nereids.trees.TreeNode<Expression> node) {
                        return node instanceof Function;
                    }
                });
        for (Expression expression : functions) {
            Function func = (Function) expression;
            if (MASK_UDF1.equalsIgnoreCase(func.getName())) {
                return true;
            }
        }
        return false;
    }

    private boolean containsMaskUdf2(Expression expr) {
        List<Expression> functions = expr.collectToList(
                new java.util.function.Predicate<org.apache.doris.nereids.trees.TreeNode<Expression>>() {
                    @Override
                    public boolean test(org.apache.doris.nereids.trees.TreeNode<Expression> node) {
                        return node instanceof Function;
                    }
                });
        for (Expression expression : functions) {
            Function func = (Function) expression;
            if (MASK_UDF2.equalsIgnoreCase(func.getName())) {
                return true;
            }
        }
        return false;
    }

    private boolean containsAggregate(Expression expr) {
        return extractFirstAggregate(expr) != null;
    }

    private AggregateFunction extractFirstAggregate(Expression expr) {
        List<Expression> aggs = expr.collectToList(
                new java.util.function.Predicate<org.apache.doris.nereids.trees.TreeNode<Expression>>() {
                    @Override
                    public boolean test(org.apache.doris.nereids.trees.TreeNode<Expression> node) {
                        return node instanceof AggregateFunction;
                    }
                });
        return aggs.isEmpty() ? null : (AggregateFunction) aggs.get(0);
    }

    private Expression buildNereidsUdf1(Expression input, MaskingContext context) {
        Expression udfInput = input;
        if (input.getDataType() != null && !input.getDataType().isStringType()) {
            udfInput = new org.apache.doris.nereids.trees.expressions.Cast(
                    input, VarcharType.SYSTEM_DEFAULT, true);
        }
        List<Literal> lineage = buildNereidsLineage(input, context);
        org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral lineageLiteral =
                new org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral(
                        lineage, org.apache.doris.nereids.types.ArrayType.of(VarcharType.SYSTEM_DEFAULT));
        List<Expression> args = ImmutableList.of(
                udfInput,
                new VarcharLiteral(input.getDataType().toString()),
                new VarcharLiteral(context.policy),
                lineageLiteral,
                new VarcharLiteral(context.env)
        );
        return buildNereidsUdf(context, MASK_UDF1, args);
    }

    private Expression buildNereidsUdf2ForExpr(Expression input, String aggOpName, MaskingContext context) {
        List<Literal> ops =
                ImmutableList.of(new VarcharLiteral(aggOpName));
        org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral opsLiteral =
                new org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral(
                        ops, org.apache.doris.nereids.types.ArrayType.of(VarcharType.SYSTEM_DEFAULT));
        List<Literal> lineage =
                buildAggregateLineage(input, aggOpName, context);
        org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral lineageLiteral =
                new org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral(
                        lineage, org.apache.doris.nereids.types.ArrayType.of(VarcharType.SYSTEM_DEFAULT));
        Expression maxLiteral = new org.apache.doris.nereids.trees.expressions.literal.NullLiteral();
        Expression minLiteral = new org.apache.doris.nereids.trees.expressions.literal.NullLiteral();
        if (input instanceof SlotReference) {
            MinMax minMax = findColumnMinMax((SlotReference) input, context);
            if (minMax.max != null) {
                maxLiteral = new VarcharLiteral(minMax.max);
            }
            if (minMax.min != null) {
                minLiteral = new VarcharLiteral(minMax.min);
            }
        }
        String dataTypeStr = input.getDataType() != null ? input.getDataType().toString() : "unknown";
        List<Expression> args = ImmutableList.of(
                input,
                new VarcharLiteral(dataTypeStr),
                new VarcharLiteral(context.policy),
                opsLiteral,
                maxLiteral,
                minLiteral,
                lineageLiteral
        );
        return buildNereidsUdf(context, MASK_UDF2, args);
    }

    private List<Literal> buildAggregateLineage(
            Expression aggInput, String aggOpName, MaskingContext context) {
        List<Literal> lineage =
                buildNereidsLineage(aggInput, context);
        if (!lineage.isEmpty()) {
            return lineage;
        }
        if (!"count".equalsIgnoreCase(Strings.nullToEmpty(aggOpName))) {
            return lineage;
        }
        return buildFallbackCountLineage(context);
    }

    private List<Literal> buildFallbackCountLineage(
            MaskingContext context) {
        List<Literal> lineage = new ArrayList<>();
        if (context == null || Strings.isNullOrEmpty(context.policy)) {
            return lineage;
        }
        Map<String, Set<String>> coveredColumns = parsePolicyCoveredColumns(context.policy);
        if (coveredColumns.isEmpty()) {
            return lineage;
        }

        List<String> preferred = new ArrayList<>();
        try {
            if (context.ctx != null && context.rootPlan != null) {
                List<SandboxApi.TableDescription> tables = collectTableDescriptionsFromPlan(
                        context.ctx, context.rootPlan, "", "", 0);
                for (SandboxApi.TableDescription table : tables) {
                    String db = Strings.nullToEmpty(table.getDbName()).toLowerCase(Locale.ROOT);
                    String tb = Strings.nullToEmpty(table.getTbName()).toLowerCase(Locale.ROOT);
                    if (Strings.isNullOrEmpty(db) || Strings.isNullOrEmpty(tb)) {
                        continue;
                    }
                    String tableKey = db + "." + tb;
                    Set<String> cols = coveredColumns.get(tableKey);
                    if (cols == null || cols.isEmpty()) {
                        continue;
                    }
                    List<String> sortedCols = new ArrayList<>(cols);
                    sortedCols.sort(String::compareTo);
                    preferred.add(tableKey + "." + sortedCols.get(0));
                }
            }
        } catch (Exception e) {
            LOG.debug("Failed to build preferred fallback lineage for count aggregate", e);
        }

        if (preferred.isEmpty()) {
            List<String> tableKeys = new ArrayList<>(coveredColumns.keySet());
            tableKeys.sort(String::compareTo);
            for (String tableKey : tableKeys) {
                Set<String> cols = coveredColumns.get(tableKey);
                if (cols == null || cols.isEmpty()) {
                    continue;
                }
                List<String> sortedCols = new ArrayList<>(cols);
                sortedCols.sort(String::compareTo);
                preferred.add(tableKey + "." + sortedCols.get(0));
            }
        }

        if (!preferred.isEmpty()) {
            lineage.add(new VarcharLiteral(preferred.get(0)));
        }
        return lineage;
    }

    /** Resolve from current database first; fall back to global scope when unavailable. */
    private Expression buildNereidsUdf(MaskingContext context, String name,
            List<Expression> args) {
        String dbName = resolveNereidsDbName(context);
        if (!Strings.isNullOrEmpty(dbName)) {
            try {
                FunctionBuilder builder = context.ctx.getFunctionRegistry().findFunctionBuilder(dbName, name, args);
                return builder.build(name, args).first;
            } catch (Exception e) {
                LOG.debug("Mask udf {} not in current db {}, fallback to global: {}", name, dbName, e.getMessage());
            }
        }
        try {
            // null dbName -> FunctionRegistry scopes to GLOBAL in current logic.
            FunctionBuilder builder = context.ctx.getFunctionRegistry().findFunctionBuilder(null, name, args);
            return builder.build(name, args).first;
        } catch (Exception e) {
            LOG.warn("Failed to build nereids udf {} (global fallback): {}", name, e.getMessage());
            AnalysisException ae =
                    new AnalysisException(
                            String.format("Masking UDF '%s' cannot be resolved; refusing to return unmasked data.",
                                    name),
                            e);
            throw new NereidsException(ae);
        }
    }

    private String resolveNereidsDbName(MaskingContext context) {
        if (context == null || context.ctx == null) {
            return null;
        }
        String dbName = context.ctx.getDatabase();
        if (Strings.isNullOrEmpty(dbName)) {
            return null;
        }
        try {
            Optional<DatabaseIf> db = context.ctx.getCurrentCatalog().getDb(dbName);
            if (db.isPresent() && !Strings.isNullOrEmpty(db.get().getFullName())) {
                return db.get().getFullName();
            }
        } catch (Exception e) {
            LOG.warn("Failed to resolve full db name for {}", dbName, e);
        }
        return dbName;
    }

    private List<Literal> buildNereidsLineage(
            Expression expr, MaskingContext context) {
        List<Literal> lineage = new ArrayList<>();
        Set<String> keys = collectNereidsLineageKeys(expr, context);
        for (String key : keys) {
            lineage.add(new VarcharLiteral(key));
        }
        return lineage;
    }

    private Set<String> collectNereidsLineageKeys(Expression expr, MaskingContext context) {
        Set<String> keys = new LinkedHashSet<>();
        if (context == null || context.rootPlan == null) {
            return keys;
        }
        Expression lineaged = ExpressionUtils.shuttleExpressionWithLineage(expr, context.rootPlan);
        List<Expression> slots = lineaged.collectToList(
                new java.util.function.Predicate<org.apache.doris.nereids.trees.TreeNode<Expression>>() {
                    @Override
                    public boolean test(org.apache.doris.nereids.trees.TreeNode<Expression> node) {
                        return node instanceof SlotReference;
                    }
                });
        for (Expression e : slots) {
            SlotReference slot = (SlotReference) e;
            String key = buildLineageKey(null, null, slot.getName(), slot.getQualifier());
            if (!Strings.isNullOrEmpty(key)) {
                keys.add(key.toLowerCase(Locale.ROOT));
            }
        }
        return keys;
    }

    private boolean shouldMaskExpression(Expression expr, MaskingContext context) {
        if (context == null || Strings.isNullOrEmpty(context.policy)) {
            return false;
        }
        Set<String> lineageKeys = collectNereidsLineageKeys(expr, context);
        if (!lineageKeys.isEmpty() && shouldMaskLineageKeys(lineageKeys, context.policy)) {
            return true;
        }
        if (!isSingleTableRootPlan(context)) {
            return false;
        }
        Map<String, Set<String>> coveredColumns = parsePolicyCoveredColumns(context.policy);
        if (coveredColumns.size() != 1) {
            return false;
        }
        if (expr instanceof SlotReference) {
            String colName = ((SlotReference) expr).getName();
            if (Strings.isNullOrEmpty(colName)) {
                return false;
            }
            return coveredColumns.values().iterator().next().contains(colName.toLowerCase(Locale.ROOT));
        }
        List<Expression> slots = expr.collectToList(
                new java.util.function.Predicate<org.apache.doris.nereids.trees.TreeNode<Expression>>() {
                    @Override
                    public boolean test(org.apache.doris.nereids.trees.TreeNode<Expression> node) {
                        return node instanceof SlotReference;
                    }
                });
        if (slots.isEmpty()) {
            return false;
        }
        Set<String> covered = coveredColumns.values().iterator().next();
        for (Expression slotExpr : slots) {
            SlotReference slot = (SlotReference) slotExpr;
            String colName = slot.getName();
            if (!Strings.isNullOrEmpty(colName) && covered.contains(colName.toLowerCase(Locale.ROOT))) {
                return true;
            }
        }
        return false;
    }

    private boolean shouldMaskAggregateExpression(AggregateFunction agg, MaskingContext context) {
        if (agg == null || context == null || Strings.isNullOrEmpty(context.policy)) {
            return false;
        }
        if (shouldMaskExpression(agg, context)) {
            return true;
        }

        // count(*) / count(1) have no SlotReference lineage; apply masking when policy
        // covers any column from scanned root tables.
        List<Expression> slots = agg.collectToList(
                new java.util.function.Predicate<org.apache.doris.nereids.trees.TreeNode<Expression>>() {
                    @Override
                    public boolean test(org.apache.doris.nereids.trees.TreeNode<Expression> node) {
                        return node instanceof SlotReference;
                    }
                });
        if (!slots.isEmpty()) {
            return false;
        }
        String aggName = agg.getName();
        if (!"count".equalsIgnoreCase(aggName)) {
            return false;
        }
        if (!parsePolicyCoveredColumns(context.policy).isEmpty()) {
            return true;
        }
        return hasPolicyCoverageForRootTables(context);
    }

    private boolean hasPolicyCoverageForRootTables(MaskingContext context) {
        if (context == null || context.ctx == null || context.rootPlan == null
                || Strings.isNullOrEmpty(context.policy)) {
            return false;
        }
        Map<String, Set<String>> coveredColumns = parsePolicyCoveredColumns(context.policy);
        if (coveredColumns.isEmpty()) {
            return false;
        }
        try {
            List<SandboxApi.TableDescription> tables = collectTableDescriptionsFromPlan(context.ctx, context.rootPlan,
                    "", "", 0);
            if (tables.isEmpty()) {
                // In some planner/unit-test paths table descriptors may be unavailable.
                // Keep count(*) safe: if policy has covered columns and root output carries slots,
                // treat it as covered and apply aggregate masking.
                List<? extends Slot> outputs = context.rootPlan.getOutput();
                return outputs != null && !outputs.isEmpty();
            }
            for (SandboxApi.TableDescription table : tables) {
                String db = Strings.nullToEmpty(table.getDbName()).toLowerCase(Locale.ROOT);
                String tb = Strings.nullToEmpty(table.getTbName()).toLowerCase(Locale.ROOT);
                if (Strings.isNullOrEmpty(db) || Strings.isNullOrEmpty(tb)) {
                    continue;
                }
                Set<String> cols = coveredColumns.get(db + "." + tb);
                if (cols != null && !cols.isEmpty()) {
                    return true;
                }
            }
        } catch (Exception e) {
            LOG.debug("Failed to evaluate policy coverage for root tables", e);
        }
        return false;
    }

    private boolean isSingleTableRootPlan(MaskingContext context) {
        if (context == null || context.rootPlan == null || context.ctx == null) {
            return false;
        }
        try {
            List<SandboxApi.TableDescription> tables = collectTableDescriptionsFromPlan(
                    context.ctx, context.rootPlan, "", "", 0);
            return tables.size() == 1;
        } catch (Exception e) {
            LOG.debug("Failed to determine single-table root plan for masking fallback", e);
            return false;
        }
    }

    private MinMax findColumnMinMax(SlotReference slot, MaskingContext context) {
        if (slot == null || context == null || context.ctx == null) {
            return new MinMax(null, null);
        }
        String columnName = slot.getName();
        if (Strings.isNullOrEmpty(columnName)) {
            return new MinMax(null, null);
        }
        Optional<TableIf> table = resolveTableFromSlot(slot, context);
        if (!table.isPresent()) {
            return new MinMax(null, null);
        }
        Column column = table.get().getColumn(columnName);
        String lookupColumnName = column != null ? column.getName() : columnName;
        Optional<ColumnStatistic> stat = table.get().getColumnStatistic(lookupColumnName);
        if (!stat.isPresent() || stat.get().isUnKnown) {
            return new MinMax(null, null);
        }
        ColumnStatistic stats = stat.get();
        String min = stats.minExpr != null ? stats.minExpr.getStringValue() : null;
        String max = stats.maxExpr != null ? stats.maxExpr.getStringValue() : null;
        if (min == null && !Double.isNaN(stats.minValue) && !Double.isInfinite(stats.minValue)) {
            min = Double.toString(stats.minValue);
        }
        if (max == null && !Double.isNaN(stats.maxValue) && !Double.isInfinite(stats.maxValue)) {
            max = Double.toString(stats.maxValue);
        }
        return new MinMax(min, max);
    }

    @SuppressWarnings("unchecked")
    private Optional<TableIf> resolveTableFromSlot(SlotReference slot, MaskingContext context) {
        List<String> qualifier = slot.getQualifier();
        String tableName = qualifier != null && !qualifier.isEmpty()
                ? qualifier.get(qualifier.size() - 1)
                : null;
        String dbName = qualifier != null && qualifier.size() > 1
                ? qualifier.get(qualifier.size() - 2)
                : context.ctx.getDatabase();
        if (Strings.isNullOrEmpty(tableName) || Strings.isNullOrEmpty(dbName)) {
            return Optional.empty();
        }
        dbName = ClusterNamespace.getNameFromFullName(dbName);
        try {
            Optional<? extends DatabaseIf> db = context.ctx.getCurrentCatalog().getDb(dbName);
            if (!db.isPresent()) {
                return Optional.empty();
            }
            return (Optional<TableIf>) (Optional<?>) db.get().getTable(tableName);
        } catch (Exception e) {
            LOG.debug("Failed to resolve table for slot {}.{}", dbName, tableName, e);
            return Optional.empty();
        }
    }

    private boolean shouldMaskLineageKeys(Set<String> lineageKeys, String policyJson) {
        if (lineageKeys == null || lineageKeys.isEmpty()) {
            return false;
        }
        Map<String, Set<String>> coveredColumns = parsePolicyCoveredColumns(policyJson);
        for (String lineageKey : lineageKeys) {
            String[] parts = lineageKey.split("\\.", 3);
            if (parts.length != 3) {
                return false;
            }
            String tableKey = parts[0] + "." + parts[1];
            String columnName = parts[2];
            Set<String> columns = coveredColumns.get(tableKey);
            if (columns == null || !columns.contains(columnName)) {
                return false;
            }
        }
        return true;
    }

    private Map<String, Set<String>> parsePolicyCoveredColumns(String policyJson) {
        Map<String, Set<String>> coveredColumns = new HashMap<>();
        if (Strings.isNullOrEmpty(policyJson)) {
            return coveredColumns;
        }
        try {
            JSONArray policies = JSONArray.parseArray(policyJson);
            if (policies == null) {
                return coveredColumns;
            }
            for (int i = 0; i < policies.size(); i++) {
                JSONObject policy = policies.getJSONObject(i);
                if (policy == null) {
                    continue;
                }
                String dbName = Strings.nullToEmpty(policy.getString("dbName")).toLowerCase(Locale.ROOT);
                String tbName = Strings.nullToEmpty(policy.getString("tbName")).toLowerCase(Locale.ROOT);
                if (Strings.isNullOrEmpty(dbName) || Strings.isNullOrEmpty(tbName)) {
                    continue;
                }
                Set<String> columns = coveredColumns.computeIfAbsent(dbName + "." + tbName, k -> new HashSet<>());
                JSONArray maskSettings = policy.getJSONArray("maskSetting");
                if (maskSettings == null) {
                    continue;
                }
                for (int j = 0; j < maskSettings.size(); j++) {
                    JSONObject setting = maskSettings.getJSONObject(j);
                    if (setting == null) {
                        continue;
                    }
                    String colName = Strings.nullToEmpty(setting.getString("colName")).toLowerCase(Locale.ROOT);
                    if (!Strings.isNullOrEmpty(colName)) {
                        columns.add(colName);
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to parse masking policy json for coverage check", e);
        }
        return coveredColumns;
    }

    private String buildLineageKey(TableIf table, Column column, String fallbackColumnName,
            List<String> qualifier) {
        String tableName = null;
        String dbName = null;
        if (table != null) {
            tableName = table.getName();
            DatabaseIf<?> db = table.getDatabase();
            if (db != null) {
                dbName = ClusterNamespace.getNameFromFullName(db.getFullName());
            }
        }
        if (Strings.isNullOrEmpty(tableName) && qualifier != null && !qualifier.isEmpty()) {
            tableName = qualifier.get(qualifier.size() - 1);
        }
        if (Strings.isNullOrEmpty(dbName) && qualifier != null && qualifier.size() > 1) {
            dbName = qualifier.get(qualifier.size() - 2);
        }
        String columnName = column != null ? column.getName() : fallbackColumnName;
        if (Strings.isNullOrEmpty(dbName) || Strings.isNullOrEmpty(tableName) || Strings.isNullOrEmpty(columnName)) {
            return null;
        }
        return dbName + "." + tableName + "." + columnName;
    }

    private class MinMax {
        private final String min;
        private final String max;

        MinMax(String min, String max) {
            this.min = min;
            this.max = max;
        }
    }
}
