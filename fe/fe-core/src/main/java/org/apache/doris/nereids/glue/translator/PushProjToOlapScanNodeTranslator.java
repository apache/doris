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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.ArrayLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.analysis.FunctionParams;
import org.apache.doris.analysis.IndexDef;
import org.apache.doris.analysis.MapLiteral;
import org.apache.doris.analysis.MatchPredicate;
import org.apache.doris.analysis.MatchPredicate.Operator;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.VectorIndexUtil;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.NotAllowedFallbackException;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ApproxCosineSimilarity;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ApproxInnerProduct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ApproxL2Distance;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnary;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.MutableState;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Push down Nereids' {@link PhysicalProject} to the executable physical expression {@link OlapScanNode}.
 *
 * TODO: Add a set of transformation rules for OlapScanNode pushdown optimization to make the push-down
 *       process clearer.
 *       - Add PushFilterToOlapScan
 *       - Add PushTopNToOlapScan
 *       - Add PushProjToOlapScan
 *       - Improve LogicalOlapScanToPhysicalOlapScan
 */
public class PushProjToOlapScanNodeTranslator {

    private static final Map<String, PushdownFuncRewriter> REGISTERED_PUSHDOWN_FUNCTIONS = ImmutableMap.of(
            ApproxCosineSimilarityRewriter.NAME, new ApproxCosineSimilarityRewriter(),
            ApproxL2DistanceRewriter.NAME, new ApproxL2DistanceRewriter(),
            ApproxInnerProductRewriter.NAME, new ApproxInnerProductRewriter(),
            BM25PDFuncRewriter.NAME, new BM25PDFuncRewriter()
    );

    /**
     * Projection pushdown optimization.
     *
     * Pushes specific functions from the project to OlapScanNode, such as BM25().
     *
     * @return Virtual projection columns, or empty if there are no specific functions.
     */
    public static List<SlotRef> rewriteSpecificProjs(
            PhysicalProject<? extends Plan> proj, OlapScanNode olapScanNode, PlanTranslatorContext ctx) {
        final VirtualColAllocator virtualColAllocator = new VirtualColAllocator(olapScanNode, ctx);

        if (olapScanNode.getProjectList() == null || olapScanNode.getProjectList().isEmpty()) {
            return Collections.emptyList();
        }

        for (Expr expr : olapScanNode.getProjectList()) {
            expr.visitIfMatch(
                    FunctionCallExpr.class::isInstance,
                    (FunctionCallExpr func) -> rewriteFunc(func, proj, olapScanNode, ctx, virtualColAllocator)
            );
        }

        if (!virtualColAllocator.isEmpty()) {
            return virtualColAllocator.vSlotRefs;
        } else {
            return Collections.emptyList();
        }
    }

    /**
     * Projection Pushdown Optimization.
     *
     * Pushes specific functions from the filter to OlapScanNode, such as BM25().
     *
     * @return Virtual projection columns, or an empty list if there are no specific functions.
     */
    public static List<SlotRef> rewriteSpecificProjsInFilter(
            PhysicalFilter<? extends Plan> filter, OlapScanNode olapScanNode, PlanTranslatorContext ctx) {
        final VirtualColAllocator virtualColAllocator = new VirtualColAllocator(olapScanNode, ctx);

        if (olapScanNode.getConjuncts() == null || olapScanNode.getConjuncts().isEmpty()) {
            return Collections.emptyList();
        }

        for (Expr expr : olapScanNode.getConjuncts()) {
            expr.visitIfMatch(
                    FunctionCallExpr.class::isInstance,
                    (FunctionCallExpr func) -> rewriteFunc(func, filter, olapScanNode, ctx, virtualColAllocator)
            );
        }

        if (!virtualColAllocator.isEmpty()) {
            return virtualColAllocator.vSlotRefs;
        } else {
            return Collections.emptyList();
        }
    }

    private static void rewriteFunc(FunctionCallExpr func, PhysicalUnary<? extends Plan> node,
            OlapScanNode olapScanNode, PlanTranslatorContext ctx, VirtualColAllocator virtualColAllocator) {

        PushdownFuncRewriter pdFuncRewriter = matchPdFuncRewriter(func, node, olapScanNode, ctx);
        if (pdFuncRewriter == null) {
            return;
        }

        List<Expr> newArgs = pdFuncRewriter.rewriteParams(func, ctx, virtualColAllocator);
        FunctionParams newFnParams = func.getParams().clone(newArgs);
        Function newFn = func.getFn()
                .clone(newArgs.stream().map(Expr::getType).collect(Collectors.toList()));

        func.setFn(newFn);
        func.setFnParams(newFnParams);
        func.clearChildren();
        func.addChildren(newArgs);
    }

    public static void extractSlots(FunctionCallExpr func, PhysicalUnary<? extends Plan> node,
            OlapScanNode olapScanNode, PlanTranslatorContext ctx, BooleanHolder matchPushProjExtract) {

        if (null != matchPdFuncRewriter(func, node, olapScanNode, ctx)) {
            matchPushProjExtract.setValue(true);
        }
    }

    private static PushdownFuncRewriter matchPdFuncRewriter(FunctionCallExpr func, PhysicalUnary<? extends Plan> node,
            OlapScanNode olapScanNode, PlanTranslatorContext ctx) {
        PushdownFuncRewriter pdFuncRewriter = REGISTERED_PUSHDOWN_FUNCTIONS.get(func.getFnName().getFunction());

        if (pdFuncRewriter == null) {
            return null;
        }

        if (node instanceof PhysicalProject) {
            if (!pdFuncRewriter.matches(func, (PhysicalProject) node, olapScanNode, ctx)) {
                return null;
            }
        } else if (node instanceof PhysicalFilter) {
            if (!pdFuncRewriter.matches(func, (PhysicalFilter) node, olapScanNode, ctx)) {
                return null;
            }
        } else {
            return null;
        }

        return pdFuncRewriter;
    }

    ///////////////////////////////////////////////
    //      Pushdown function definitions        //
    ///////////////////////////////////////////////

    static class VirtualColAllocator {
        private List<SlotRef> vSlotRefs;
        private OlapScanNode olapScanNode;
        private PlanTranslatorContext ctx;

        public VirtualColAllocator(OlapScanNode olapScanNode, PlanTranslatorContext ctx) {
            Preconditions.checkNotNull(olapScanNode, "olapScanNode is null");
            Preconditions.checkNotNull(ctx, "ctx is null");

            this.olapScanNode = olapScanNode;
            this.ctx = ctx;
        }

        /**
         * Generates a virtual column as an input argument for a function. The results of the virtual columns will
         * be computed in the underlying storage layer.
         *
         * @return A SlotRef referring to a virtual column in the table.
         */
        private SlotRef allocateVirtualCol(FunctionName fnName, Type type, boolean nullable) {
            if (vSlotRefs == null) {
                vSlotRefs = new ArrayList<>();
            }

            TupleDescriptor tupleDescriptor = olapScanNode.getTupleDesc();
            SlotDescriptor desc = ctx.addSlotDesc(tupleDescriptor);
            String vColName =
                    ExpressionUtils.VIRTUAL_PROJ_COL_PREFIX + fnName.getFunction() + "_" + desc.getId().asInt();

            desc.setIsMaterialized(true);
            desc.setColumn(new Column(vColName, type, nullable));
            desc.setLabel(vColName);

            SlotRef vSlot = new SlotRef(desc);
            vSlot.setTblName(new TableName(olapScanNode.getOlapTable().getQualifiedName()));

            vSlotRefs.add(vSlot);
            return vSlot;
        }

        private boolean isEmpty() {
            return vSlotRefs == null || vSlotRefs.isEmpty();
        }
    }

    interface PushdownFuncRewriter {
        String name();

        List<Expr> rewriteParams(
                FunctionCallExpr func,
                PlanTranslatorContext ctx,
                VirtualColAllocator virtualColAllocator);

        boolean matches(
                FunctionCallExpr func,
                PhysicalProject<? extends Plan> proj,
                OlapScanNode olapScanNode,
                PlanTranslatorContext ctx);

        boolean matches(
                FunctionCallExpr func,
                PhysicalFilter<? extends Plan> filter,
                OlapScanNode olapScanNode,
                PlanTranslatorContext ctx);
    }

    /**
     * BM25 function to compute a score that measures text similarity between a query and documents.
     * Its computation relies on full-text index statistics from the storage layer.
     */
    static class BM25PDFuncRewriter implements PushdownFuncRewriter {
        public static final String NAME = "bm25";
        private static final Pattern MATCH_PHRASE_WITH_SLOP_PATTERN = Pattern.compile(".*~\\d+$");

        @Override
        public String name() {
            return NAME;
        }

        @Override
        public List<Expr> rewriteParams(FunctionCallExpr func, PlanTranslatorContext ctx,
                VirtualColAllocator virtualColAllocator) {
            List<Expr> newArgs = new ArrayList<>(func.getParams().exprs().size() + 1);
            newArgs.addAll(func.getParams().exprs());
            newArgs.add(virtualColAllocator.allocateVirtualCol(func.getFnName(), Type.FLOAT, true));
            return newArgs;
        }

        @Override
        public boolean matches(FunctionCallExpr func, PhysicalProject<? extends Plan> proj, OlapScanNode olapScanNode,
                PlanTranslatorContext ctx) {
            return containsMatchPredicate(olapScanNode);
        }

        @Override
        public boolean matches(FunctionCallExpr func, PhysicalFilter<? extends Plan> filter, OlapScanNode olapScanNode,
                PlanTranslatorContext ctx) {
            return containsMatchPredicate(olapScanNode);
        }

        public static boolean containsMatchPredicate(OlapScanNode olapScanNode) {
            // At least one full-text matching predicate.
            for (Expr expr : olapScanNode.getConjuncts()) {
                if (expr.contains((e) -> {
                    if (e instanceof MatchPredicate) {
                        MatchPredicate mp = (MatchPredicate) e;
                        if (Operator.MATCH_PHRASE_PREFIX.equals(mp.getOp())) {
                            throw new NotAllowedFallbackException(
                                    "The BM25 function is not supported for MATCH_PHRASE_PREFIX");
                        } else if (Operator.MATCH_PHRASE_EDGE.equals(mp.getOp())) {
                            throw new NotAllowedFallbackException(
                                    "The BM25 function is not supported for MATCH_PHRASE_EDGE");
                        }

                        boolean containsValidMatchPredicate = Operator.MATCH_ANY.equals(mp.getOp())
                                || Operator.MATCH_ALL.equals(mp.getOp())
                                || Operator.MATCH_PHRASE.equals(mp.getOp());
                        if (containsValidMatchPredicate) {
                            if (e.getChildren().size() == 2 && MATCH_PHRASE_WITH_SLOP_PATTERN.matcher(
                                            e.getChild(1).getStringValue()).matches()) {
                                throw new NotAllowedFallbackException(
                                        "The BM25 function is not supported for MATCH_PHRASE "
                                                + "with slop in unordered mode.");
                            }
                            return true;
                        }
                    }
                    return false;
                })) {
                    return true;
                }
            }

            throw new NotAllowedFallbackException(
                    "The BM25 function must be used with full-text matching predicates.");
        }
    }

    /**
     * approx distance function compute two vector index similarity or distance
     * it will be pushed down only when topN or filter push down vector index
     */
    abstract static class ApproxDistanceFuncRewriter implements PushdownFuncRewriter {
        @Override
        public List<Expr> rewriteParams(FunctionCallExpr func, PlanTranslatorContext ctx,
                VirtualColAllocator virtualColAllocator) {
            List<Expr> newArgs = new ArrayList<>(func.getParams().exprs().size() + 1);
            // In some cases, it is necessary to read the vector columns.
            boolean needVectorColumn = false;
            List<Expr> funcExprs = func.getParams().exprs();
            for (Expr e : funcExprs) {
                if (!(e instanceof SlotRef)) {
                    continue;
                }
                // pq index need reed vector columns
                SlotRef slot = (SlotRef) e;
                if (slot.getTable() instanceof OlapTable) {
                    for (Index idx : ((OlapTable) ((SlotRef) e).getTable()).getIndexes()) {
                        if (idx.getIndexType() != IndexDef.IndexType.VECTOR) {
                            continue;
                        }
                        if (!StringUtils.equalsAny(idx.getProperties().get(VectorIndexUtil.VECTOR_INDEX_INDEX_TYPE_KEY),
                                "hnswpq", "ivfpq")) {
                            continue;
                        }
                        if (idx.getColumns().contains(slot.getColumnName())) {
                            needVectorColumn = true;
                            break;
                        }
                    }
                }
                if (needVectorColumn) {
                    break;
                }
            }
            // Do not read unnecessary vector column.
            if (ctx.getSessionVariable() == null || ctx.getSessionVariable().isEnableScanVectorColumn()
                    || needVectorColumn) {
                newArgs.addAll(func.getParams().exprs());
            }
            newArgs.add(virtualColAllocator.allocateVirtualCol(func.getFnName(), Type.DOUBLE, true));
            return newArgs;
        }

        @Override
        public boolean matches(FunctionCallExpr func, PhysicalProject<? extends Plan> project,
                               OlapScanNode olapScanNode, PlanTranslatorContext ctx) {
            // project will be pushed down, only when topN or filter push down vector index
            // 1. check func itself
            SlotRef funcIndexVector;
            ArrayLiteral funcConstVector = null;
            MapLiteral funcConstSparseVector = null;
            if (func.getChild(0) instanceof ArrayLiteral && func.getChild(1) instanceof SlotRef) {
                funcIndexVector = (SlotRef) func.getChild(1);
                funcConstVector = (ArrayLiteral) func.getChild(0);
            } else if (func.getChild(0) instanceof SlotRef && func.getChild(1) instanceof ArrayLiteral) {
                funcIndexVector = (SlotRef) func.getChild(0);
                funcConstVector = (ArrayLiteral) func.getChild(1);
            } else if (func.getChild(0) instanceof MapLiteral && func.getChild(1) instanceof SlotRef) {
                funcIndexVector = (SlotRef) func.getChild(1);
                funcConstSparseVector = (MapLiteral) func.getChild(0);
            } else if (func.getChild(0) instanceof SlotRef && func.getChild(1) instanceof MapLiteral) {
                funcIndexVector = (SlotRef) func.getChild(0);
                funcConstSparseVector = (MapLiteral) func.getChild(1);
            } else {
                return false;
            }

            String funcIndexColumnName = funcIndexVector.getColumnName();
            List<Index> indexList = olapScanNode.getOlapTable().getIndexes();
            boolean funcPushDown = false;
            for (Index index : indexList) {
                if (index.getIndexType() == IndexDef.IndexType.VECTOR
                        && index.getColumns().contains(funcIndexColumnName)) {
                    //index.getProperties().get("metric_type");
                    funcPushDown = true;
                    break;
                }
            }
            if (!funcPushDown) {
                return false;
            }

            // push down to vector index:
            // 2. judge filter push down
            for (Expr expr : olapScanNode.getConjuncts()) {
                for (Expr child : expr.getChildren()) {
                    // 2.1 projection need be same with function in where filter
                    if (child instanceof FunctionCallExpr && child.equals(func)) {

                        // where filter function name
                        String fnName = child.getFn().functionName();
                        // TExprOpcode.LE TExprOpcode.GE
                        TExprOpcode opCode = expr.getOpcode();
                        // 2.2 push down when:
                        // a. approx_cosine_similarity/approx_inner_product && >=
                        // b. approx_l2_distance && <=
                        if ((fnName.equals(ApproxCosineSimilarity.name()) || fnName.equals(ApproxInnerProduct.name()))
                                && (opCode != TExprOpcode.GE && opCode != TExprOpcode.GT)) {
                            break;
                        } else if (fnName.equals(ApproxL2Distance.name())
                                && (opCode != TExprOpcode.LE && opCode != TExprOpcode.LT)) {
                            break;
                        }

                        // 2.3 params need one is const vector, another is vector index column
                        SlotRef indexVector;
                        if (child.getChild(0) instanceof ArrayLiteral && child.getChild(1) instanceof SlotRef) {
                            indexVector = (SlotRef) child.getChild(1);
                        } else if (child.getChild(0) instanceof SlotRef && child.getChild(1) instanceof ArrayLiteral) {
                            indexVector = (SlotRef) child.getChild(0);
                        } else if (child.getChild(0) instanceof MapLiteral && child.getChild(1) instanceof SlotRef) {
                            indexVector = (SlotRef) child.getChild(1);
                        } else if (child.getChild(0) instanceof SlotRef && child.getChild(1) instanceof MapLiteral) {
                            indexVector = (SlotRef) child.getChild(0);
                        } else {
                            break;
                        }

                        if (funcIndexColumnName.equals(indexVector.getColumnName())) {
                            return true;
                        }
                    }
                }
            }

            // 3. judge topn push down
            Optional<Object> optional = project.getMutableState(MutableState.KEY_PARENT);
            if (optional.isPresent() && optional.get() instanceof PhysicalTopN) {
                PhysicalTopN parent = (PhysicalTopN) optional.get();
                // 3.1 get topN orderkey
                List<OrderKey> orderKeys = parent.getOrderKeys();
                for (OrderKey orderKey : orderKeys) {
                    Expression expression = orderKey.getExpr();
                    if (expression instanceof SlotReference) {
                        // 3.2 find orderkey exprId
                        ExprId exprId = ((SlotReference) expression).getExprId();
                        String projFuncName = func.getFn().functionName();
                        boolean isAsc = orderKey.isAsc();
                        for (NamedExpression namedExpression : project.getProjects()) {
                            if (exprId == namedExpression.getExprId()) {
                                for (Expression child : namedExpression.children()) {
                                    String exprName = child.getExpressionName();
                                    if (!exprName.equals(projFuncName)) {
                                        break;
                                    }
                                    // 3.3 push down when:
                                    // a. order by approx_cosine_similarity/approx_inner_product desc
                                    // b. order by approx_l2_distance     asc
                                    if ((exprName.equals(ApproxCosineSimilarity.name())
                                            || exprName.equals(ApproxInnerProduct.name()))
                                            && isAsc) {
                                        break;
                                    }
                                    if (exprName.equals(ApproxL2Distance.name()) && !isAsc) {
                                        break;
                                    }

                                    // 3.4 params need one is const vector, another is vector index column
                                    SlotReference indexVector;
                                    org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral constVector = null;
                                    org.apache.doris.nereids.trees.expressions.literal.MapLiteral
                                            constSparseVector = null;
                                    if (child.child(0)
                                            instanceof org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral
                                            && child.child(1) instanceof SlotReference) {
                                        indexVector = (SlotReference) child.child(1);
                                        constVector = (org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral)
                                                        child.child(0);
                                    } else if (child.child(1)
                                            instanceof org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral
                                            && child.child(0) instanceof SlotReference) {
                                        indexVector = (SlotReference) child.child(0);
                                        constVector = (org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral)
                                                        child.child(1);
                                    } else if (child.child(0)
                                            instanceof org.apache.doris.nereids.trees.expressions.literal.MapLiteral
                                            && child.child(1) instanceof SlotReference) {
                                        indexVector = (SlotReference) child.child(1);
                                        constSparseVector = (org.apache.doris.nereids.trees.expressions.literal
                                            .MapLiteral) child.child(0);
                                    } else if (child.child(1)
                                            instanceof org.apache.doris.nereids.trees.expressions.literal.MapLiteral
                                            && child.child(0) instanceof SlotReference) {
                                        indexVector = (SlotReference) child.child(0);
                                        constSparseVector = (org.apache.doris.nereids.trees.expressions.literal
                                            .MapLiteral) child.child(1);
                                    } else {
                                        break;
                                    }

                                    if (indexVector == null || !indexVector.getColumn().isPresent()) {
                                        break;
                                    }

                                    if (funcIndexColumnName.equals(indexVector.getColumn().get().getName())) {
                                        if (funcConstVector != null && constVector != null
                                                && funcConstVector.equals(constVector.toLegacyLiteral())) {
                                            return true;
                                        }

                                        if (funcConstSparseVector != null && constSparseVector != null
                                                && funcConstSparseVector.equals(constSparseVector.toLegacyLiteral())) {
                                            return true;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            return false;
        }

        @Override
        public boolean matches(FunctionCallExpr func, PhysicalFilter<? extends Plan> filter,
                               OlapScanNode olapScanNode, PlanTranslatorContext ctx) {
            return false;
        }
    }

    static class ApproxL2DistanceRewriter extends ApproxDistanceFuncRewriter {
        private static final String NAME = "approx_l2_distance";

        @Override
        public String name() {
            return NAME;
        }
    }

    static class ApproxCosineSimilarityRewriter extends ApproxDistanceFuncRewriter {
        private static final String NAME = "approx_cosine_similarity";

        @Override
        public String name() {
            return NAME;
        }
    }

    static class ApproxInnerProductRewriter extends ApproxDistanceFuncRewriter {
        private static final String NAME = "approx_inner_product";

        @Override
        public String name() {
            return NAME;
        }
    }

    static class BooleanHolder {
        private boolean value;

        public BooleanHolder(boolean value) {
            this.value = value;
        }

        public boolean getValue() {
            return value;
        }

        public void setValue(boolean value) {
            this.value = value;
        }
    }
}
