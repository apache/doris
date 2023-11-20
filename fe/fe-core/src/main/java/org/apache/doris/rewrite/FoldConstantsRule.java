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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/FoldConstantsRule.java
// and modified by Doris

package org.apache.doris.rewrite;


import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BetweenPredicate;
import org.apache.doris.analysis.CaseExpr;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InformationFunction;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.VariableExpr;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types.PScalarType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TFoldConstantParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TQueryGlobals;
import org.apache.doris.thrift.TQueryOptions;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * This rule replaces a constant Expr with its equivalent LiteralExpr by evaluating the
 * Expr in the BE. Exprs that are already LiteralExprs are not changed.
 *
 * TODO: Expressions fed into this rule are currently not required to be analyzed
 * in order to support constant folding in expressions that contain unresolved
 * references to select-list aliases (such expressions cannot be analyzed).
 * The cross-dependencies between rule transformations and analysis are vague at the
 * moment and make rule application overly complicated.
 *
 * Examples:
 * 1 + 1 + 1 --> 3
 * toupper('abc') --> 'ABC'
 * cast('2016-11-09' as timestamp) --> TIMESTAMP '2016-11-09 00:00:00'
 */
public class FoldConstantsRule implements ExprRewriteRule {
    private static final Logger LOG = LogManager.getLogger(FoldConstantsRule.class);

    public static ExprRewriteRule INSTANCE = new FoldConstantsRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ExprRewriter.ClauseType clauseType) throws AnalysisException {
        // evaluate `case when expr` when possible
        if (expr instanceof CaseExpr) {
            return CaseExpr.computeCaseExpr((CaseExpr) expr);
        }

        // Avoid calling Expr.isConstant() because that would lead to repeated traversals
        // of the Expr tree. Assumes the bottom-up application of this rule. Constant
        // children should have been folded at this point.
        for (Expr child : expr.getChildren()) {
            if (!child.isLiteral() && !(child instanceof CastExpr)) {
                return expr;
            }
        }

        if (expr.isLiteral() || !expr.isConstant()) {
            return expr;
        }

        // Do not constant fold cast(null as dataType) because we cannot preserve the
        // cast-to-types and that can lead to query failures, e.g., CTAS
        if (expr instanceof CastExpr) {
            CastExpr castExpr = (CastExpr) expr;
            if (castExpr.isNotFold()) {
                return castExpr;
            }
            if (castExpr.getChild(0) instanceof NullLiteral) {
                return castExpr.getChild(0);
            }
        }

        // Analyze constant exprs, if necessary. Note that the 'expr' may become non-constant
        // after analysis (e.g., aggregate functions).
        if (!expr.isAnalyzed()) {
            expr.analyze(analyzer);
            if (!expr.isConstant()) {
                return expr;
            }
        }
        return expr.getResultValue(false);
    }

    /**
     * fold constant expr by BE
     * SysVariableDesc and InformationFunction need handling specially
     * @param exprMap
     * @param analyzer
     * @return
     * @throws AnalysisException
     */
    public boolean apply(Map<String, Expr> exprMap, Analyzer analyzer, boolean changed, TQueryOptions tQueryOptions)
            throws AnalysisException {
        // root_expr_id_string:
        //     child_expr_id_string : texpr
        //     child_expr_id_string : texpr
        Map<String, Map<String, TExpr>> paramMap = new HashMap<>();
        Map<String, Expr> allConstMap = new HashMap<>();
        // map to collect SysVariableDesc
        Map<String, Map<String, Expr>> sysVarsMap = new HashMap<>();
        // map to collect InformationFunction
        Map<String, Map<String, Expr>> infoFnsMap = new HashMap<>();
        for (Map.Entry<String, Expr> entry : exprMap.entrySet()) {
            Map<String, TExpr> constMap = new HashMap<>();
            Map<String, Expr> oriConstMap = new HashMap<>();
            Map<String, Expr> sysVarMap = new HashMap<>();
            Map<String, Expr> infoFnMap = new HashMap<>();
            getConstExpr(entry.getValue(), constMap, oriConstMap, analyzer, sysVarMap, infoFnMap);

            if (!constMap.isEmpty()) {
                paramMap.put(entry.getKey(), constMap);
                allConstMap.putAll(oriConstMap);
            }
            if (!sysVarMap.isEmpty()) {
                sysVarsMap.put(entry.getKey(), sysVarMap);
            }
            if (!infoFnMap.isEmpty()) {
                infoFnsMap.put(entry.getKey(), infoFnMap);
            }
        }

        if (!sysVarsMap.isEmpty()) {
            putBackConstExpr(exprMap, sysVarsMap);
            changed = true;
        }

        if (!infoFnsMap.isEmpty()) {
            putBackConstExpr(exprMap, infoFnsMap);
            changed = true;
        }

        if (!paramMap.isEmpty()) {
            Map<String, Map<String, Expr>> resultMap = calcConstExpr(paramMap, allConstMap, analyzer.getContext(),
                    tQueryOptions);

            if (!resultMap.isEmpty()) {
                putBackConstExpr(exprMap, resultMap);
                changed = true;

            }

        }
        return changed;
    }

    /**
     * get all constant children expr from a expr
     * @param expr
     * @param constExprMap
     * @param analyzer
     * @throws AnalysisException
     */
    // public only for unit test
    public void getConstExpr(Expr expr, Map<String, TExpr> constExprMap, Map<String, Expr> oriConstMap,
            Analyzer analyzer, Map<String, Expr> sysVarMap, Map<String, Expr> infoFnMap)
            throws AnalysisException {
        if (expr.isConstant()) {
            // Do not constant fold cast(null as dataType) because we cannot preserve the
            // cast-to-types and that can lead to query failures, e.g., CTAS
            if (expr instanceof CastExpr) {
                CastExpr castExpr = (CastExpr) expr;
                if (castExpr.getChild(0) instanceof NullLiteral) {
                    return;
                }
            }
            // skip literal expr
            if (expr instanceof LiteralExpr) {
                return;
            }
            // skip BetweenPredicate need to be rewrite to CompoundPredicate
            if (expr instanceof BetweenPredicate) {
                return;
            }
            // collect sysVariableDesc expr
            if (expr.contains(Predicates.instanceOf(VariableExpr.class))) {
                getSysVarDescExpr(expr, sysVarMap);
                return;
            }
            // collect InformationFunction
            if (expr.contains(Predicates.instanceOf(InformationFunction.class))) {
                getInfoFnExpr(expr, infoFnMap);
                return;
            }
            constExprMap.put(expr.getId().toString(), expr.treeToThrift());
            oriConstMap.put(expr.getId().toString(), expr);
        } else {
            recursiveGetChildrenConstExpr(expr, constExprMap, oriConstMap, analyzer, sysVarMap, infoFnMap);

        }
    }

    private void recursiveGetChildrenConstExpr(Expr expr, Map<String, TExpr> constExprMap,
            Map<String, Expr> oriConstMap, Analyzer analyzer, Map<String, Expr> sysVarMap, Map<String, Expr> infoFnMap)
            throws AnalysisException {
        for (int i = 0; i < expr.getChildren().size(); i++) {
            final Expr child = expr.getChildren().get(i);
            getConstExpr(child, constExprMap, oriConstMap, analyzer, sysVarMap, infoFnMap);
        }

    }

    private void getSysVarDescExpr(Expr expr, Map<String, Expr> sysVarMap) {
        if (expr instanceof VariableExpr) {
            Expr literalExpr = ((VariableExpr) expr).getLiteralExpr();
            if (literalExpr == null) {
                try {
                    VariableMgr.fillValue(ConnectContext.get().getSessionVariable(), (VariableExpr) expr);
                    literalExpr = ((VariableExpr) expr).getLiteralExpr();
                } catch (AnalysisException e) {
                    if (ConnectContext.get() != null) {
                        ConnectContext.get().getState().reset();
                    }
                    LOG.warn("failed to get session variable value: " + ((VariableExpr) expr).getName());
                }
            }
            sysVarMap.put(expr.getId().toString(), literalExpr);
        } else {
            for (Expr child : expr.getChildren()) {
                getSysVarDescExpr(child, sysVarMap);
            }
        }
    }

    private void getInfoFnExpr(Expr expr, Map<String, Expr> infoFnMap) {
        if (expr instanceof InformationFunction) {
            Type type = expr.getType();
            LiteralExpr literalExpr = null;
            try {
                String str = null;
                if (type.equals(Type.VARCHAR)) {
                    str = ((InformationFunction) expr).getStrValue();
                } else if (type.equals(Type.BIGINT)) {
                    str = ((InformationFunction) expr).getIntValue();
                }
                Preconditions.checkNotNull(str);
                literalExpr = LiteralExpr.create(str, type);
                infoFnMap.put(expr.getId().toString(), literalExpr);
            } catch (AnalysisException e) {
                if (ConnectContext.get() != null) {
                    ConnectContext.get().getState().reset();
                }
                LOG.warn("failed to get const expr value from InformationFunction: {}", e.getMessage());
            }

        } else {
            for (Expr child : expr.getChildren()) {
                getInfoFnExpr(child, infoFnMap);
            }
        }
    }

    /**
     * put all rewritten expr back to ori expr map
     * @param exprMap
     * @param resultMap
     */
    private void putBackConstExpr(Map<String, Expr> exprMap, Map<String, Map<String, Expr>> resultMap) {
        for (Map.Entry<String, Map<String, Expr>> entry : resultMap.entrySet()) {
            Expr rewrittenExpr = putBackConstExpr(exprMap.get(entry.getKey()), entry.getValue());
            exprMap.put(entry.getKey(), rewrittenExpr);
        }
    }

    private Expr putBackConstExpr(Expr expr, Map<String, Expr> resultMap) {
        for (Map.Entry<String, Expr> entry : resultMap.entrySet()) {
            if (entry.getValue() instanceof LiteralExpr) {
                expr = replaceExpr(expr, entry.getKey(), (LiteralExpr) entry.getValue());

            }
        }
        return expr;
    }

    /**
     * find and replace constant child expr of a expr by literal expr
     * @param expr
     * @param key
     * @param literalExpr
     * @return
     */
    private Expr replaceExpr(Expr expr, String key, LiteralExpr literalExpr) {
        if (expr.getId().toString().equals(key)) {
            return literalExpr;
        }
        // ATTN: make sure the child order of expr keep unchanged
        for (int i = 0; i < expr.getChildren().size(); i++) {
            Expr child = expr.getChild(i);
            if (!(child instanceof LiteralExpr) && literalExpr.equals(replaceExpr(child, key, literalExpr))) {
                literalExpr.setId(child.getId());
                expr.setChild(i, literalExpr);
                break;
            }
        }
        return expr;
    }

    /**
     * calc all constant exprs by BE
     * @param map
     * @param context
     * @return
     */
    private Map<String, Map<String, Expr>> calcConstExpr(Map<String, Map<String, TExpr>> map,
            Map<String, Expr> allConstMap,
            ConnectContext context, TQueryOptions tQueryOptions) {
        TNetworkAddress brpcAddress = null;
        Map<String, Map<String, Expr>> resultMap = new HashMap<>();
        try {
            List<Long> backendIds = Env.getCurrentSystemInfo().getAllBackendIds(true);
            if (backendIds.isEmpty()) {
                throw new LoadException("Failed to get all partitions. No alive backends");
            }
            Collections.shuffle(backendIds);
            Backend be = Env.getCurrentSystemInfo().getBackend(backendIds.get(0));
            brpcAddress = new TNetworkAddress(be.getHost(), be.getBrpcPort());

            TQueryGlobals queryGlobals = new TQueryGlobals();
            queryGlobals.setNowString(TimeUtils.DATETIME_FORMAT.format(LocalDateTime.now()));
            queryGlobals.setTimestampMs(System.currentTimeMillis());
            queryGlobals.setNanoSeconds(LocalDateTime.now().getNano());
            queryGlobals.setTimeZone(TimeUtils.DEFAULT_TIME_ZONE);
            if (context.getSessionVariable().getTimeZone().equals("CST")) {
                queryGlobals.setTimeZone(TimeUtils.DEFAULT_TIME_ZONE);
            } else {
                queryGlobals.setTimeZone(context.getSessionVariable().getTimeZone());
            }

            TFoldConstantParams tParams = new TFoldConstantParams(map, queryGlobals);
            tParams.setVecExec(true);
            tParams.setQueryOptions(tQueryOptions);
            tParams.setQueryId(context.queryId());

            Future<InternalService.PConstantExprResult> future
                    = BackendServiceProxy.getInstance().foldConstantExpr(brpcAddress, tParams);
            InternalService.PConstantExprResult result = future.get(5, TimeUnit.SECONDS);

            if (result.getStatus().getStatusCode() == 0) {
                for (Map.Entry<String, InternalService.PExprResultMap> entry
                        : result.getExprResultMapMap().entrySet()) {
                    Map<String, Expr> tmp = new HashMap<>();
                    for (Map.Entry<String, InternalService.PExprResult> entry1
                            : entry.getValue().getMapMap().entrySet()) {
                        PScalarType scalarType = entry1.getValue().getType();
                        TPrimitiveType ttype = TPrimitiveType.findByValue(scalarType.getType());
                        Expr retExpr = null;
                        if (entry1.getValue().getSuccess()) {
                            Type type = null;
                            if (ttype == TPrimitiveType.CHAR) {
                                Preconditions.checkState(scalarType.hasLen());
                                type = ScalarType.createCharType(scalarType.getLen());
                            } else if (ttype == TPrimitiveType.VARCHAR) {
                                Preconditions.checkState(scalarType.hasLen());
                                type = ScalarType.createVarcharType(scalarType.getLen());
                            } else if (ttype == TPrimitiveType.DECIMALV2) {
                                type = ScalarType.createDecimalType(scalarType.getPrecision(),
                                        scalarType.getScale());
                            } else if (ttype == TPrimitiveType.DATETIMEV2) {
                                type = ScalarType.createDatetimeV2Type(scalarType.getScale());
                            } else if (ttype == TPrimitiveType.DECIMAL32
                                    || ttype == TPrimitiveType.DECIMAL64
                                    || ttype == TPrimitiveType.DECIMAL128I
                                    || ttype == TPrimitiveType.DECIMAL256) {
                                type = ScalarType.createDecimalV3Type(scalarType.getPrecision(),
                                        scalarType.getScale());
                            } else {
                                type = ScalarType.createType(
                                        PrimitiveType.fromThrift(ttype));
                            }
                            retExpr = LiteralExpr.create(entry1.getValue().getContent(),
                                    type);
                        } else {
                            retExpr = allConstMap.get(entry1.getKey());
                        }
                        LOG.debug("retExpr: " + retExpr.toString());
                        tmp.put(entry1.getKey(), retExpr);
                    }
                    if (!tmp.isEmpty()) {
                        resultMap.put(entry.getKey(), tmp);
                    }
                }

            } else {
                LOG.warn("failed_fold_context.queryId(): " + DebugUtil.printId(context.queryId()));
                LOG.warn("failed to get const expr value from be: {}", result.getStatus().getErrorMsgsList());
            }
        } catch (Exception e) {
            LOG.warn("failed_fold_context.queryId(): " + DebugUtil.printId(context.queryId()));
            LOG.warn("failed to get const expr value from be: {}", e.getMessage());
        }
        return resultMap;
    }
}
