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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprId;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.rules.expression.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PConstantExprResult;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TFoldConstantParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TQueryGlobals;
import org.apache.doris.thrift.TQueryOptions;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Constant evaluation of an expression.
 */
public class FoldConstantRuleOnBE extends AbstractExpressionRewriteRule {
    private static final Logger LOG = LogManager.getLogger(FoldConstantRuleOnBE.class);
    private final IdGenerator<ExprId> idGenerator = ExprId.createGenerator();

    @Override
    public Expression rewrite(Expression expr, ExpressionRewriteContext ctx) {
        Expression expression = FoldConstantRuleOnFE.INSTANCE.rewrite(expr, ctx);
        return foldByBE(expression, ctx);
    }

    private Expression foldByBE(Expression root, ExpressionRewriteContext context) {
        Map<String, Expression> constMap = Maps.newHashMap();
        Map<String, TExpr> staleConstTExprMap = Maps.newHashMap();
        Expression rootWithoutAlias = root;
        if (root instanceof Alias) {
            rootWithoutAlias = ((Alias) root).child();
        }
        collectConst(rootWithoutAlias, constMap, staleConstTExprMap);
        if (constMap.isEmpty()) {
            return root;
        }
        Map<String, Map<String, TExpr>> paramMap = new HashMap<>();
        paramMap.put("0", staleConstTExprMap);
        Map<String, Expression> resultMap = evalOnBE(paramMap, constMap, context.cascadesContext.getConnectContext());
        if (!resultMap.isEmpty()) {
            return replace(root, constMap, resultMap);
        }
        return root;
    }

    private Expression replace(Expression root, Map<String, Expression> constMap, Map<String, Expression> resultMap) {
        for (Entry<String, Expression> entry : constMap.entrySet()) {
            if (entry.getValue().equals(root)) {
                return resultMap.get(entry.getKey());
            }
        }
        List<Expression> newChildren = new ArrayList<>();
        boolean hasNewChildren = false;
        for (Expression child : root.children()) {
            Expression newChild = replace(child, constMap, resultMap);
            if (newChild != child) {
                hasNewChildren = true;
            }
            newChildren.add(newChild);
        }
        return hasNewChildren ? root.withChildren(newChildren) : root;
    }

    private void collectConst(Expression expr, Map<String, Expression> constMap, Map<String, TExpr> tExprMap) {
        if (expr.isConstant()) {
            // Do not constant fold cast(null as dataType) because we cannot preserve the
            // cast-to-types and that can lead to query failures, e.g., CTAS
            if (expr instanceof Cast) {
                if (((Cast) expr).child().isNullLiteral()) {
                    return;
                }
            }
            // skip literal expr
            if (expr.isLiteral()) {
                return;
            }
            String id = idGenerator.getNextId().toString();
            constMap.put(id, expr);
            Expr staleExpr;
            try {
                staleExpr = ExpressionTranslator.translate(expr, null);
            } catch (Exception e) {
                LOG.warn("expression {} translate to legacy expr failed. ", expr, e);
                return;
            }
            tExprMap.put(id, staleExpr.treeToThrift());
        } else {
            for (int i = 0; i < expr.children().size(); i++) {
                final Expression child = expr.children().get(i);
                collectConst(child, constMap, tExprMap);
            }
        }
    }

    private Map<String, Expression> evalOnBE(Map<String, Map<String, TExpr>> paramMap,
            Map<String, Expression> constMap, ConnectContext context) {

        Map<String, Expression> resultMap = new HashMap<>();
        try {
            List<Long> backendIds = Env.getCurrentSystemInfo().getAllBackendIds(true);
            if (backendIds.isEmpty()) {
                throw new UserException("No alive backends");
            }
            Collections.shuffle(backendIds);
            Backend be = Env.getCurrentSystemInfo().getBackend(backendIds.get(0));
            TNetworkAddress brpcAddress = new TNetworkAddress(be.getHost(), be.getBrpcPort());

            TQueryGlobals queryGlobals = new TQueryGlobals();
            queryGlobals.setNowString(TimeUtils.DATETIME_FORMAT.format(LocalDateTime.now()));
            queryGlobals.setTimestampMs(System.currentTimeMillis());
            queryGlobals.setTimeZone(TimeUtils.DEFAULT_TIME_ZONE);
            if (context.getSessionVariable().getTimeZone().equals("CST")) {
                queryGlobals.setTimeZone(TimeUtils.DEFAULT_TIME_ZONE);
            } else {
                queryGlobals.setTimeZone(context.getSessionVariable().getTimeZone());
            }

            TQueryOptions tQueryOptions = new TQueryOptions();
            tQueryOptions.setRepeatMaxNum(context.getSessionVariable().repeatMaxNum);

            TFoldConstantParams tParams = new TFoldConstantParams(paramMap, queryGlobals);
            tParams.setVecExec(true);
            tParams.setQueryOptions(tQueryOptions);
            tParams.setQueryId(context.queryId());

            Future<PConstantExprResult> future =
                    BackendServiceProxy.getInstance().foldConstantExpr(brpcAddress, tParams);
            PConstantExprResult result = future.get(5, TimeUnit.SECONDS);

            if (result.getStatus().getStatusCode() == 0) {
                for (Entry<String, InternalService.PExprResultMap> e : result.getExprResultMapMap().entrySet()) {
                    for (Entry<String, InternalService.PExprResult> e1 : e.getValue().getMapMap().entrySet()) {
                        Expression ret;
                        if (e1.getValue().getSuccess()) {
                            TPrimitiveType type = TPrimitiveType.findByValue(e1.getValue().getType().getType());
                            Type t = Type.fromPrimitiveType(PrimitiveType.fromThrift(Objects.requireNonNull(type)));
                            Expr staleExpr = LiteralExpr.create(e1.getValue().getContent(), Objects.requireNonNull(t));
                            // Nereids type
                            DataType t1 = DataType.convertFromString(staleExpr.getType().getPrimitiveType().toString());
                            ret = Literal.of(staleExpr.getStringValue()).castTo(t1);
                        } else {
                            ret = constMap.get(e1.getKey());
                        }
                        resultMap.put(e1.getKey(), ret);
                    }
                }

            } else {
                LOG.warn("failed to get const expr value from be: {}", result.getStatus().getErrorMsgsList());
            }
        } catch (Exception e) {
            LOG.warn("failed to get const expr value from be: {}", e.getMessage());
        }
        return resultMap;
    }
}

