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

package org.apache.doris.nereids.rules.expression.rewrite.rules;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprId;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.VectorizedUtil;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.rules.expression.rewrite.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Between;
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.ExpressionEvaluator;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Constant evaluation of an expression.
 */
public class FoldConstantRuleOnFE extends AbstractExpressionRewriteRule {
    public static final FoldConstantRuleOnFE INSTANCE = new FoldConstantRuleOnFE();
    private static final Logger LOG = LogManager.getLogger(FoldConstantRuleOnFE.class);
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private final IdGenerator<ExprId> idGenerator = ExprId.createGenerator();

    @Override
    public Expression rewrite(Expression expr, ExpressionRewriteContext ctx) {
        return visit(expr, ctx);
    }

    @Override
    public Expression visit(Expression expr, ExpressionRewriteContext ctx) {
        if (expr instanceof PropagateNullable) {
            List<Expression> children = expr.children()
                    .stream()
                    .map(child -> visit(child, ctx))
                    .collect(Collectors.toList());

            if (ExpressionUtils.hasNullLiteral(children)) {
                return Literal.of(null);
            }

            if (!ExpressionUtils.isAllLiteral(children)) {
                return expr.withChildren(children);
            }
            return expr.withChildren(children).accept(this, ctx);
        } else {
            return expr.accept(this, ctx);
        }
    }

    @Override
    public Expression visitEqualTo(EqualTo equalTo, ExpressionRewriteContext context) {
        return BooleanLiteral.of(((Literal) equalTo.left()).compareTo((Literal) equalTo.right()) == 0);
    }

    @Override
    public Expression visitGreaterThan(GreaterThan greaterThan, ExpressionRewriteContext context) {
        return BooleanLiteral.of(((Literal) greaterThan.left()).compareTo((Literal) greaterThan.right()) > 0);
    }

    @Override
    public Expression visitGreaterThanEqual(GreaterThanEqual greaterThanEqual, ExpressionRewriteContext context) {
        return BooleanLiteral.of(((Literal) greaterThanEqual.left())
                .compareTo((Literal) greaterThanEqual.right()) >= 0);
    }

    @Override
    public Expression visitLessThan(LessThan lessThan, ExpressionRewriteContext context) {
        return BooleanLiteral.of(((Literal) lessThan.left()).compareTo((Literal) lessThan.right()) < 0);

    }

    @Override
    public Expression visitLessThanEqual(LessThanEqual lessThanEqual, ExpressionRewriteContext context) {
        return BooleanLiteral.of(((Literal) lessThanEqual.left()).compareTo((Literal) lessThanEqual.right()) <= 0);
    }

    @Override
    public Expression visitNullSafeEqual(NullSafeEqual nullSafeEqual, ExpressionRewriteContext context) {
        Expression left = visit(nullSafeEqual.left(), context);
        Expression right = visit(nullSafeEqual.right(), context);
        if (ExpressionUtils.isAllLiteral(left, right)) {
            Literal l = (Literal) left;
            Literal r = (Literal) right;
            if (l.isNullLiteral() && r.isNullLiteral()) {
                return BooleanLiteral.TRUE;
            } else if (!l.isNullLiteral() && !r.isNullLiteral()) {
                return BooleanLiteral.of(l.compareTo(r) == 0);
            } else {
                return BooleanLiteral.FALSE;
            }
        }
        return nullSafeEqual.withChildren(left, right);
    }

    @Override
    public Expression visitNot(Not not, ExpressionRewriteContext context) {
        return BooleanLiteral.of(!((BooleanLiteral) not.child()).getValue());
    }

    @Override
    public Expression visitSlot(Slot slot, ExpressionRewriteContext context) {
        return slot;
    }

    @Override
    public Expression visitLiteral(Literal literal, ExpressionRewriteContext context) {
        return literal;
    }

    @Override
    public Expression visitAnd(And and, ExpressionRewriteContext context) {
        List<Expression> children = Lists.newArrayList();
        for (Expression child : and.children()) {
            Expression newChild = visit(child, context);
            if (newChild.equals(BooleanLiteral.FALSE)) {
                return BooleanLiteral.FALSE;
            }
            if (!newChild.equals(BooleanLiteral.TRUE)) {
                children.add(newChild);
            }
        }
        if (children.isEmpty()) {
            return BooleanLiteral.TRUE;
        }
        if (children.size() == 1) {
            return children.get(0);
        }
        if (ExpressionUtils.allNullLiteral(children)) {
            return Literal.of(null);
        }
        return and.withChildren(children);
    }

    @Override
    public Expression visitOr(Or or, ExpressionRewriteContext context) {
        List<Expression> children = Lists.newArrayList();
        for (Expression child : or.children()) {
            Expression newChild = visit(child, context);
            if (newChild.equals(BooleanLiteral.TRUE)) {
                return BooleanLiteral.TRUE;
            }
            if (!newChild.equals(BooleanLiteral.FALSE)) {
                children.add(newChild);
            }
        }
        if (children.isEmpty()) {
            return BooleanLiteral.FALSE;
        }
        if (children.size() == 1) {
            return children.get(0);
        }
        if (ExpressionUtils.allNullLiteral(children)) {
            return Literal.of(null);
        }
        return or.withChildren(children);
    }

    @Override
    public Expression visitLike(Like like, ExpressionRewriteContext context) {
        return like;
    }

    @Override
    public Expression visitCast(Cast cast, ExpressionRewriteContext context) {
        Expression child = visit(cast.child(), context);
        // todo: process other null case
        if (child.isNullLiteral()) {
            return Literal.of(null);
        }
        if (child.isLiteral()) {
            return child.castTo(cast.getDataType());
        }
        return cast.withChildren(child);
    }

    @Override
    public Expression visitBoundFunction(BoundFunction boundFunction, ExpressionRewriteContext context) {
        List<Expression> newArgs = boundFunction.getArguments().stream().map(arg -> visit(arg, context))
                .collect(Collectors.toList());
        if (ExpressionUtils.isAllLiteral(newArgs)) {
            return ExpressionEvaluator.INSTANCE.eval(boundFunction.withChildren(newArgs));
        }
        return boundFunction.withChildren(newArgs);
    }

    @Override
    public Expression visitBinaryArithmetic(BinaryArithmetic binaryArithmetic, ExpressionRewriteContext context) {
        return ExpressionEvaluator.INSTANCE.eval(binaryArithmetic);
    }

    @Override
    public Expression visitCaseWhen(CaseWhen caseWhen, ExpressionRewriteContext context) {
        Expression newDefault = null;
        boolean foundNewDefault = false;

        List<WhenClause> whenClauses = new ArrayList<>();
        for (WhenClause whenClause : caseWhen.getWhenClauses()) {
            Expression whenOperand = visit(whenClause.getOperand(), context);

            if (!(whenOperand.isLiteral())) {
                whenClauses.add(new WhenClause(whenOperand, visit(whenClause.getResult(), context)));
            } else if (BooleanLiteral.TRUE.equals(whenOperand)) {
                foundNewDefault = true;
                newDefault = visit(whenClause.getResult(), context);
                break;
            }
        }

        Expression defaultResult;
        if (foundNewDefault) {
            defaultResult = newDefault;
        } else {
            defaultResult = visit(caseWhen.getDefaultValue().orElse(Literal.of(null)), context);
        }

        if (whenClauses.isEmpty()) {
            return defaultResult;
        }
        return new CaseWhen(whenClauses, defaultResult);
    }

    @Override
    public Expression visitInPredicate(InPredicate inPredicate, ExpressionRewriteContext context) {
        Expression value = visit(inPredicate.child(0), context);
        List<Expression> children = Lists.newArrayList();
        children.add(value);
        if (value.isNullLiteral()) {
            return Literal.of(null);
        }
        boolean hasNull = false;
        boolean hasUnresolvedValue = !value.isLiteral();
        for (int i = 1; i < inPredicate.children().size(); i++) {
            Expression inValue = visit(inPredicate.child(i), context);
            children.add(inValue);
            if (!inValue.isLiteral()) {
                hasUnresolvedValue = true;
            }
            if (inValue.isNullLiteral()) {
                hasNull = true;
            }
            if (inValue.isLiteral() && value.isLiteral() && ((Literal) value).compareTo((Literal) inValue) == 0) {
                return Literal.of(true);
            }
        }
        if (hasUnresolvedValue) {
            return inPredicate.withChildren(children);
        }
        return hasNull ? Literal.of(null) : Literal.of(false);
    }

    @Override
    public Expression visitIsNull(IsNull isNull, ExpressionRewriteContext context) {
        Expression child = visit(isNull.child(), context);
        if (child.isNullLiteral()) {
            return Literal.of(true);
        } else if (!child.nullable()) {
            return Literal.of(false);
        }
        return isNull.withChildren(child);
    }

    @Override
    public Expression visitTimestampArithmetic(TimestampArithmetic arithmetic, ExpressionRewriteContext context) {
        return ExpressionEvaluator.INSTANCE.eval(arithmetic);
    }

    private Expression foldByBe(Expression root, ExpressionRewriteContext context) {
        Map<String, Expression> constMap = Maps.newHashMap();
        Map<String, TExpr> staleConstTExprMap = Maps.newHashMap();
        collectConst(root, constMap, staleConstTExprMap);
        if (constMap.isEmpty()) {
            return root;
        }
        Map<String, Map<String, TExpr>> paramMap = new HashMap<>();
        paramMap.put("0", staleConstTExprMap);
        Map<String, Expression> resultMap = evalOnBe(paramMap, constMap, context.connectContext);
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
            if (expr instanceof Cast) {
                if (((Cast) expr).child().isNullLiteral()) {
                    return;
                }
            }
            if (expr.isLiteral()) {
                return;
            }
            if (expr instanceof Between) {
                return;
            }
            String id = idGenerator.getNextId().toString();
            constMap.put(id, expr);
            Expr staleExpr = ExpressionTranslator.translate(expr, null);
            tExprMap.put(id, staleExpr.treeToThrift());
        } else {
            for (int i = 0; i < expr.children().size(); i++) {
                final Expression child = expr.children().get(i);
                collectConst(child, constMap, tExprMap);
            }
        }
    }

    private Map<String, Expression> evalOnBe(Map<String, Map<String, TExpr>> paramMap,
            Map<String, Expression> constMap, ConnectContext context) {

        Map<String, Expression> resultMap = new HashMap<>();
        try {
            List<Long> backendIds = Env.getCurrentSystemInfo().getBackendIds(true);
            if (backendIds.isEmpty()) {
                throw new LoadException("No alive backends");
            }
            Collections.shuffle(backendIds);
            Backend be = Env.getCurrentSystemInfo().getBackend(backendIds.get(0));
            TNetworkAddress brpcAddress = new TNetworkAddress(be.getHost(), be.getBrpcPort());

            TQueryGlobals queryGlobals = new TQueryGlobals();
            queryGlobals.setNowString(DATE_FORMAT.format(LocalDateTime.now()));
            queryGlobals.setTimestampMs(System.currentTimeMillis());
            queryGlobals.setTimeZone(TimeUtils.DEFAULT_TIME_ZONE);
            if (context.getSessionVariable().getTimeZone().equals("CST")) {
                queryGlobals.setTimeZone(TimeUtils.DEFAULT_TIME_ZONE);
            } else {
                queryGlobals.setTimeZone(context.getSessionVariable().getTimeZone());
            }

            TFoldConstantParams tParams = new TFoldConstantParams(paramMap, queryGlobals);
            tParams.setVecExec(VectorizedUtil.isVectorized());

            Future<PConstantExprResult> future =
                    BackendServiceProxy.getInstance().foldConstantExpr(brpcAddress, tParams);
            InternalService.PConstantExprResult result = future.get(5, TimeUnit.SECONDS);

            if (result.getStatus().getStatusCode() == 0) {
                for (Map.Entry<String, InternalService.PExprResultMap> e : result.getExprResultMapMap().entrySet()) {
                    for (Map.Entry<String, InternalService.PExprResult> e1 : e.getValue().getMapMap().entrySet()) {
                        Expression ret;
                        if (e1.getValue().getSuccess()) {
                            TPrimitiveType type = TPrimitiveType.findByValue(e1.getValue().getType().getType());
                            Type t = Type.fromPrimitiveType(PrimitiveType.fromThrift(Objects.requireNonNull(type)));
                            Expr staleExpr = LiteralExpr.create(e1.getValue().getContent(), Objects.requireNonNull(t));
                            // Nereids type
                            DataType t1 = DataType.convertFromString(staleExpr.getType().getPrimitiveType().toString());
                            ret = Literal.of(staleExpr.getStringValue()).castTo(t1);
                        } else {
                            ret = constMap.get(e.getKey());
                        }
                        resultMap.put(e.getKey(), ret);
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

