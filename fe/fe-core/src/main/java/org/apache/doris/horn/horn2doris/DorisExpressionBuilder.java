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

package org.apache.doris.horn.horn2doris;

import org.apache.horn4j.thrift.TArithmeticOperatorType;
import org.apache.horn4j.thrift.TLiteralValue;
import org.apache.horn4j.thrift.TOperator;
import org.apache.horn4j.thrift.TOperatorType;
import org.apache.horn4j.thrift.TScalar;
import org.apache.horn4j.thrift.TScalarAggFn;
import org.apache.horn4j.thrift.TScalarArithmetic;
import org.apache.horn4j.thrift.TScalarBinaryPredicate;
import org.apache.horn4j.thrift.TScalarColumnRef;
import org.apache.horn4j.thrift.TScalarAnalyticWindowBoundary;
import org.apache.horn4j.thrift.TScalarFnCall;
import org.apache.horn4j.thrift.TScalarLiteral;
import org.apache.horn4j.thrift.TScalarWindowFrame;
import org.apache.horn4j.thrift.TWindowType;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.BitAnd;
import org.apache.doris.nereids.trees.expressions.BitNot;
import org.apache.doris.nereids.trees.expressions.BitOr;
import org.apache.doris.nereids.trees.expressions.BitXor;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.Between;
import org.apache.doris.nereids.trees.expressions.IntegralDivide;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.MatchAll;
import org.apache.doris.nereids.trees.expressions.MatchAny;
import org.apache.doris.nereids.trees.expressions.MatchPhrase;
import org.apache.doris.nereids.trees.expressions.MatchPhraseEdge;
import org.apache.doris.nereids.trees.expressions.MatchPhrasePrefix;
import org.apache.doris.nereids.trees.expressions.MatchRegexp;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Mod;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctSum;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Abs;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Round;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Substring;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Year;
import org.apache.doris.nereids.trees.expressions.functions.window.CumeDist;
import org.apache.doris.nereids.trees.expressions.functions.window.DenseRank;
import org.apache.doris.nereids.trees.expressions.functions.window.FirstValue;
import org.apache.doris.nereids.trees.expressions.functions.window.Lag;
import org.apache.doris.nereids.trees.expressions.functions.window.LastValue;
import org.apache.doris.nereids.trees.expressions.functions.window.Lead;
import org.apache.doris.nereids.trees.expressions.functions.window.NthValue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Grouping;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingId;
import org.apache.doris.nereids.trees.expressions.functions.window.Ntile;
import org.apache.doris.nereids.trees.expressions.functions.window.PercentRank;
import org.apache.doris.nereids.trees.expressions.functions.window.Rank;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameBoundType;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameBoundary;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameUnitsType;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.FloatType;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.doris.horn.HornOptimizationContext;
import org.apache.doris.horn.doris2horn.HornScalarThriftBuilder;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/** Translate Horn TOperator (TScalar expressions) back to Doris Nereids Expressions */
public class DorisExpressionBuilder {

    private static final Logger LOG = LogManager.getLogger(DorisExpressionBuilder.class);

    /** 反译 ctx：scalarUidToSlot 等 backward 共享状态都挂在这里 */
    private final HornOptimizationContext hornCtx;

    public DorisExpressionBuilder(HornOptimizationContext hornCtx) {
        this.hornCtx = hornCtx;
    }

    public Expression translate(TOperator op) {
        return translateHelper(op);
    }

    public List<Expression> translateList(List<TOperator> ops) {
        List<Expression> result = new ArrayList<>();
        if (ops == null) {
            return result;
        }
        for (TOperator op : ops) {
            result.add(translate(op));
        }
        return result;
    }
    

    private Expression translateHelper(TOperator op) {
        // BE output can be either fscalar (pre-order flattened) or scalar (single node);
        if (op.getOp_union().isSetFscalar()) {
            List<TOperator> scalars = op.getOp_union().getFscalar().getScalars();
            if (scalars != null && !scalars.isEmpty()) {
                return translateFlattenedScalar(scalars, new AtomicInteger(0));
            }
        }

        TOperatorType opType = op.getOp_type();
        if (!op.getOp_union().isSetScalar()) {
            throw new IllegalArgumentException(
                    "Horn expr translator: TOperator has no scalar data, type=" + opType);
        }

        TScalar scalar = op.getOp_union().getScalar();

        List<Expression> children = new ArrayList<>();
        if (scalar.isSetChildren()) {
            for (TOperator childOp : scalar.getChildren()) {
                children.add(translateHelper(childOp));
            }
        }

        return translateScalarNode(opType, scalar, children);
    }

    /** Rebuild an expression tree from a pre-order flattened TScalar list */
    private Expression translateFlattenedScalar(List<TOperator> scalars, AtomicInteger idx) {
        TOperator op = scalars.get(idx.getAndIncrement());
        TScalar scalar = op.getOp_union().getScalar();
        int arity = (int) scalar.getArity();

        List<Expression> children = new ArrayList<>();
        for (int i = 0; i < arity; i++) {
            children.add(translateFlattenedScalar(scalars, idx));
        }
        return translateScalarNode(op.getOp_type(), scalar, children);
    }

    private Expression translateScalarNode(TOperatorType opType, TScalar scalar,
                                             List<Expression> children) {
        // resultType lazy：只有 kLiteral / kScalarFnCall 真正需要 evaluated 类型
        switch (opType) {
            case kColumnRef:
                return translateColumnRef(scalar);
            case kLiteral:
                return translateLiteral(scalar,
                        HornTypeToDorisConverter.convert(scalar.getData_type()));
            case kBinaryPredicate:
                return translateBinaryPredicate(scalar, children);
            case kAndPredicate:
                return new And(children);
            case kOrPredicate:
                return new Or(children);
            case kArithmetic:
                return translateArithmetic(scalar, children);
            case kScalarFnCall:
                return translateScalarFnCall(scalar, children,
                        HornTypeToDorisConverter.convert(scalar.getData_type()));
            case kAggFn:
                return translateAggFn(scalar, children);
            case kWindowFrame:
                // WindowFrame is-a Expression（未 override getDataType），与正向
                return buildWindowFrame(scalar);
            default:
                throw new UnsupportedOperationException(
                        "Horn expr translator: unsupported operator type " + opType);
        }
    }

    private Expression translateArithmetic(TScalar scalar, List<Expression> children) {
        TScalarArithmetic arith = scalar.getScalar_union().getScalar_arithmetic();
        Expression left = children.get(0);
        Expression right = children.get(1);
        TArithmeticOperatorType opType = arith.getArithmetic_op_type();
        switch (opType) {
            case kAdd:
                return new Add(left, right);
            case kSubtract:
                return new Subtract(left, right);
            case kMultiply:
                return new Multiply(left, right);
            case kDivide:
                return new Divide(left, right);
            case kMod:
                return new Mod(left, right);
            case kIntDivide:
                return new IntegralDivide(left, right);
            case kBitAnd:
                return new BitAnd(left, right);
            case kBitOr:
                return new BitOr(left, right);
            case kBitXor:
                return new BitXor(left, right);
            default:
                throw new UnsupportedOperationException(
                        "Horn expr translator: unsupported arithmetic op " + opType);
        }
    }

    /** Reverse of {@link HornScalarThriftBuilder#visitCast} */
    private Expression translateScalarFnCall(TScalar scalar, List<Expression> children,
                                              DataType resultType) {
        TScalarFnCall fn = scalar.getScalar_union().getScalar_fn_call();
        String fnName = fn.getFn_name();
        if (fnName == null) {
            throw new UnsupportedOperationException(
                    "Horn expr translator: ScalarFnCall without fn_name");
        }
        if (fnName.startsWith("castto") && children.size() == 1) {
            return new Cast(children.get(0), resultType, !fn.isIs_implicit());
        }
        // 黑盒标记函数 alias()（复合根→独立 uid，与被掩的 grouping 键分家）
        if ("alias".equals(fnName)) {
            return children.get(0);
        }
        // 特例：not / in / is_null 不是 BoundFunction 子类（属于 predicate 类），
        switch (fnName) {
            case "not":
                requireArity(fnName, children, 1);
                return new Not(children.get(0));
            case "in":
                // kernel 模式：children = [compareExpr, opt1, ..., optN]，
                if (children.size() < 2) {
                    throw new UnsupportedOperationException(
                            "Horn expr translator: in needs ≥2 children, got "
                                    + children.size());
                }
                return new InPredicate(children.get(0),
                        children.subList(1, children.size()));
            case "between":
                // Between(compareExpr, lowerBound, upperBound),3 children,与 forward
                requireArity(fnName, children, 3);
                return new Between(children.get(0), children.get(1), children.get(2));
            case "bitnot":
                // BitNot(x)(SQL `~x`),与 forward visitBitNot 对称
                requireArity(fnName, children, 1);
                return new BitNot(children.get(0));
            case "match_any":
                // Match 家族 fn 通道反译,与 HornScalarThriftBuilder.visitMatch 对偶
                requireArity(fnName, children, 2);
                return new MatchAny(children.get(0), children.get(1));
            case "match_all":
                requireArity(fnName, children, 2);
                return new MatchAll(children.get(0), children.get(1));
            case "match_phrase":
                requireArity(fnName, children, 2);
                return new MatchPhrase(children.get(0), children.get(1));
            case "match_phrase_prefix":
                requireArity(fnName, children, 2);
                return new MatchPhrasePrefix(children.get(0), children.get(1));
            case "match_phrase_edge":
                requireArity(fnName, children, 2);
                return new MatchPhraseEdge(children.get(0), children.get(1));
            case "match_regexp":
                requireArity(fnName, children, 2);
                return new MatchRegexp(children.get(0), children.get(1));
            case "is_null":
                // x IS NULL，与正向 visitIsNull 的 fn_name="is_null" 同步。
                requireArity(fnName, children, 1);
                return new IsNull(children.get(0));
            case "grouping":
                // 反向 HornScalarThriftBuilder.visitGroupingScalarFunction（fn_name="grouping"）
                requireArity(fnName, children, 1);
                return new Grouping(children.get(0));
            case "grouping_id":
                if (children.isEmpty()) {
                    throw new UnsupportedOperationException(
                            "Horn expr translator: grouping_id needs ≥1 args");
                }
                // GroupingId(Expression firstArg, Expression... varArgs) varargs ctor
                Expression[] rest = children.subList(1, children.size())
                        .toArray(new Expression[0]);
                return new GroupingId(children.get(0), rest);
            default:
                // 普通 scalar / window fn：白名单内的走 Doris FunctionRegistry 通用工厂
                if (DorisFunctionBuilder.KNOWN_SCALAR_FNS.contains(fnName)
                        || DorisFunctionBuilder.KNOWN_WINDOW_FNS.contains(fnName)) {
                    return DorisFunctionBuilder.build(fnName, children);
                }
                throw new UnsupportedOperationException(
                        "Horn expr translator: unsupported ScalarFnCall fn_name=" + fnName
                                + " arity=" + children.size());
        }
    }

    /** horn ScalarAggFn → Doris AggregateFunction */
    private Expression translateAggFn(TScalar scalar, List<Expression> children) {
        TScalarAggFn fn = scalar.getScalar_union().getScalar_agg_fn();
        String fnName = fn.getFn_name();
        if (fnName == null) {
            throw new UnsupportedOperationException(
                    "Horn expr translator: ScalarAggFn missing fn_name");
        }
        if (DorisFunctionBuilder.KNOWN_AGG_FNS.contains(fnName)) {
            return DorisFunctionBuilder.build(fnName, children);
        }
        throw new UnsupportedOperationException(
                "Horn expr translator: unsupported ScalarAggFn fn_name=" + fnName);
    }

    private static void requireArity(String fnName, List<Expression> children, int expected) {
        if (children.size() != expected) {
            throw new UnsupportedOperationException(
                    "Horn expr translator: " + fnName + " expected arity=" + expected
                            + " got=" + children.size());
        }
    }

    /** Reverse of HornScalarThriftBuilder.visitWindowFrame */
    private WindowFrame buildWindowFrame(TScalar scalar) {
        TScalarWindowFrame twf = scalar.getScalar_union().getScalar_window_frame();
        FrameUnitsType units = twf.getWindow_type() == TWindowType.kRows
                ? FrameUnitsType.ROWS : FrameUnitsType.RANGE;
        FrameBoundary left = twf.isSetWindow_start() && !twf.getWindow_start().isEmpty()
                ? buildFrameBoundary(twf.getWindow_start().get(0), true)
                : new FrameBoundary(FrameBoundType.EMPTY_BOUNDARY);
        FrameBoundary right = twf.isSetWindow_end() && !twf.getWindow_end().isEmpty()
                ? buildFrameBoundary(twf.getWindow_end().get(0), false)
                : new FrameBoundary(FrameBoundType.EMPTY_BOUNDARY);
        return new WindowFrame(units, left, right);
    }

    /** Reverse of HornScalarThriftBuilder.buildFrameBoundary */
    private FrameBoundary buildFrameBoundary(TOperator op, boolean isLeft) {
        // boundary 子树（cc ScalarWindowFrame::ToThrift 也走 ToThriftList 拍平进
        TScalar scalar = Horn2DorisUtils.getRootScalar(op);
        TScalarAnalyticWindowBoundary tb = scalar
                .getScalar_union().getScalar_analytic_window_boundary();
        switch (tb.getBoundary_type()) {
            case kCurrentRow:
                return new FrameBoundary(FrameBoundType.CURRENT_ROW);
            case kUnbounded:
                return new FrameBoundary(isLeft
                        ? FrameBoundType.UNBOUNDED_PRECEDING
                        : FrameBoundType.UNBOUNDED_FOLLOWING);
            case kPreceding:
                return new FrameBoundary(
                        Optional.of(buildBoundaryOffset(tb)),
                        FrameBoundType.PRECEDING);
            case kFollowing:
                return new FrameBoundary(
                        Optional.of(buildBoundaryOffset(tb)),
                        FrameBoundType.FOLLOWING);
            default:
                throw new UnsupportedOperationException(
                        "Horn expr translator: unsupported boundary type "
                                + tb.getBoundary_type());
        }
    }

    /** Extract offset for PRECEDING / FOLLOWING boundary */
    private Expression buildBoundaryOffset(TScalarAnalyticWindowBoundary tb) {
        if (tb.isSetRange_offset_predicate() && !tb.getRange_offset_predicate().isEmpty()) {
            return translate(tb.getRange_offset_predicate().get(0));
        }
        if (tb.isSetRows_offset_value()) {
            return new BigIntLiteral(tb.getRows_offset_value());
        }
        throw new UnsupportedOperationException(
                "Horn expr translator: FrameBoundary PRECEDING/FOLLOWING missing offset");
    }

    private Expression translateColumnRef(TScalar scalar) {
        long hornScalarUid = scalar.getScalar_unique_id().getUnique_id();
        Slot existing = hornCtx.getScalarUidToSlot().get(hornScalarUid);
        if (existing != null) {
            return existing;
        }
        TScalarColumnRef colRef = scalar.getScalar_union().getScalar_column_ref();
        DataType type = HornTypeToDorisConverter.convert(scalar.getData_type());
        List<String> qualifier = ImmutableList.of(colRef.getDatabase_name(), colRef.getTable_name());
        SlotReference newSlot = new SlotReference(colRef.getColumn_name(), type, true, qualifier);
        hornCtx.getScalarUidToSlot().put(hornScalarUid, newSlot);
        return newSlot;
    }

    private Expression translateLiteral(TScalar scalar, DataType resultType) {
        TScalarLiteral lit = scalar.getScalar_union().getScalar_literal();

        if (lit.isIs_null_type()) {
            return new NullLiteral(resultType);
        }

        if (lit.isSetLiteral_value()) {
            TLiteralValue literalValue = lit.getLiteral_value();
            if (literalValue.isSetBool_literal()) {
                return BooleanLiteral.of(literalValue.getBool_literal().isValue());
            }
            if (literalValue.isSetInt_literal()) {
                Literal typed = Literal.convertToTypedLiteral(
                        literalValue.getInt_literal().getValue(), resultType);
                if (typed != null) {
                    return typed;
                }
            }
            if (literalValue.isSetString_literal()) {
                return new StringLiteral(literalValue.getString_literal().getValue());
            }
            if (literalValue.isSetFloat_literal()) {
                double val = literalValue.getFloat_literal().getValue();
                if (resultType instanceof FloatType) {
                    return new FloatLiteral((float) val);
                }
                return new DoubleLiteral(val);
            }
            if (literalValue.isSetDecimal_literal()) {
                // 统一转化使用DecimalV3Literal
                String decimalString = new String(
                        literalValue.getDecimal_literal().getValue(), StandardCharsets.UTF_8);
                return new DecimalV3Literal((DecimalV3Type) resultType, new BigDecimal(decimalString));
            }
            if (literalValue.isSetDate_literal()) {
                // 统一转化使用DateV2Literal
                String dateString = literalValue.getDate_literal().getDate_string();
                return new DateV2Literal(dateString);
            }
        }

        throw new UnsupportedOperationException(
                "Horn expr translator: unrecognized literal type for result " + resultType);
    }

    private Expression translateBinaryPredicate(TScalar scalar, List<Expression> children) {
        TScalarBinaryPredicate pred = scalar.getScalar_union()
                .getScalar_predicate().getScalar_binary_predicate();
        Expression left = children.get(0);
        Expression right = children.get(1);

        switch (pred.getBinary_type()) {
            case kEqual:
                return new EqualTo(left, right);
            case kNotEqual:
                // Nereids 没有 NotEqual 类，用 Not(EqualTo) 表达
                return new Not(new EqualTo(left, right));
            case kLowerThan:
                return new LessThan(left, right);
            case kLowerEqual:
                return new LessThanEqual(left, right);
            case kGatherThan:
                return new GreaterThan(left, right);
            case kGatherEqual:
                return new GreaterThanEqual(left, right);
            default:
                throw new UnsupportedOperationException(
                        "Horn expr translator: unsupported binary predicate type "
                                + pred.getBinary_type());
        }
    }

}
