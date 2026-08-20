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


import org.apache.doris.horn.HornOptimizationContext;
import org.apache.doris.horn.doris2horn.HornScalarThriftBuilder;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.BitAnd;
import org.apache.doris.nereids.trees.expressions.BitOr;
import org.apache.doris.nereids.trees.expressions.BitXor;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IntegralDivide;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Mod;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameBoundType;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameBoundary;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameUnitsType;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Grouping;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingId;
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
import org.apache.horn4j.thrift.TArithmeticOperatorType;
import org.apache.horn4j.thrift.TLiteralValue;
import org.apache.horn4j.thrift.TOperator;
import org.apache.horn4j.thrift.TOperatorType;
import org.apache.horn4j.thrift.TScalar;
import org.apache.horn4j.thrift.TScalarAggFn;
import org.apache.horn4j.thrift.TScalarAnalyticWindowBoundary;
import org.apache.horn4j.thrift.TScalarArithmetic;
import org.apache.horn4j.thrift.TScalarBinaryPredicate;
import org.apache.horn4j.thrift.TScalarColumnRef;
import org.apache.horn4j.thrift.TScalarFnCall;
import org.apache.horn4j.thrift.TScalarLiteral;
import org.apache.horn4j.thrift.TScalarWindowFrame;
import org.apache.horn4j.thrift.TWindowType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Translate Horn TOperator (TScalar expressions) back to Doris Nereids Expressions.
 * 不支持的 TOperator / 字面值类型一律抛 {@link UnsupportedOperationException}，
 * 由 caller fallback 到 Nereids 主路径。
 */
public class DorisExpressionBuilder {

    private static final Logger LOG = LogManager.getLogger(DorisExpressionBuilder.class);

    /**
     * 反译共享状态：ColumnRef 反译时按 unique_id 查
     * {@link HornOptimizationContext#getScalarUidToSlot()} 复用同一 Slot 实例，
     * 保持 ExprId 跨 plan 节点一致。
     */
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
        // fscalar must be rebuilt into a tree before translating.
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

    /**
     * Rebuild an expression tree from a pre-order flattened TScalar list.
     * Inverse of BE Scalar::FromFlattenedThrift.
     */
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
        // resultType lazy：只有 kLiteral / kScalarFnCall 真正需要 evaluated 类型。
        // kWindowFrame 的 data_type 是 INVALID_TYPE，eager convert 会抛异常。
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
                // WindowFrame is-a Expression，统一从 translateScalarNode 分发。
                // boundary 在 union 字段（window_start/window_end），由 buildWindowFrame 读取。
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

    /**
     * Reverse of {@link HornScalarThriftBuilder#visitCast}.
     * fn_name "castto&lt;type&gt;" → Cast; target type from {@code scalar.data_type};
     * is_implicit ⇒ !isExplicitType. TryCast/Cast distinction not preserved.
     */
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
        // alias：BE 把重命名副本的 producer 包成黑盒标记函数 alias()（独立 uid）；
        // backward 剥回唯一 child，在 buildProject 里还原成真 Doris Alias。
        if ("alias".equals(fnName)) {
            return children.get(0);
        }
        // 特例：not / in / is_null 不是 BoundFunction 子类，FunctionRegistry 不能 lookup，
        // 单独处理；其他普通 scalar fn 走通用工厂 DorisFunctionBuilder.build()。
        switch (fnName) {
            case "not":
                requireArity(fnName, children, 1);
                return new Not(children.get(0));
            case "in":
                // children = [compareExpr, opt1, ..., optN]，与 Doris InPredicate 同序。
                // NOT IN 是 Not(In(...))，由 "not" case 处理。
                if (children.size() < 2) {
                    throw new UnsupportedOperationException(
                            "Horn expr translator: in needs ≥2 children, got "
                                    + children.size());
                }
                return new InPredicate(children.get(0),
                        children.subList(1, children.size()));
            case "is_null":
                // x IS NULL，与正向 visitIsNull 的 fn_name="is_null" 同步。
                requireArity(fnName, children, 1);
                return new IsNull(children.get(0));
            case "grouping":
                // 反向 visitGroupingScalarFunction；黑盒通道用于 mv rewrite 等保留原
                // 函数形态的兜底（一般 NormalizeRepeat 已替成 GROUPING_PREFIX 虚拟 slot）。
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
                // DorisFunctionBuilder.build。新增 fn 只动 KNOWN_SCALAR_FNS / KNOWN_WINDOW_FNS。
                if (DorisFunctionBuilder.KNOWN_SCALAR_FNS.contains(fnName)
                        || DorisFunctionBuilder.KNOWN_WINDOW_FNS.contains(fnName)) {
                    return DorisFunctionBuilder.build(fnName, children);
                }
                throw new UnsupportedOperationException(
                        "Horn expr translator: unsupported ScalarFnCall fn_name=" + fnName
                                + " arity=" + children.size());
        }
    }

    /**
     * horn ScalarAggFn → Doris AggregateFunction。白名单内的 fn_name
     * （{@link DorisFunctionBuilder#KNOWN_AGG_FNS}）走 FunctionRegistry 通用工厂。
     * phase/mode 不在此表达（由 buildAgg 设 AggregateParam）；distinct 已消解成 multi_distinct_xxx。
     */
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

    /**
     * Reverse of HornScalarThriftBuilder.visitWindowFrame.
     * 经 translateScalarNode 的 {@code case kWindowFrame} 分发（WindowFrame is-a Expression）。
     * boundary 在 union 字段 window_start/window_end，由 buildFrameBoundary 反序列化；
     * 缺失 → EMPTY_BOUNDARY。
     */
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

    /**
     * Reverse of HornScalarThriftBuilder.buildFrameBoundary.
     * kUnbounded 端点根据 isLeft 决定 UNBOUNDED_PRECEDING / UNBOUNDED_FOLLOWING。
     */
    private FrameBoundary buildFrameBoundary(TOperator op, boolean isLeft) {
        // boundary 子树取 root —— 复用 Horn2DorisUtils.getRootScalar
        // （兼容 scalar 单节点 / fscalar 扁平两形态）。
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

    /**
     * Extract offset for PRECEDING / FOLLOWING boundary.
     * - range_offset_predicate (TOperator) → translate() 递归反译（RANGE frame）
     * - rows_offset_value (long) → BigIntLiteral（ROWS frame）
     *
     * <p>顺序敏感：range_offset_predicate **先**判断。cc 端无条件 set rows_offset_value，
     * RANGE boundary wire 上同时 isset 两边；先判 rows 会拿 BigIntLiteral(0) 覆盖 RANGE 谓词。
     */
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
