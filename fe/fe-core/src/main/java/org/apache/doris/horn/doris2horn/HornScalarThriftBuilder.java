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

package org.apache.doris.horn.doris2horn;

import org.apache.doris.catalog.TableIf;
import org.apache.horn4j.thrift.TArithmeticOperatorType;
import org.apache.horn4j.thrift.TBinaryOperatorType;
import org.apache.horn4j.thrift.TBoolLiteral;
import org.apache.horn4j.thrift.TDateLiteral;
import org.apache.horn4j.thrift.TDecimalLiteral;
import org.apache.horn4j.thrift.TFlattenedScalar;
import org.apache.horn4j.thrift.TFloatLiteral;
import org.apache.horn4j.thrift.TIntLiteral;
import org.apache.horn4j.thrift.TLiteralType;
import org.apache.horn4j.thrift.TLiteralValue;
import org.apache.horn4j.thrift.TOperator;
import org.apache.horn4j.thrift.TOperatorType;
import org.apache.horn4j.thrift.TOperatorUnion;
import org.apache.horn4j.thrift.TScalar;
import org.apache.horn4j.thrift.TScalarAggFn;
import org.apache.horn4j.thrift.TScalarArithmetic;
import org.apache.horn4j.thrift.TScalarBinaryPredicate;
import org.apache.horn4j.thrift.TScalarFnCall;
import org.apache.horn4j.thrift.TScalarColumnRef;
import org.apache.horn4j.thrift.TScalarLiteral;
import org.apache.horn4j.thrift.TScalarPredicate;
import org.apache.horn4j.thrift.TScalarUnion;
import org.apache.horn4j.thrift.TScalarAnalyticWindowBoundary;
import org.apache.horn4j.thrift.TScalarUniqueId;
import org.apache.horn4j.thrift.TScalarWindowFrame;
import org.apache.horn4j.thrift.TStringLiteral;
import org.apache.horn4j.thrift.TColumnType;
import org.apache.horn4j.thrift.TWindowBoundaryType;
import org.apache.horn4j.thrift.TWindowType;
import org.apache.doris.catalog.Type;
import org.apache.doris.horn.horn2doris.DorisFunctionBuilder;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.BitAnd;
import org.apache.doris.nereids.trees.expressions.BitNot;
import org.apache.doris.nereids.trees.expressions.BitOr;
import org.apache.doris.nereids.trees.expressions.BitXor;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Between;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.IntegralDivide;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.MatchAll;
import org.apache.doris.nereids.trees.expressions.MatchAny;
import org.apache.doris.nereids.trees.expressions.MatchPhrase;
import org.apache.doris.nereids.trees.expressions.MatchPhraseEdge;
import org.apache.doris.nereids.trees.expressions.MatchPhrasePrefix;
import org.apache.doris.nereids.trees.expressions.MatchRegexp;
import org.apache.doris.nereids.trees.expressions.Mod;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameBoundType;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameBoundary;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameUnitsType;
import org.apache.doris.nereids.trees.expressions.functions.window.CumeDist;
import org.apache.doris.nereids.trees.expressions.functions.window.DenseRank;
import org.apache.doris.nereids.trees.expressions.functions.window.FirstValue;
import org.apache.doris.nereids.trees.expressions.functions.window.Lag;
import org.apache.doris.nereids.trees.expressions.functions.window.LastValue;
import org.apache.doris.nereids.trees.expressions.functions.window.Lead;
import org.apache.doris.nereids.trees.expressions.functions.window.NthValue;
import org.apache.doris.nereids.trees.expressions.functions.window.Ntile;
import org.apache.doris.nereids.trees.expressions.functions.window.PercentRank;
import org.apache.doris.nereids.trees.expressions.functions.window.Rank;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.expressions.functions.window.WindowFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctSum;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Abs;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Round;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingScalarFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Substring;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Year;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** ExpressionVisitor: Doris Nereids Expression → Horn TOperator (TFlattenedScalar) */
public class HornScalarThriftBuilder extends ExpressionVisitor<TOperator, Void> {

    /** Serialize an Expression as a TOperator (fscalar slot, pre-order flattened) */
    public TOperator translate(Expression expr) {
        TOperator top = new TOperator();
        TOperatorUnion opUnion = new TOperatorUnion();
        TFlattenedScalar fscalar = new TFlattenedScalar();
        List<TOperator> scalars = new ArrayList<>();
        flattenPreOrder(expr, scalars);
        fscalar.setScalars(scalars);
        opUnion.setFscalar(fscalar);
        top.setOp_union(opUnion);
        top.setOp_type(TOperatorType.kInvalid);
        return top;
    }

    public List<TOperator> translateList(List<? extends Expression> exprs) {
        List<TOperator> result = new ArrayList<>();
        for (Expression expr : exprs) {
            result.add(translate(expr));
        }
        return result;
    }

    /** Pre-order traversal: visit, then recurse children */
    private void flattenPreOrder(Expression expr, List<TOperator> out) {
        // Alias is transparent: emit only its inner expression's subtree,
        if (expr instanceof Alias) {
            flattenPreOrder(expr.child(0), out);
            return;
        }
        out.add(expr.accept(this, null));
        for (Expression child : expr.children()) {
            flattenPreOrder(child, out);
        }
    }

    /** 把已经算好的 (data_type, arity, op_type, scalar_union) 打包成 TOperator */
    private TOperator buildScalar(TColumnType dataType, int arity,
                                  TOperatorType opType, TScalarUnion scalarUnion) {
        TScalar tscalar = new TScalar();
        tscalar.setData_type(dataType);
        // Set uid to -1 (empty): BE's CreateUniqueId will assign a proper counter-based uid
        tscalar.setScalar_unique_id(new TScalarUniqueId(-1));
        tscalar.setArity(arity);
        tscalar.setScalar_union(scalarUnion);

        TOperatorUnion opUnion = new TOperatorUnion();
        opUnion.setScalar(tscalar);
        TOperator top = new TOperator();
        top.setOp_union(opUnion);
        top.setOp_type(opType);
        return top;
    }

    @Override
    public TOperator visit(Expression expr, Void ctx) {
        throw new UnsupportedOperationException(
                "Horn: unsupported expression type: " + expr.getClass().getSimpleName());
    }

    @Override
    public TOperator visitSlotReference(SlotReference slot, Void ctx) {
        TScalarUnion scalarUnion = new TScalarUnion();
        TScalarColumnRef columnRef = new TScalarColumnRef();
        columnRef.setColumn_name(slot.getName());
        // 用 oneLevelTable（不穿透 view）而不是 originalTable（穿透到 base table）
        if (slot.getOneLevelTable().isPresent()) {
            TableIf table = slot.getOneLevelTable().get();
            columnRef.setTable_name(table.getName());
            if (table.getDatabase() != null) {
                columnRef.setDatabase_name(table.getDatabase().getFullName());
            }
        }
        columnRef.setExternal_slot_id((int) slot.getExprId().asInt());
        scalarUnion.setScalar_column_ref(columnRef);
        return buildScalar(DorisTypeToHornConverter.convert(slot.getDataType()),
                0, TOperatorType.kColumnRef, scalarUnion);
    }

    @Override
    public TOperator visitLiteral(Literal literal, Void ctx) {
        TScalarLiteral scalarLiteral = new TScalarLiteral();
        TStringLiteral displayLiteral = new TStringLiteral();
        displayLiteral.setValue(literal.toString());
        scalarLiteral.setDisplay_literal(displayLiteral);
        if (literal instanceof NullLiteral) {
            // Null literal: no literal_value set, only is_null_type=true.
            scalarLiteral.setIs_null_type(true);
        } else {
            TLiteralValue literalValue = new TLiteralValue();
            TLiteralType literalType;
            if (literal instanceof BooleanLiteral) {
                literalValue.setBool_literal(new TBoolLiteral(((BooleanLiteral) literal).getValue()));
                literalType = TLiteralType.kBoolLiteral;
            } else if (literal instanceof TinyIntLiteral || literal instanceof SmallIntLiteral
                    || literal instanceof IntegerLiteral || literal instanceof BigIntLiteral) {
                // LargeIntLiteral 暂不支持
                literalValue.setInt_literal(
                        new TIntLiteral(((IntegerLikeLiteral) literal).getLongValue()));
                literalType = TLiteralType.kIntLiteral;
            } else if (literal instanceof DecimalLiteral || literal instanceof DecimalV3Literal) {
                String decimalString = literal.getValue().toString();
                TDecimalLiteral decimalLiteral = new TDecimalLiteral();
                decimalLiteral.setValue(decimalString.getBytes(StandardCharsets.UTF_8));
                literalValue.setDecimal_literal(decimalLiteral);
                literalType = TLiteralType.kDecimalLiteral;
            } else if (literal instanceof FloatLiteral || literal instanceof DoubleLiteral) {
                // Float / Double
                double doubleValue = ((Number) literal.getValue()).doubleValue();
                literalValue.setFloat_literal(new TFloatLiteral(doubleValue));
                literalType = TLiteralType.kFloatLiteral;
            } else if (literal instanceof StringLikeLiteral) {
                // Varchar/Char/String: use getStringValue() to avoid quotes.
                TStringLiteral stringLiteral = new TStringLiteral();
                stringLiteral.setValue(((StringLikeLiteral) literal).getStringValue());
                literalValue.setString_literal(stringLiteral);
                literalType = TLiteralType.kStringLiteral;
            } else if (literal instanceof DateLiteral || literal instanceof DateV2Literal) {
                // 统一先转化为 DateV2Literal
                DateLiteral src = (DateLiteral) literal;
                DateV2Literal dateLiteral = (literal instanceof DateV2Literal)
                        ? (DateV2Literal) literal
                        : new DateV2Literal(src.getYear(), src.getMonth(), src.getDay());
                long daysSinceEpoch = dateLiteral.toJavaDateType().toLocalDate().toEpochDay();
                TDateLiteral tDate = new TDateLiteral();
                tDate.setDays_since_epoch((int) daysSinceEpoch);
                tDate.setDate_string(dateLiteral.toString());
                literalValue.setDate_literal(tDate);
                literalType = TLiteralType.kDateLiteral;
            } else {
                // fallback
                return visit(literal, ctx);
            }
            scalarLiteral.setLiteral_value(literalValue);
            scalarLiteral.setLiteral_type(literalType);
            scalarLiteral.setIs_null_type(false);
        }
        TScalarUnion scalarUnion = new TScalarUnion();
        scalarUnion.setScalar_literal(scalarLiteral);
        return buildScalar(DorisTypeToHornConverter.convert(literal.getDataType()),
                0, TOperatorType.kLiteral, scalarUnion);
    }

    @Override
    public TOperator visitComparisonPredicate(ComparisonPredicate cp, Void ctx) {
        TBinaryOperatorType type;
        if (cp instanceof EqualTo) {
            type = TBinaryOperatorType.kEqual;
        } else if (cp instanceof GreaterThan) {
            type = TBinaryOperatorType.kGatherThan;
        } else if (cp instanceof GreaterThanEqual) {
            type = TBinaryOperatorType.kGatherEqual;
        } else if (cp instanceof LessThan) {
            type = TBinaryOperatorType.kLowerThan;
        } else if (cp instanceof LessThanEqual) {
            type = TBinaryOperatorType.kLowerEqual;
        } else {
            // NullSafeEqual / DistinctFrom 走 horn 的特殊 binary type，独立任务。
            return visit(cp, ctx);
        }
        TScalarUnion scalarUnion = new TScalarUnion();
        TScalarBinaryPredicate binaryPredicate = new TScalarBinaryPredicate();
        binaryPredicate.setBinary_type(type);
        TScalarPredicate scalarPredicate = new TScalarPredicate();
        scalarPredicate.setHas_always_true_hint(false);
        scalarPredicate.setScalar_binary_predicate(binaryPredicate);
        scalarUnion.setScalar_predicate(scalarPredicate);
        return buildScalar(DorisTypeToHornConverter.convert(cp.getDataType()),
                cp.children().size(), TOperatorType.kBinaryPredicate, scalarUnion);
    }

    @Override
    public TOperator visitCompoundPredicate(CompoundPredicate cp, Void ctx) {
        TOperatorType opType;
        if (cp instanceof And) {
            opType = TOperatorType.kAndPredicate;
        } else if (cp instanceof Or) {
            opType = TOperatorType.kOrPredicate;
        } else {
            return visit(cp, ctx);
        }
        TScalarUnion scalarUnion = new TScalarUnion();
        scalarUnion.setScalar_predicate(new TScalarPredicate(false));
        return buildScalar(DorisTypeToHornConverter.convert(cp.getDataType()),
                cp.children().size(), opType, scalarUnion);
    }

    /** Alias is "transparent" in horn's flattened scalar model: the Alias's */
    @Override
    public TOperator visitAlias(Alias alias, Void ctx) {
        return alias.child().accept(this, ctx);
    }

    /** P0 聚合函数（count / sum / min / max / avg）+ multi_distinct_count / multi_distinct_sum */
    @Override
    public TOperator visitAggregateFunction(AggregateFunction agg, Void ctx) {
        if (agg.isDistinct()) {
            throw new UnsupportedOperationException(
                    "Horn: unsupported expression type: distinct " + agg.getName().toLowerCase());
        }
        // 共享白名单（DorisFunctionBuilder.KNOWN_AGG_FNS）：跟 scalar / window 同模式，
        String name = agg.getName().toLowerCase();
        if (!DorisFunctionBuilder.KNOWN_AGG_FNS.contains(name)) {
            throw new UnsupportedOperationException(
                    "Horn: unsupported expression type: " + agg.getClass().getSimpleName());
        }
        TScalarAggFn aggFn = new TScalarAggFn();
        aggFn.setFn_name(name);
        TScalarUnion scalarUnion = new TScalarUnion();
        scalarUnion.setScalar_agg_fn(aggFn);
        return buildScalar(DorisTypeToHornConverter.convert(agg.getDataType()),
                agg.children().size(), TOperatorType.kAggFn, scalarUnion);
    }

    /** Cast → horn ScalarFunctionCall with fn_name="castto<lowercased_target_type>" */
    @Override
    public TOperator visitCast(Cast cast, Void ctx) {
        String typeName = cast.getDataType().simpleString().toLowerCase();
        TScalarFnCall fnCall = new TScalarFnCall();
        fnCall.setFn_name("castto" + typeName);
        fnCall.setIs_implicit(!cast.isExplicitType());
        TScalarUnion scalarUnion = new TScalarUnion();
        scalarUnion.setScalar_fn_call(fnCall);
        return buildScalar(DorisTypeToHornConverter.convert(cast.getDataType()),
                cast.children().size(), TOperatorType.kScalarFnCall, scalarUnion);
    }

    /** TScalarFnCall(fn_name=小写函数名)，复用 cast 同一条 wire 通道 */
    /** Not（NOT LIKE / != 的 Not(EqualTo) 形态等）走 fn 通道黑盒 */
    @Override
    public TOperator visitNot(Not not, Void ctx) {
        TScalarFnCall fnCall = new TScalarFnCall();
        fnCall.setFn_name("not");
        TScalarUnion scalarUnion = new TScalarUnion();
        scalarUnion.setScalar_fn_call(fnCall);
        return buildScalar(DorisTypeToHornConverter.convert(not.getDataType()),
                not.children().size(), TOperatorType.kScalarFnCall, scalarUnion);
    }

    /** Impala/kernel 的做法 —— `InPredicate.toThrift` 序列化为 FUNCTION_CALL */
    @Override
    public TOperator visitInPredicate(InPredicate in, Void ctx) {
        TScalarFnCall fnCall = new TScalarFnCall();
        fnCall.setFn_name("in");
        TScalarUnion scalarUnion = new TScalarUnion();
        scalarUnion.setScalar_fn_call(fnCall);
        return buildScalar(DorisTypeToHornConverter.convert(in.getDataType()),
                in.children().size(), TOperatorType.kScalarFnCall, scalarUnion);
    }

    /** BitNot(x)(SQL `~x`)走 fn 通道黑盒,fn_name="bitnot",children=[x] */
    @Override
    public TOperator visitBitNot(BitNot bitNot, Void ctx) {
        TScalarFnCall fnCall = new TScalarFnCall();
        fnCall.setFn_name("bitnot");
        TScalarUnion scalarUnion = new TScalarUnion();
        scalarUnion.setScalar_fn_call(fnCall);
        return buildScalar(DorisTypeToHornConverter.convert(bitNot.getDataType()),
                bitNot.children().size(), TOperatorType.kScalarFnCall, scalarUnion);
    }

    /** Between(x, lo, hi)(即 SQL `x BETWEEN lo AND hi`)走 fn 通道黑盒, */
    @Override
    public TOperator visitBetween(Between between, Void ctx) {
        TScalarFnCall fnCall = new TScalarFnCall();
        fnCall.setFn_name("between");
        TScalarUnion scalarUnion = new TScalarUnion();
        scalarUnion.setScalar_fn_call(fnCall);
        return buildScalar(DorisTypeToHornConverter.convert(between.getDataType()),
                between.children().size(), TOperatorType.kScalarFnCall, scalarUnion);
    }

    /** Match 家族(MatchAny / MatchAll / MatchPhrase / MatchPhrasePrefix / */
    @Override
    public TOperator visitMatch(Match match, Void ctx) {
        String fnName;
        if (match instanceof MatchAny) {
            fnName = "match_any";
        } else if (match instanceof MatchAll) {
            fnName = "match_all";
        } else if (match instanceof MatchPhrasePrefix) {
            fnName = "match_phrase_prefix";
        } else if (match instanceof MatchPhraseEdge) {
            fnName = "match_phrase_edge";
        } else if (match instanceof MatchPhrase) {
            fnName = "match_phrase";
        } else if (match instanceof MatchRegexp) {
            fnName = "match_regexp";
        } else {
            throw new UnsupportedOperationException(
                    "Horn: unsupported Match subclass: " + match.getClass().getSimpleName());
        }
        TScalarFnCall fnCall = new TScalarFnCall();
        fnCall.setFn_name(fnName);
        TScalarUnion scalarUnion = new TScalarUnion();
        scalarUnion.setScalar_fn_call(fnCall);
        return buildScalar(DorisTypeToHornConverter.convert(match.getDataType()),
                match.children().size(), TOperatorType.kScalarFnCall, scalarUnion);
    }

    /** IsNull(x)（x IS NULL）走 fn 通道黑盒，fn_name="is_null"，child=[x] */
    @Override
    public TOperator visitIsNull(IsNull isNull, Void ctx) {
        TScalarFnCall fnCall = new TScalarFnCall();
        fnCall.setFn_name("is_null");
        TScalarUnion scalarUnion = new TScalarUnion();
        scalarUnion.setScalar_fn_call(fnCall);
        return buildScalar(DorisTypeToHornConverter.convert(isNull.getDataType()),
                isNull.children().size(), TOperatorType.kScalarFnCall, scalarUnion);
    }

    /** GROUPING(col) / GROUPING_ID(c1,...,cn) 走 ScalarFunc("grouping"/"grouping_id") 黑盒通道 */
    @Override
    public TOperator visitGroupingScalarFunction(GroupingScalarFunction fn, Void ctx) {
        TScalarFnCall fnCall = new TScalarFnCall();
        fnCall.setFn_name(fn.getName().toLowerCase());
        TScalarUnion scalarUnion = new TScalarUnion();
        scalarUnion.setScalar_fn_call(fnCall);
        return buildScalar(DorisTypeToHornConverter.convert(fn.getDataType()),
                fn.children().size(), TOperatorType.kScalarFnCall, scalarUnion);
    }

    @Override
    public TOperator visitScalarFunction(ScalarFunction fn, Void ctx) {
        // 共享白名单（DorisFunctionBuilder.KNOWN_SCALAR_FNS）：backward 端反译时
        String name = fn.getName().toLowerCase();
        if (!DorisFunctionBuilder.KNOWN_SCALAR_FNS.contains(name)) {
            return visit(fn, ctx);
        }
        TScalarFnCall fnCall = new TScalarFnCall();
        fnCall.setFn_name(name);
        TScalarUnion scalarUnion = new TScalarUnion();
        scalarUnion.setScalar_fn_call(fnCall);
        return buildScalar(DorisTypeToHornConverter.convert(fn.getDataType()),
                fn.children().size(), TOperatorType.kScalarFnCall, scalarUnion);
    }

    /** TimestampArithmetic（{@code date +/- INTERVAL n UNIT}）：它 extends Expression（非 */
    @Override
    public TOperator visitTimestampArithmetic(TimestampArithmetic arith, Void ctx) {
        String name = arith.getFuncName() == null ? null : arith.getFuncName().toLowerCase();
        if (name == null || !DorisFunctionBuilder.KNOWN_SCALAR_FNS.contains(name)) {
            return visit(arith, ctx);
        }
        TScalarFnCall fnCall = new TScalarFnCall();
        fnCall.setFn_name(name);
        TScalarUnion scalarUnion = new TScalarUnion();
        scalarUnion.setScalar_fn_call(fnCall);
        return buildScalar(DorisTypeToHornConverter.convert(arith.getDataType()),
                arith.children().size(), TOperatorType.kScalarFnCall, scalarUnion);
    }

    /** ScalarFunction 共用 wire 通道 —— 都 emit kScalarFnCall + fn_name=lower(getName()) */
    @Override
    public TOperator visitWindowFunction(WindowFunction fn, Void ctx) {
        // 共享白名单（DorisFunctionBuilder.KNOWN_WINDOW_FNS）：跟 scalar fn 同模式，
        String name = fn.getName().toLowerCase();
        if (!DorisFunctionBuilder.KNOWN_WINDOW_FNS.contains(name)) {
            return visit(fn, ctx);
        }
        TScalarFnCall fnCall = new TScalarFnCall();
        fnCall.setFn_name(name);
        TScalarUnion scalarUnion = new TScalarUnion();
        scalarUnion.setScalar_fn_call(fnCall);
        return buildScalar(DorisTypeToHornConverter.convert(fn.getDataType()),
                fn.children().size(), TOperatorType.kScalarFnCall, scalarUnion);
    }

    /** WindowFrame → horn TScalarWindowFrame（kWindowFrame） */
    /** WindowFrame → TScalarWindowFrame（kWindowFrame） */
    @Override
    public TOperator visitWindowFrame(WindowFrame frame, Void ctx) {
        TScalarWindowFrame twf = new TScalarWindowFrame();
        twf.setWindow_type(frame.getFrameUnits() == FrameUnitsType.ROWS
                ? TWindowType.kRows : TWindowType.kRange);
        if (!frame.getLeftBoundary().isNull()) {
            twf.setWindow_start(Collections.singletonList(
                    buildFrameBoundary(frame.getLeftBoundary(), true)));
        }
        if (!frame.getRightBoundary().isNull()) {
            twf.setWindow_end(Collections.singletonList(
                    buildFrameBoundary(frame.getRightBoundary(), false)));
        }
        TScalarUnion union = new TScalarUnion();
        union.setScalar_window_frame(twf);
        return buildScalar(DorisTypeToHornConverter.convertCatalogType(Type.INVALID),
                0, TOperatorType.kWindowFrame, union);
    }

    /** FrameBoundary 不是 Expression（是 WindowFrame 的静态内部类，根本没 */
    private TOperator buildFrameBoundary(FrameBoundary frameBoundary, boolean isLeft) {
        TScalarAnalyticWindowBoundary tBoundary = new TScalarAnalyticWindowBoundary();
        FrameBoundType frameBoundType = frameBoundary.getFrameBoundType();
        switch (frameBoundType) {
            case UNBOUNDED_PRECEDING:
            case UNBOUNDED_FOLLOWING:
                tBoundary.setBoundary_type(TWindowBoundaryType.kUnbounded);
                break;
            case CURRENT_ROW:
                tBoundary.setBoundary_type(TWindowBoundaryType.kCurrentRow);
                break;
            case PRECEDING:
                tBoundary.setBoundary_type(TWindowBoundaryType.kPreceding);
                break;
            case FOLLOWING:
                tBoundary.setBoundary_type(TWindowBoundaryType.kFollowing);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Horn: unsupported FrameBoundType: " + frameBoundType);
        }
        if (frameBoundary.hasOffset() && frameBoundary.getBoundOffset().isPresent()) {
            Expression offset = frameBoundary.getBoundOffset().get();
            if (offset instanceof IntegerLikeLiteral) {
                tBoundary.setRows_offset_value(((IntegerLikeLiteral) offset).getLongValue());
            } else {
                tBoundary.setRange_offset_predicate(Collections.singletonList(
                        offset.accept(this, null)));
            }
        }
        TScalarUnion union = new TScalarUnion();
        union.setScalar_analytic_window_boundary(tBoundary);
        return buildScalar(DorisTypeToHornConverter.convertCatalogType(Type.INVALID),
                0, TOperatorType.kAnalyticWindowBoundary, union);
    }

    @Override
    public TOperator visitBinaryArithmetic(BinaryArithmetic arith, Void ctx) {
        TArithmeticOperatorType opType;
        if (arith instanceof Add) {
            opType = TArithmeticOperatorType.kAdd;
        } else if (arith instanceof Subtract) {
            opType = TArithmeticOperatorType.kSubtract;
        } else if (arith instanceof Multiply) {
            opType = TArithmeticOperatorType.kMultiply;
        } else if (arith instanceof Divide) {
            opType = TArithmeticOperatorType.kDivide;
        } else if (arith instanceof Mod) {
            opType = TArithmeticOperatorType.kMod;
        } else if (arith instanceof IntegralDivide) {
            opType = TArithmeticOperatorType.kIntDivide;
        } else if (arith instanceof BitAnd) {
            opType = TArithmeticOperatorType.kBitAnd;
        } else if (arith instanceof BitOr) {
            opType = TArithmeticOperatorType.kBitOr;
        } else if (arith instanceof BitXor) {
            opType = TArithmeticOperatorType.kBitXor;
        } else {
            return visit(arith, ctx);
        }
        TScalarUnion scalarUnion = new TScalarUnion();
        TScalarArithmetic arithmetic = new TScalarArithmetic();
        arithmetic.setArithmetic_op_type(opType);
        scalarUnion.setScalar_arithmetic(arithmetic);
        return buildScalar(DorisTypeToHornConverter.convert(arith.getDataType()),
                arith.children().size(), TOperatorType.kArithmetic, scalarUnion);
    }

}
