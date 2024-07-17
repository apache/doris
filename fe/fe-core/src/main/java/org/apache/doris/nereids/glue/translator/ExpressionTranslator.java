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

import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.AssertNumRowsElement.Assertion;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CaseExpr;
import org.apache.doris.analysis.CaseWhenClause;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.ColumnRefExpr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.analysis.FunctionParams;
import org.apache.doris.analysis.IndexDef;
import org.apache.doris.analysis.InvertedIndexUtil;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LambdaFunctionCallExpr;
import org.apache.doris.analysis.LambdaFunctionExpr;
import org.apache.doris.analysis.MatchPredicate;
import org.apache.doris.analysis.OrderByElement;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TimestampArithmeticExpr;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.AssertNumRowsElement;
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Exists;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.UnaryArithmetic;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.combinator.ForEachCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.MergeCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.StateCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.UnionCombinator;
import org.apache.doris.nereids.trees.expressions.functions.generator.TableGeneratingFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HighOrderFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdaf;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdf;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdtf;
import org.apache.doris.nereids.trees.expressions.functions.window.WindowFunction;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.thrift.TFunctionBinaryType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Used to translate expression of new optimizer to stale expr.
 */
@SuppressWarnings("rawtypes")
public class ExpressionTranslator extends DefaultExpressionVisitor<Expr, PlanTranslatorContext> {

    public static ExpressionTranslator INSTANCE = new ExpressionTranslator();

    /**
     * The entry function of ExpressionTranslator.
     *
     * @param expression nereids expression
     * @param context translator context
     * @return stale planner's expr
     */
    public static Expr translate(Expression expression, PlanTranslatorContext context) {
        return expression.accept(INSTANCE, context);
    }

    @Override
    public Expr visitAlias(Alias alias, PlanTranslatorContext context) {
        return alias.child().accept(this, context);
    }

    @Override
    public Expr visitEqualTo(EqualTo equalTo, PlanTranslatorContext context) {
        BinaryPredicate eq = new BinaryPredicate(Operator.EQ,
                equalTo.child(0).accept(this, context),
                equalTo.child(1).accept(this, context),
                equalTo.getDataType().toCatalogDataType(),
                NullableMode.DEPEND_ON_ARGUMENT);
        eq.setNullableFromNereids(equalTo.nullable());
        return eq;
    }

    @Override
    public Expr visitGreaterThan(GreaterThan greaterThan, PlanTranslatorContext context) {
        BinaryPredicate gt = new BinaryPredicate(Operator.GT,
                greaterThan.child(0).accept(this, context),
                greaterThan.child(1).accept(this, context),
                greaterThan.getDataType().toCatalogDataType(),
                NullableMode.DEPEND_ON_ARGUMENT);
        gt.setNullableFromNereids(greaterThan.nullable());
        return gt;
    }

    @Override
    public Expr visitGreaterThanEqual(GreaterThanEqual greaterThanEqual, PlanTranslatorContext context) {
        BinaryPredicate ge = new BinaryPredicate(Operator.GE,
                greaterThanEqual.child(0).accept(this, context),
                greaterThanEqual.child(1).accept(this, context),
                greaterThanEqual.getDataType().toCatalogDataType(),
                NullableMode.DEPEND_ON_ARGUMENT);
        ge.setNullableFromNereids(greaterThanEqual.nullable());
        return ge;
    }

    @Override
    public Expr visitLessThan(LessThan lessThan, PlanTranslatorContext context) {
        BinaryPredicate lt = new BinaryPredicate(Operator.LT,
                lessThan.child(0).accept(this, context),
                lessThan.child(1).accept(this, context),
                lessThan.getDataType().toCatalogDataType(),
                NullableMode.DEPEND_ON_ARGUMENT);
        lt.setNullableFromNereids(lessThan.nullable());
        return lt;
    }

    @Override
    public Expr visitLessThanEqual(LessThanEqual lessThanEqual, PlanTranslatorContext context) {
        BinaryPredicate le = new BinaryPredicate(Operator.LE,
                lessThanEqual.child(0).accept(this, context),
                lessThanEqual.child(1).accept(this, context),
                lessThanEqual.getDataType().toCatalogDataType(),
                NullableMode.DEPEND_ON_ARGUMENT);
        le.setNullableFromNereids(lessThanEqual.nullable());
        return le;
    }

    private OlapTable getOlapTableFromSlotDesc(SlotDescriptor slotDesc) {
        if (slotDesc != null && slotDesc.isScanSlot()) {
            TupleDescriptor slotParent = slotDesc.getParent();
            return (OlapTable) slotParent.getTable();
        }
        return null;
    }

    private OlapTable getOlapTableDirectly(SlotRef left) {
        if (left.getTableDirect() instanceof OlapTable) {
            return (OlapTable) left.getTableDirect();
        }
        return null;
    }

    @Override
    public Expr visitElementAt(ElementAt elementAt, PlanTranslatorContext context) {
        return visitScalarFunction(elementAt, context);
    }

    @Override
    public Expr visitMatch(Match match, PlanTranslatorContext context) {
        String invertedIndexParser = InvertedIndexUtil.INVERTED_INDEX_PARSER_UNKNOWN;
        String invertedIndexParserMode = InvertedIndexUtil.INVERTED_INDEX_PARSER_COARSE_GRANULARITY;
        Map<String, String> invertedIndexCharFilter = new HashMap<>();
        // Get the first slot from match's left expr
        SlotRef left = (SlotRef) match.left().getInputSlots().stream().findFirst().get().accept(this, context);
        OlapTable olapTbl = Optional.ofNullable(getOlapTableFromSlotDesc(left.getDesc()))
                                    .orElse(getOlapTableDirectly(left));

        if (olapTbl == null) {
            throw new AnalysisException("slotRef in matchExpression failed to get OlapTable");
        }

        List<Index> indexes = olapTbl.getIndexes();
        if (indexes != null) {
            for (Index index : indexes) {
                if (index.getIndexType() == IndexDef.IndexType.INVERTED) {
                    List<String> columns = index.getColumns();
                    if (columns != null && !columns.isEmpty() && left.getColumnName().equals(columns.get(0))) {
                        invertedIndexParser = index.getInvertedIndexParser();
                        invertedIndexParserMode = index.getInvertedIndexParserMode();
                        invertedIndexCharFilter = index.getInvertedIndexCharFilter();
                        break;
                    }
                }
            }
        }

        MatchPredicate.Operator op = match.op();
        MatchPredicate matchPredicate = new MatchPredicate(op, match.left().accept(this, context),
                match.right().accept(this, context), match.getDataType().toCatalogDataType(),
                NullableMode.DEPEND_ON_ARGUMENT, invertedIndexParser, invertedIndexParserMode,
                invertedIndexCharFilter);
        matchPredicate.setNullableFromNereids(match.nullable());
        return matchPredicate;
    }

    @Override
    public Expr visitNullSafeEqual(NullSafeEqual nullSafeEqual, PlanTranslatorContext context) {
        BinaryPredicate eq = new BinaryPredicate(Operator.EQ_FOR_NULL,
                nullSafeEqual.child(0).accept(this, context),
                nullSafeEqual.child(1).accept(this, context),
                nullSafeEqual.getDataType().toCatalogDataType(),
                NullableMode.ALWAYS_NOT_NULLABLE);
        eq.setNullableFromNereids(nullSafeEqual.nullable());
        return eq;
    }

    @Override
    public Expr visitNot(Not not, PlanTranslatorContext context) {
        if (not.child() instanceof InPredicate) {
            InPredicate inPredicate = (InPredicate) not.child();
            List<Expr> inList = inPredicate.getOptions().stream()
                    .map(e -> translate(e, context))
                    .collect(Collectors.toList());
            boolean allConstant = inPredicate.getOptions().stream().allMatch(Expression::isConstant);
            org.apache.doris.analysis.InPredicate in = new org.apache.doris.analysis.InPredicate(
                    inPredicate.getCompareExpr().accept(this, context),
                    inList, true, allConstant);
            in.setNullableFromNereids(inPredicate.nullable());
            return in;
        } else if (not.child() instanceof EqualTo) {
            EqualTo equalTo = (EqualTo) not.child();
            BinaryPredicate ne = new BinaryPredicate(Operator.NE,
                    equalTo.child(0).accept(this, context),
                    equalTo.child(1).accept(this, context),
                    equalTo.getDataType().toCatalogDataType(),
                    NullableMode.DEPEND_ON_ARGUMENT);
            ne.setNullableFromNereids(equalTo.nullable());
            return ne;
        } else if (not.child() instanceof InSubquery || not.child() instanceof Exists) {
            return new BoolLiteral(true);
        } else if (not.child() instanceof IsNull) {
            IsNullPredicate isNull = new IsNullPredicate(
                    ((IsNull) not.child()).child().accept(this, context), true, true);
            isNull.setNullableFromNereids(not.child().nullable());
            return isNull;
        } else {
            CompoundPredicate cp = new CompoundPredicate(CompoundPredicate.Operator.NOT,
                    not.child(0).accept(this, context), null);
            cp.setNullableFromNereids(not.nullable());
            return cp;
        }
    }

    @Override
    public Expr visitSlotReference(SlotReference slotReference, PlanTranslatorContext context) {
        return context.findSlotRef(slotReference.getExprId());
    }

    @Override
    public Expr visitArrayItemSlot(ArrayItemReference.ArrayItemSlot arrayItemSlot, PlanTranslatorContext context) {
        return context.findColumnRef(arrayItemSlot.getExprId());
    }

    @Override
    public Expr visitArrayItemReference(ArrayItemReference arrayItemReference, PlanTranslatorContext context) {
        return context.findColumnRef(arrayItemReference.getExprId());
    }

    @Override
    public Expr visitMarkJoinReference(MarkJoinSlotReference markJoinSlotReference, PlanTranslatorContext context) {
        return markJoinSlotReference.isExistsHasAgg()
                ? new BoolLiteral(true) : context.findSlotRef(markJoinSlotReference.getExprId());
    }

    @Override
    public Expr visitLiteral(Literal literal, PlanTranslatorContext context) {
        return literal.toLegacyLiteral();
    }

    @Override
    public Expr visitNullLiteral(NullLiteral nullLiteral, PlanTranslatorContext context) {
        org.apache.doris.analysis.NullLiteral nullLit = new org.apache.doris.analysis.NullLiteral();
        nullLit.setType(nullLiteral.getDataType().toCatalogDataType());
        return nullLit;
    }

    @Override
    public Expr visitAnd(And and, PlanTranslatorContext context) {
        org.apache.doris.analysis.CompoundPredicate cp = new org.apache.doris.analysis.CompoundPredicate(
                org.apache.doris.analysis.CompoundPredicate.Operator.AND,
                and.child(0).accept(this, context),
                and.child(1).accept(this, context));
        cp.setNullableFromNereids(and.nullable());
        return cp;
    }

    @Override
    public Expr visitOr(Or or, PlanTranslatorContext context) {
        org.apache.doris.analysis.CompoundPredicate cp = new org.apache.doris.analysis.CompoundPredicate(
                org.apache.doris.analysis.CompoundPredicate.Operator.OR,
                or.child(0).accept(this, context),
                or.child(1).accept(this, context));
        cp.setNullableFromNereids(or.nullable());
        return cp;
    }

    @Override
    public Expr visitCaseWhen(CaseWhen caseWhen, PlanTranslatorContext context) {
        List<CaseWhenClause> caseWhenClauses = new ArrayList<>();
        for (WhenClause whenClause : caseWhen.getWhenClauses()) {
            caseWhenClauses.add(new CaseWhenClause(
                    whenClause.left().accept(this, context),
                    whenClause.right().accept(this, context)
            ));
        }
        Expr elseExpr = null;
        Optional<Expression> defaultValue = caseWhen.getDefaultValue();
        if (defaultValue.isPresent()) {
            elseExpr = defaultValue.get().accept(this, context);
        }
        CaseExpr caseExpr = new CaseExpr(caseWhenClauses, elseExpr);
        caseExpr.setNullableFromNereids(caseWhen.nullable());
        return caseExpr;
    }

    @Override
    public Expr visitCast(Cast cast, PlanTranslatorContext context) {
        // left child of cast is expression, right child of cast is target type
        CastExpr castExpr = new CastExpr(cast.getDataType().toCatalogDataType(),
                cast.child().accept(this, context), null);
        castExpr.setNullableFromNereids(cast.nullable());
        return castExpr;
    }

    @Override
    public Expr visitInPredicate(InPredicate inPredicate, PlanTranslatorContext context) {
        List<Expr> inList = inPredicate.getOptions().stream()
                .map(e -> e.accept(this, context))
                .collect(Collectors.toList());
        boolean allConstant = inPredicate.getOptions().stream().allMatch(Expression::isConstant);
        org.apache.doris.analysis.InPredicate in = new org.apache.doris.analysis.InPredicate(
                inPredicate.getCompareExpr().accept(this, context),
                inList, false, allConstant);
        in.setNullableFromNereids(inPredicate.nullable());
        return in;
    }

    @Override
    public Expr visitWindowFunction(WindowFunction function, PlanTranslatorContext context) {
        // translate argument types from DataType to Type
        List<Expr> catalogArguments = function.getArguments()
                .stream()
                .map(arg -> arg.accept(this, context))
                .collect(ImmutableList.toImmutableList());
        ImmutableList<Type> argTypes = catalogArguments.stream()
                .map(arg -> arg.getType())
                .collect(ImmutableList.toImmutableList());

        // translate argument from List<Expression> to FunctionParams
        List<Expr> arguments = function.getArguments()
                .stream()
                .map(arg -> new SlotRef(arg.getDataType().toCatalogDataType(), arg.nullable()))
                .collect(ImmutableList.toImmutableList());
        FunctionParams windowFnParams = new FunctionParams(false, arguments);

        // translate isNullable()
        NullableMode nullableMode = function.nullable()
                ? NullableMode.ALWAYS_NULLABLE
                : NullableMode.ALWAYS_NOT_NULLABLE;

        // translate function from WindowFunction to old AggregateFunction
        boolean isAnalyticFunction = true;
        org.apache.doris.catalog.AggregateFunction catalogFunction = new org.apache.doris.catalog.AggregateFunction(
                new FunctionName(function.getName()), argTypes,
                function.getDataType().toCatalogDataType(),
                function.getDataType().toCatalogDataType(),
                function.hasVarArguments(),
                null, "", "", null, "",
                null, "", null, false,
                isAnalyticFunction, false, TFunctionBinaryType.BUILTIN,
                true, true, nullableMode
        );

        // generate FunctionCallExpr
        boolean isMergeFn = false;
        FunctionCallExpr functionCallExpr =
                new FunctionCallExpr(catalogFunction, windowFnParams, windowFnParams, isMergeFn, catalogArguments);
        functionCallExpr.setIsAnalyticFnCall(true);
        functionCallExpr.setNullableFromNereids(function.nullable());
        return functionCallExpr;

    }

    @Override
    public Expr visitLambda(Lambda lambda, PlanTranslatorContext context) {
        Expr func = lambda.getLambdaFunction().accept(this, context);
        List<Expr> arguments = lambda.getLambdaArguments().stream().map(e -> e.accept(this, context))
                .collect(Collectors.toList());
        LambdaFunctionExpr functionExpr = new LambdaFunctionExpr(func, lambda.getLambdaArgumentNames(), arguments);
        functionExpr.setNullableFromNereids(lambda.nullable());
        return functionExpr;
    }

    @Override
    public Expr visitArrayMap(ArrayMap arrayMap, PlanTranslatorContext context) {
        Lambda lambda = (Lambda) arrayMap.child(0);
        List<Expr> arguments = new ArrayList<>(arrayMap.children().size());
        arguments.add(null);
        int columnId = 0;
        for (ArrayItemReference arrayItemReference : lambda.getLambdaArguments()) {
            String argName = arrayItemReference.getName();
            Expr expr = arrayItemReference.getArrayExpression().accept(this, context);
            arguments.add(expr);

            ColumnRefExpr column = new ColumnRefExpr();
            column.setName(argName);
            column.setColumnId(columnId);
            column.setNullable(true);
            column.setType(((ArrayType) expr.getType()).getItemType());
            context.addExprIdColumnRefPair(arrayItemReference.getExprId(), column);
            columnId += 1;
        }

        List<Type> argTypes = arrayMap.getArguments().stream()
                .map(Expression::getDataType)
                .map(DataType::toCatalogDataType)
                .collect(Collectors.toList());
        lambda.getLambdaArguments().stream()
                .map(ArrayItemReference::getArrayExpression)
                .map(Expression::getDataType)
                .map(DataType::toCatalogDataType)
                .forEach(argTypes::add);
        NullableMode nullableMode = arrayMap.nullable()
                ? NullableMode.ALWAYS_NULLABLE
                : NullableMode.ALWAYS_NOT_NULLABLE;
        org.apache.doris.catalog.Function catalogFunction = new Function(
                new FunctionName(arrayMap.getName()), argTypes,
                ArrayType.create(lambda.getRetType().toCatalogDataType(), true),
                true, true, nullableMode);

        // create catalog FunctionCallExpr without analyze again
        Expr lambdaBody = visitLambda(lambda, context);
        arguments.set(0, lambdaBody);
        LambdaFunctionCallExpr functionCallExpr =
                new LambdaFunctionCallExpr(catalogFunction, new FunctionParams(false, arguments));
        functionCallExpr.setNullableFromNereids(arrayMap.nullable());
        return functionCallExpr;
    }

    @Override
    public Expr visitScalarFunction(ScalarFunction function, PlanTranslatorContext context) {
        List<Expr> arguments = function.getArguments().stream()
                .map(arg -> arg.accept(this, context))
                .collect(Collectors.toList());

        List<Type> argTypes = function.getArguments().stream()
                .map(Expression::getDataType)
                .map(DataType::toCatalogDataType)
                .collect(Collectors.toList());

        NullableMode nullableMode = function.nullable()
                ? NullableMode.ALWAYS_NULLABLE
                : NullableMode.ALWAYS_NOT_NULLABLE;

        org.apache.doris.catalog.ScalarFunction catalogFunction = new org.apache.doris.catalog.ScalarFunction(
                new FunctionName(function.getName()), argTypes,
                function.getDataType().toCatalogDataType(), function.hasVarArguments(),
                "", TFunctionBinaryType.BUILTIN, true, true, nullableMode);

        FunctionCallExpr functionCallExpr;
        // create catalog FunctionCallExpr without analyze again
        if (function instanceof HighOrderFunction) {
            functionCallExpr = new LambdaFunctionCallExpr(catalogFunction, new FunctionParams(false, arguments));
        } else {
            functionCallExpr = new FunctionCallExpr(catalogFunction, new FunctionParams(false, arguments));
        }
        functionCallExpr.setNullableFromNereids(function.nullable());
        return functionCallExpr;
    }

    @Override
    public Expr visitAggregateExpression(AggregateExpression aggregateExpression, PlanTranslatorContext context) {
        // aggFnArguments is used to build TAggregateExpr.param_types, so backend can find the aggregate function
        List<Expr> aggFnArguments = aggregateExpression.getFunction().children()
                .stream()
                .map(arg -> new SlotRef(arg.getDataType().toCatalogDataType(), arg.nullable()))
                .collect(ImmutableList.toImmutableList());

        Expression child = aggregateExpression.child();
        List<Expression> currentPhaseArguments = child instanceof AggregateFunction
                ? child.children()
                : aggregateExpression.children();
        return translateAggregateFunction(aggregateExpression.getFunction(),
                currentPhaseArguments, aggFnArguments, aggregateExpression.getAggregateParam(), context);
    }

    @Override
    public Expr visitTableGeneratingFunction(TableGeneratingFunction function,
            PlanTranslatorContext context) {
        List<Expr> arguments = function.getArguments()
                .stream()
                .map(arg -> arg.accept(this, context))
                .collect(Collectors.toList());
        List<Type> argTypes = function.expectedInputTypes().stream()
                .map(DataType::toCatalogDataType)
                .collect(Collectors.toList());

        NullableMode nullableMode = function.nullable()
                ? NullableMode.ALWAYS_NULLABLE
                : NullableMode.ALWAYS_NOT_NULLABLE;

        org.apache.doris.catalog.ScalarFunction catalogFunction = new org.apache.doris.catalog.ScalarFunction(
                new FunctionName(function.getName()), argTypes,
                function.getDataType().toCatalogDataType(), function.hasVarArguments(),
                "", TFunctionBinaryType.BUILTIN, true, true, nullableMode);

        // create catalog FunctionCallExpr without analyze again
        FunctionCallExpr functionCallExpr =
                new FunctionCallExpr(catalogFunction, new FunctionParams(false, arguments));
        functionCallExpr.setNullableFromNereids(function.nullable());
        return functionCallExpr;
    }

    @Override
    public Expr visitBinaryArithmetic(BinaryArithmetic binaryArithmetic, PlanTranslatorContext context) {
        NullableMode nullableMode = NullableMode.DEPEND_ON_ARGUMENT;
        if (binaryArithmetic instanceof AlwaysNullable) {
            nullableMode = NullableMode.ALWAYS_NULLABLE;
        } else if (binaryArithmetic instanceof AlwaysNotNullable) {
            nullableMode = NullableMode.ALWAYS_NOT_NULLABLE;
        }
        ArithmeticExpr arithmeticExpr = new ArithmeticExpr(binaryArithmetic.getLegacyOperator(),
                binaryArithmetic.child(0).accept(this, context),
                binaryArithmetic.child(1).accept(this, context),
                binaryArithmetic.getDataType().toCatalogDataType(), nullableMode);
        arithmeticExpr.setNullableFromNereids(binaryArithmetic.nullable());
        return arithmeticExpr;
    }

    @Override
    public Expr visitUnaryArithmetic(UnaryArithmetic unaryArithmetic, PlanTranslatorContext context) {
        ArithmeticExpr arithmeticExpr = new ArithmeticExpr(unaryArithmetic.getLegacyOperator(),
                unaryArithmetic.child().accept(this, context), null,
                unaryArithmetic.getDataType().toCatalogDataType(), NullableMode.DEPEND_ON_ARGUMENT);
        arithmeticExpr.setNullableFromNereids(unaryArithmetic.nullable());
        return arithmeticExpr;
    }

    @Override
    public Expr visitTimestampArithmetic(TimestampArithmetic arithmetic, PlanTranslatorContext context) {
        Preconditions.checkNotNull(arithmetic.getFuncName(),
                "funcName in TimestampArithmetic should not be null");
        NullableMode nullableMode = NullableMode.ALWAYS_NULLABLE;
        if (arithmetic.children().stream().anyMatch(e -> e.getDataType().isDateV2LikeType())) {
            nullableMode = NullableMode.DEPEND_ON_ARGUMENT;
        }
        TimestampArithmeticExpr timestampArithmeticExpr = new TimestampArithmeticExpr(
                arithmetic.getFuncName(), arithmetic.getOp(),
                arithmetic.left().accept(this, context), arithmetic.right().accept(this, context),
                arithmetic.getTimeUnit().toString(), arithmetic.getDataType().toCatalogDataType(),
                nullableMode);
        timestampArithmeticExpr.setNullableFromNereids(arithmetic.nullable());
        return timestampArithmeticExpr;
    }

    @Override
    public Expr visitVirtualReference(VirtualSlotReference virtualSlotReference, PlanTranslatorContext context) {
        return context.findSlotRef(virtualSlotReference.getExprId());
    }

    @Override
    public Expr visitIsNull(IsNull isNull, PlanTranslatorContext context) {
        IsNullPredicate isNullPredicate = new IsNullPredicate(isNull.child().accept(this, context), false, true);
        isNullPredicate.setNullableFromNereids(isNull.nullable());
        return isNullPredicate;
    }

    @Override
    public Expr visitStateCombinator(StateCombinator combinator, PlanTranslatorContext context) {
        List<Expr> arguments = combinator.getArguments().stream().map(arg -> arg.accept(this, context))
                .collect(Collectors.toList());
        return Function.convertToStateCombinator(
                new FunctionCallExpr(visitAggregateFunction(combinator.getNestedFunction(), context).getFn(),
                        new FunctionParams(false, arguments)));
    }

    @Override
    public Expr visitMergeCombinator(MergeCombinator combinator, PlanTranslatorContext context) {
        List<Expr> arguments = combinator.children().stream()
                .map(arg -> new SlotRef(arg.getDataType().toCatalogDataType(), arg.nullable()))
                .collect(ImmutableList.toImmutableList());
        return Function.convertToMergeCombinator(
                new FunctionCallExpr(visitAggregateFunction(combinator.getNestedFunction(), context).getFn(),
                        new FunctionParams(false, arguments)));
    }

    @Override
    public Expr visitUnionCombinator(UnionCombinator combinator, PlanTranslatorContext context) {
        List<Expr> arguments = combinator.children().stream()
                .map(arg -> new SlotRef(arg.getDataType().toCatalogDataType(), arg.nullable()))
                .collect(ImmutableList.toImmutableList());
        return Function.convertToUnionCombinator(
                new FunctionCallExpr(visitAggregateFunction(combinator.getNestedFunction(), context).getFn(),
                        new FunctionParams(false, arguments)));
    }

    @Override
    public Expr visitForEachCombinator(ForEachCombinator combinator, PlanTranslatorContext context) {
        List<Expr> arguments = combinator.children().stream()
                .map(arg -> new SlotRef(arg.getDataType().toCatalogDataType(), arg.nullable()))
                .collect(ImmutableList.toImmutableList());
        return Function.convertForEachCombinator(
                new FunctionCallExpr(visitAggregateFunction(combinator.getNestedFunction(), context).getFn(),
                        new FunctionParams(false, arguments)));
    }

    @Override
    public Expr visitAggregateFunction(AggregateFunction function, PlanTranslatorContext context) {
        List<Expr> arguments = function.children().stream()
                .map(arg -> new SlotRef(arg.getDataType().toCatalogDataType(), arg.nullable()))
                .collect(ImmutableList.toImmutableList());
        org.apache.doris.catalog.AggregateFunction catalogFunction = new org.apache.doris.catalog.AggregateFunction(
                new FunctionName(function.getName()),
                function.getArguments().stream().filter(arg -> !(arg instanceof OrderExpression))
                        .map(arg -> arg.getDataType().toCatalogDataType()).collect(ImmutableList.toImmutableList()),
                function.getDataType().toCatalogDataType(), function.getIntermediateTypes().toCatalogDataType(),
                function.hasVarArguments(), null, "", "", null, "", null, "", null, false, false, false,
                TFunctionBinaryType.BUILTIN, true, true,
                function.nullable() ? NullableMode.ALWAYS_NULLABLE : NullableMode.ALWAYS_NOT_NULLABLE);

        FunctionCallExpr functionCallExpr = new FunctionCallExpr(catalogFunction,
                new FunctionParams(function.isDistinct(), arguments));
        functionCallExpr.setNullableFromNereids(function.nullable());
        return functionCallExpr;
    }

    @Override
    public Expr visitJavaUdf(JavaUdf udf, PlanTranslatorContext context) {
        FunctionParams exprs = new FunctionParams(udf.children().stream()
                .map(expression -> expression.accept(this, context))
                .collect(Collectors.toList()));
        FunctionCallExpr functionCallExpr = new FunctionCallExpr(udf.getCatalogFunction(), exprs);
        functionCallExpr.setNullableFromNereids(udf.nullable());
        return functionCallExpr;
    }

    @Override
    public Expr visitJavaUdtf(JavaUdtf udf, PlanTranslatorContext context) {
        FunctionParams exprs = new FunctionParams(udf.children().stream()
                .map(expression -> expression.accept(this, context))
                .collect(Collectors.toList()));
        FunctionCallExpr functionCallExpr = new FunctionCallExpr(udf.getCatalogFunction(), exprs);
        functionCallExpr.setNullableFromNereids(udf.nullable());
        return functionCallExpr;
    }

    @Override
    public Expr visitJavaUdaf(JavaUdaf udaf, PlanTranslatorContext context) {
        FunctionParams exprs = new FunctionParams(udaf.isDistinct(), udaf.children().stream()
                .map(expression -> expression.accept(this, context))
                .collect(Collectors.toList()));
        FunctionCallExpr functionCallExpr = new FunctionCallExpr(udaf.getCatalogFunction(), exprs);
        functionCallExpr.setNullableFromNereids(udaf.nullable());
        return functionCallExpr;
    }

    // TODO: Supports for `distinct`
    private Expr translateAggregateFunction(AggregateFunction function,
            List<Expression> currentPhaseArguments, List<Expr> aggFnArguments,
            AggregateParam aggregateParam, PlanTranslatorContext context) {
        List<Expr> currentPhaseCatalogArguments = currentPhaseArguments
                .stream()
                .map(arg -> arg instanceof OrderExpression
                        ? translateOrderExpression((OrderExpression) arg, context).getExpr()
                        : arg.accept(this, context))
                .collect(ImmutableList.toImmutableList());

        List<OrderByElement> orderByElements = function.getArguments()
                .stream()
                .filter(arg -> arg instanceof OrderExpression)
                .map(arg -> translateOrderExpression((OrderExpression) arg, context))
                .collect(ImmutableList.toImmutableList());

        FunctionParams fnParams;
        FunctionParams aggFnParams;
        if (function instanceof Count && ((Count) function).isStar()) {
            if (currentPhaseCatalogArguments.isEmpty()) {
                // for explain display the label: count(*)
                fnParams = FunctionParams.createStarParam();
            } else {
                fnParams = new FunctionParams(function.isDistinct(), currentPhaseCatalogArguments);
            }
            aggFnParams = FunctionParams.createStarParam();
        } else {
            fnParams = new FunctionParams(function.isDistinct(), currentPhaseCatalogArguments);
            aggFnParams = new FunctionParams(function.isDistinct(), aggFnArguments);
        }

        org.apache.doris.catalog.AggregateFunction catalogFunction =
                (org.apache.doris.catalog.AggregateFunction) function.accept(this, context).getFn();

        boolean isMergeFn = aggregateParam.aggMode.consumeAggregateBuffer;
        // create catalog FunctionCallExpr without analyze again
        FunctionCallExpr functionCallExpr = new FunctionCallExpr(
                catalogFunction, fnParams, aggFnParams, isMergeFn, currentPhaseCatalogArguments);
        functionCallExpr.setOrderByElements(orderByElements);
        functionCallExpr.setNullableFromNereids(function.nullable());
        return functionCallExpr;
    }

    private OrderByElement translateOrderExpression(OrderExpression orderExpression, PlanTranslatorContext context) {
        Expr child = orderExpression.child().accept(this, context);
        return new OrderByElement(child, orderExpression.isAsc(), orderExpression.isNullFirst());
    }

    public static org.apache.doris.analysis.AssertNumRowsElement translateAssert(
            AssertNumRowsElement assertNumRowsElement) {
        return new org.apache.doris.analysis.AssertNumRowsElement(assertNumRowsElement.getDesiredNumOfRows(),
                assertNumRowsElement.getSubqueryString(), translateAssertion(assertNumRowsElement.getAssertion()));
    }

    private static org.apache.doris.analysis.AssertNumRowsElement.Assertion translateAssertion(
            AssertNumRowsElement.Assertion assertion) {
        switch (assertion) {
            case EQ:
                return Assertion.EQ;
            case NE:
                return Assertion.NE;
            case LT:
                return Assertion.LT;
            case LE:
                return Assertion.LE;
            case GT:
                return Assertion.GT;
            case GE:
                return Assertion.GE;
            default:
                throw new AnalysisException("UnSupported type: " + assertion);
        }
    }
}
