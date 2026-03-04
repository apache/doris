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
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LambdaFunctionCallExpr;
import org.apache.doris.analysis.LambdaFunctionExpr;
import org.apache.doris.analysis.MatchPredicate;
import org.apache.doris.analysis.OrderByElement;
import org.apache.doris.analysis.SearchPredicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TryCastExpr;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Pair;
import org.apache.doris.dictionary.Dictionary;
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
import org.apache.doris.nereids.trees.expressions.SearchExpression;
import org.apache.doris.nereids.trees.expressions.SessionVarGuardExpr;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.TryCast;
import org.apache.doris.nereids.trees.expressions.UnaryArithmetic;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullLiteral;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.NotNullableAggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.combinator.ForEachCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.MergeCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.StateCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.UnionCombinator;
import org.apache.doris.nereids.trees.expressions.functions.generator.TableGeneratingFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArraySort;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DictGet;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DictGetMany;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdaf;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdf;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdtf;
import org.apache.doris.nereids.trees.expressions.functions.udf.PythonUdaf;
import org.apache.doris.nereids.trees.expressions.functions.udf.PythonUdf;
import org.apache.doris.nereids.trees.expressions.functions.udf.PythonUdtf;
import org.apache.doris.nereids.trees.expressions.functions.window.WindowFunction;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.thrift.TDictFunction;
import org.apache.doris.thrift.TFunctionBinaryType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
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
                equalTo.nullable());
        return eq;
    }

    @Override
    public Expr visitGreaterThan(GreaterThan greaterThan, PlanTranslatorContext context) {
        BinaryPredicate gt = new BinaryPredicate(Operator.GT,
                greaterThan.child(0).accept(this, context),
                greaterThan.child(1).accept(this, context),
                greaterThan.getDataType().toCatalogDataType(),
                greaterThan.nullable());
        return gt;
    }

    @Override
    public Expr visitGreaterThanEqual(GreaterThanEqual greaterThanEqual, PlanTranslatorContext context) {
        BinaryPredicate ge = new BinaryPredicate(Operator.GE,
                greaterThanEqual.child(0).accept(this, context),
                greaterThanEqual.child(1).accept(this, context),
                greaterThanEqual.getDataType().toCatalogDataType(),
                greaterThanEqual.nullable());
        return ge;
    }

    @Override
    public Expr visitLessThan(LessThan lessThan, PlanTranslatorContext context) {
        BinaryPredicate lt = new BinaryPredicate(Operator.LT,
                lessThan.child(0).accept(this, context),
                lessThan.child(1).accept(this, context),
                lessThan.getDataType().toCatalogDataType(),
                lessThan.nullable());
        return lt;
    }

    @Override
    public Expr visitLessThanEqual(LessThanEqual lessThanEqual, PlanTranslatorContext context) {
        BinaryPredicate le = new BinaryPredicate(Operator.LE,
                lessThanEqual.child(0).accept(this, context),
                lessThanEqual.child(1).accept(this, context),
                lessThanEqual.getDataType().toCatalogDataType(),
                lessThanEqual.nullable());
        return le;
    }

    private OlapTable getOlapTableDirectly(SlotReference slot) {
        return slot.getOriginalTable()
               .filter(OlapTable.class::isInstance)
               .map(OlapTable.class::cast)
               .orElse(null);
    }

    @Override
    public Expr visitElementAt(ElementAt elementAt, PlanTranslatorContext context) {
        return visitScalarFunction(elementAt, context);
    }

    @Override
    public Expr visitMatch(Match match, PlanTranslatorContext context) {
        // Get the first slot from match's left expr
        SlotReference slot = match.getInputSlots().stream()
                        .findFirst()
                        .filter(SlotReference.class::isInstance)
                        .map(SlotReference.class::cast)
                        .orElseThrow(() -> new AnalysisException(
                                    "No SlotReference found in Match, SQL is " + match.toSql()));

        Column column = slot.getOriginalColumn()
                        .orElseThrow(() -> new AnalysisException(
                                    "SlotReference in Match failed to get Column, SQL is " + match.toSql()));

        OlapTable olapTbl = getOlapTableDirectly(slot);
        if (olapTbl == null) {
            throw new AnalysisException("SlotReference in Match failed to get OlapTable, SQL is " + match.toSql());
        }

        String analyzer = match.getAnalyzer().orElse(null);
        Index invertedIndex = olapTbl.getInvertedIndex(column, slot.getSubPath(), analyzer);
        if (analyzer != null && invertedIndex == null) {
            throw new AnalysisException("No inverted index found for analyzer '" + analyzer
                    + "' on column " + column.getName());
        }

        MatchPredicate.Operator op = match.op();
        MatchPredicate matchPredicate = new MatchPredicate(op, match.left().accept(this, context),
                match.right().accept(this, context), match.getDataType().toCatalogDataType(),
                NullableMode.DEPEND_ON_ARGUMENT, invertedIndex, match.nullable(), analyzer);
        return matchPredicate;
    }

    @Override
    public Expr visitNullSafeEqual(NullSafeEqual nullSafeEqual, PlanTranslatorContext context) {
        BinaryPredicate eq = new BinaryPredicate(Operator.EQ_FOR_NULL,
                nullSafeEqual.child(0).accept(this, context),
                nullSafeEqual.child(1).accept(this, context),
                nullSafeEqual.getDataType().toCatalogDataType(),
                nullSafeEqual.nullable());
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
                    inList, true, allConstant, inPredicate.nullable());
            return in;
        } else if (not.child() instanceof EqualTo) {
            EqualTo equalTo = (EqualTo) not.child();
            return new BinaryPredicate(Operator.NE,
                    equalTo.child(0).accept(this, context),
                    equalTo.child(1).accept(this, context),
                    equalTo.getDataType().toCatalogDataType(),
                    equalTo.nullable());
        } else if (not.child() instanceof InSubquery || not.child() instanceof Exists) {
            return new BoolLiteral(true);
        } else if (not.child() instanceof IsNull) {
            IsNullPredicate isNull = new IsNullPredicate(
                    ((IsNull) not.child()).child().accept(this, context), true);
            return isNull;
        } else {
            CompoundPredicate cp = new CompoundPredicate(CompoundPredicate.Operator.NOT,
                    not.child(0).accept(this, context), null, not.nullable());
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
        Expr lit = literal.toLegacyLiteral();
        return lit;
    }

    private static class Frame {
        int low;
        int high;
        CompoundPredicate.Operator op;
        boolean processed;

        Frame(int low, int high, CompoundPredicate.Operator op) {
            this.low = low;
            this.high = high;
            this.op = op;
            this.processed = false;
        }
    }

    private boolean computeCompoundPredicateNullable(Expr left, Expr right) {
        return left.isNullable() || right.isNullable();
    }

    private Expr toBalancedTree(int low, int high, List<Expr> children,
            CompoundPredicate.Operator op) {
        Deque<Frame> stack = new ArrayDeque<>();
        Deque<Expr> results = new ArrayDeque<>();

        stack.push(new Frame(low, high, op));

        while (!stack.isEmpty()) {
            Frame currentFrame = stack.peek();

            if (!currentFrame.processed) {
                int l = currentFrame.low;
                int h = currentFrame.high;
                int diff = h - l;

                if (diff == 0) {
                    results.push(children.get(l));
                    stack.pop();
                } else if (diff == 1) {
                    Expr left = children.get(l);
                    Expr right = children.get(h);
                    CompoundPredicate cp = new CompoundPredicate(op, left, right,
                            computeCompoundPredicateNullable(left, right));
                    results.push(cp);
                    stack.pop();
                } else {
                    int mid = l + (h - l) / 2;

                    currentFrame.processed = true;

                    stack.push(new Frame(mid + 1, h, op));
                    stack.push(new Frame(l, mid, op));
                }
            } else {
                stack.pop();
                if (results.size() >= 2) {
                    Expr right = results.pop();
                    Expr left = results.pop();
                    CompoundPredicate cp = new CompoundPredicate(currentFrame.op, left, right,
                            computeCompoundPredicateNullable(left, right));
                    results.push(cp);
                }
            }
        }
        return results.pop();
    }

    @Override
    public Expr visitAnd(And and, PlanTranslatorContext context) {
        List<Expr> children = and.children().stream().map(
                e -> e.accept(this, context)
        ).collect(Collectors.toList());
        CompoundPredicate cp = (CompoundPredicate) toBalancedTree(0, children.size() - 1,
                children, CompoundPredicate.Operator.AND);
        return cp;
    }

    @Override
    public Expr visitOr(Or or, PlanTranslatorContext context) {
        List<Expr> children = or.children().stream().map(
                e -> e.accept(this, context)
        ).collect(Collectors.toList());
        CompoundPredicate cp = (CompoundPredicate) toBalancedTree(0, children.size() - 1,
                children, CompoundPredicate.Operator.OR);
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
        CaseExpr caseExpr = new CaseExpr(caseWhenClauses, elseExpr, caseWhen.nullable());
        return caseExpr;
    }

    @Override
    public Expr visitCast(Cast cast, PlanTranslatorContext context) {
        // left child of cast is expression, right child of cast is target type
        CastExpr castExpr = new CastExpr(cast.getDataType().toCatalogDataType(),
                cast.child().accept(this, context), cast.nullable());
        castExpr.setImplicit(!cast.isExplicitType());
        return castExpr;
    }

    @Override
    public Expr visitTryCast(TryCast tryCast, PlanTranslatorContext context) {
        // left child of cast is expression, right child of cast is target type
        TryCastExpr tryCastExpr = new TryCastExpr(tryCast.getDataType().toCatalogDataType(),
                tryCast.child().accept(this, context), tryCast.nullable(), tryCast.originCastNullable());
        return tryCastExpr;
    }

    @Override
    public Expr visitInPredicate(InPredicate inPredicate, PlanTranslatorContext context) {
        List<Expr> inList = inPredicate.getOptions().stream()
                .map(e -> e.accept(this, context))
                .collect(Collectors.toList());
        boolean allConstant = inPredicate.getOptions().stream().allMatch(Expression::isConstant);
        org.apache.doris.analysis.InPredicate in = new org.apache.doris.analysis.InPredicate(
                inPredicate.getCompareExpr().accept(this, context),
                inList, false, allConstant, inPredicate.nullable());
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
        FunctionCallExpr functionCallExpr = new FunctionCallExpr(catalogFunction, windowFnParams, windowFnParams,
                isMergeFn, catalogArguments, function.nullable());
        functionCallExpr.setIsAnalyticFnCall(true);
        return functionCallExpr;

    }

    @Override
    public Expr visitLambda(Lambda lambda, PlanTranslatorContext context) {
        Expr func = lambda.getLambdaFunction().accept(this, context);
        List<Expr> arguments = lambda.getLambdaArguments().stream().map(e -> e.accept(this, context))
                .collect(Collectors.toList());
        LambdaFunctionExpr functionExpr = new LambdaFunctionExpr(
                func, lambda.getLambdaArgumentNames(), arguments, lambda.nullable());
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

            ColumnRefExpr column = new ColumnRefExpr(true);
            column.setName(argName);
            column.setColumnId(columnId);
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
        LambdaFunctionCallExpr functionCallExpr = new LambdaFunctionCallExpr(catalogFunction,
                new FunctionParams(false, arguments), arrayMap.nullable());
        return functionCallExpr;
    }

    @Override
    public Expr visitArraySort(ArraySort arraySort, PlanTranslatorContext context) {
        if (!(arraySort.child(0) instanceof Lambda)) {
            return visitScalarFunction(arraySort, context);
        }
        Lambda lambda = (Lambda) arraySort.child(0);
        List<Expr> arguments = new ArrayList<>(arraySort.children().size());
        arguments.add(null);

        // Construct the first column
        ArrayItemReference arrayItemReference = lambda.getLambdaArgument(0);
        String argName = arrayItemReference.getName();
        Expr expr = arrayItemReference.getArrayExpression().accept(this, context);
        arguments.add(expr);
        ColumnRefExpr column = new ColumnRefExpr(true);
        column.setName(argName);
        column.setColumnId(0);
        column.setType(((ArrayType) expr.getType()).getItemType());
        context.addExprIdColumnRefPair(arrayItemReference.getExprId(), column);

        // the second column here will not be used; it's just a placeholder.
        arrayItemReference = lambda.getLambdaArgument(1);
        ColumnRefExpr column2 = new ColumnRefExpr(column);
        column2.setColumnId(1);
        context.addExprIdColumnRefPair(arrayItemReference.getExprId(), column2);

        List<Type> argTypes = arraySort.getArguments().stream()
                .map(Expression::getDataType)
                .map(DataType::toCatalogDataType)
                .collect(Collectors.toList());
        // two slots are same, we only need one
        lambda.getLambdaArguments().stream().skip(1)
                .map(ArrayItemReference::getArrayExpression)
                .map(Expression::getDataType)
                .map(DataType::toCatalogDataType)
                .forEach(argTypes::add);
        NullableMode nullableMode = arraySort.nullable()
                ? NullableMode.ALWAYS_NULLABLE
                : NullableMode.ALWAYS_NOT_NULLABLE;
        Type itemType = ((ArrayType) arguments.get(1).getType()).getItemType();
        org.apache.doris.catalog.Function catalogFunction = new Function(
                new FunctionName(arraySort.getName()), argTypes,
                ArrayType.create(itemType, true),
                true, true, nullableMode);

        // create catalog FunctionCallExpr without analyze again
        Expr lambdaBody = visitLambda(lambda, context);
        arguments.set(0, lambdaBody);
        LambdaFunctionCallExpr functionCallExpr = new LambdaFunctionCallExpr(catalogFunction,
                new FunctionParams(false, arguments), arraySort.nullable());
        return functionCallExpr;
    }

    @Override
    public Expr visitDictGet(DictGet dictGet, PlanTranslatorContext context) {
        List<Expr> arguments = dictGet.getArguments().stream()
                .map(arg -> arg.accept(this, context))
                .collect(Collectors.toList());

        List<Type> argTypes = dictGet.getArguments().stream()
                .map(Expression::getDataType)
                .map(DataType::toCatalogDataType)
                .collect(Collectors.toList());

        Pair<FunctionSignature, Dictionary> sigAndDict = dictGet.customSignatureDict();
        FunctionSignature signature = sigAndDict.first;
        Dictionary dictionary = sigAndDict.second;

        org.apache.doris.catalog.ScalarFunction catalogFunction = new org.apache.doris.catalog.ScalarFunction(
                new FunctionName(dictGet.getName()), argTypes, signature.returnType.toCatalogDataType(),
                dictGet.hasVarArguments(), "", TFunctionBinaryType.BUILTIN, true, true,
                NullableMode.ALWAYS_NOT_NULLABLE);

        // set special fields
        TDictFunction dictFunction = new TDictFunction();
        dictFunction.setDictionaryId(dictionary.getId());
        dictFunction.setVersionId(dictionary.getVersion());
        catalogFunction.setDictFunction(dictFunction);

        // create catalog FunctionCallExpr without analyze again
        return new FunctionCallExpr(catalogFunction, new FunctionParams(false, arguments), dictGet.nullable());
    }

    @Override
    public Expr visitDictGetMany(DictGetMany dictGetMany, PlanTranslatorContext context) {
        List<Expr> arguments = dictGetMany.getArguments().stream().map(arg -> arg.accept(this, context))
                .collect(Collectors.toList());

        List<Type> argTypes = dictGetMany.getArguments().stream().map(Expression::getDataType)
                .map(DataType::toCatalogDataType).collect(Collectors.toList());

        Pair<FunctionSignature, Dictionary> sigAndDict = dictGetMany.customSignatureDict();
        FunctionSignature signature = sigAndDict.first;
        Dictionary dictionary = sigAndDict.second;

        org.apache.doris.catalog.ScalarFunction catalogFunction = new org.apache.doris.catalog.ScalarFunction(
                new FunctionName(dictGetMany.getName()), argTypes, signature.returnType.toCatalogDataType(),
                dictGetMany.hasVarArguments(), "", TFunctionBinaryType.BUILTIN, true, true,
                NullableMode.ALWAYS_NOT_NULLABLE);

        // set special fields
        TDictFunction dictFunction = new TDictFunction();
        dictFunction.setDictionaryId(dictionary.getId());
        dictFunction.setVersionId(dictionary.getVersion());
        catalogFunction.setDictFunction(dictFunction);

        // create catalog FunctionCallExpr without analyze again
        return new FunctionCallExpr(catalogFunction, new FunctionParams(false, arguments), dictGetMany.nullable());
    }

    @Override
    public Expr visitSearchExpression(SearchExpression searchExpression,
            PlanTranslatorContext context) {
        List<Expr> slotChildren = new ArrayList<>();
        List<Index> fieldIndexes = new ArrayList<>();

        // Convert slot reference children from Nereids to Analysis
        for (Expression slotExpr : searchExpression.getSlotChildren()) {
            Expr translatedSlot = slotExpr.accept(this, context);
            slotChildren.add(translatedSlot);

            // Look up the inverted index for each field (needed for variant subcolumn analyzer)
            Index invertedIndex = null;
            if (slotExpr instanceof SlotReference) {
                SlotReference slot = (SlotReference) slotExpr;
                OlapTable olapTbl = getOlapTableDirectly(slot);
                if (olapTbl != null) {
                    Column column = slot.getOriginalColumn().orElse(null);
                    if (column != null) {
                        invertedIndex = olapTbl.getInvertedIndex(column, slot.getSubPath());
                    }
                }
            }
            fieldIndexes.add(invertedIndex);
        }

        // Create SearchPredicate with proper slot children for BE "action on slot" detection
        SearchPredicate searchPredicate = new SearchPredicate(searchExpression.getDslString(),
                searchExpression.getQsPlan(), slotChildren, fieldIndexes,
                searchExpression.nullable());
        return searchPredicate;
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

        // create catalog FunctionCallExpr without analyze again
        return new FunctionCallExpr(catalogFunction, new FunctionParams(false, arguments), function.nullable());
    }

    @Override
    public Expr visitAggregateExpression(AggregateExpression aggregateExpression, PlanTranslatorContext context) {
        // aggFnArguments is used to build TAggregateExpr.param_types, so backend can find the aggregate function
        List<Expr> aggFnArguments = new ArrayList<>(aggregateExpression.getFunction().arity());
        for (Expression arg : aggregateExpression.getFunction().children()) {
            aggFnArguments.add(new SlotRef(arg.getDataType().toCatalogDataType(), arg.nullable()));
        }

        Expression child = aggregateExpression.child();
        List<Expression> currentPhaseArguments = child instanceof AggregateFunction
                ? child.children()
                : aggregateExpression.children();
        return translateAggregateFunction(aggregateExpression.getFunction(),
                currentPhaseArguments, aggFnArguments, aggregateExpression.getAggregateParam(), context);
    }

    @Override
    public Expr visitSessionVarGuardExpr(SessionVarGuardExpr sessionVarGuardExpr, PlanTranslatorContext context) {
        return sessionVarGuardExpr.child().accept(this, context);
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
        return new FunctionCallExpr(catalogFunction, new FunctionParams(false, arguments), function.nullable());
    }

    @Override
    public Expr visitBinaryArithmetic(BinaryArithmetic binaryArithmetic, PlanTranslatorContext context) {
        NullableMode nullableMode = NullableMode.DEPEND_ON_ARGUMENT;
        if (binaryArithmetic instanceof AlwaysNullable || binaryArithmetic instanceof PropagateNullLiteral) {
            nullableMode = NullableMode.ALWAYS_NULLABLE;
        } else if (binaryArithmetic instanceof AlwaysNotNullable) {
            nullableMode = NullableMode.ALWAYS_NOT_NULLABLE;
        }
        ArithmeticExpr arithmeticExpr = new ArithmeticExpr(binaryArithmetic.getLegacyOperator(),
                binaryArithmetic.child(0).accept(this, context),
                binaryArithmetic.child(1).accept(this, context),
                binaryArithmetic.getDataType().toCatalogDataType(), nullableMode, binaryArithmetic.nullable());
        return arithmeticExpr;
    }

    @Override
    public Expr visitUnaryArithmetic(UnaryArithmetic unaryArithmetic, PlanTranslatorContext context) {
        ArithmeticExpr arithmeticExpr = new ArithmeticExpr(unaryArithmetic.getLegacyOperator(),
                unaryArithmetic.child().accept(this, context), null,
                unaryArithmetic.getDataType().toCatalogDataType(),
                NullableMode.DEPEND_ON_ARGUMENT, unaryArithmetic.nullable());
        return arithmeticExpr;
    }

    @Override
    public Expr visitIsNull(IsNull isNull, PlanTranslatorContext context) {
        IsNullPredicate isNullPredicate = new IsNullPredicate(isNull.child().accept(this, context), false);
        return isNullPredicate;
    }

    @Override
    public Expr visitStateCombinator(StateCombinator combinator, PlanTranslatorContext context) {
        List<Expr> arguments = combinator.getArguments().stream()
                .map(arg -> {
                    Expr expr;
                    if (arg instanceof OrderExpression) {
                        expr = ((OrderExpression) arg).accept(this, context);
                    } else {
                        expr = arg.accept(this, context);
                    }
                    return expr;
                })
                .collect(Collectors.toList());
        boolean isReturnNullable = !(combinator.getNestedFunction() instanceof NotNullableAggregateFunction);
        FunctionCallExpr functionCallExpr = new FunctionCallExpr(
                visitAggregateFunction(combinator.getNestedFunction(), context).getFn(),
                new FunctionParams(false, arguments), isReturnNullable);
        return convertToStateCombinator(combinator.getName(), functionCallExpr,
                arguments.stream().map(Expr::getType).collect(Collectors.toList()),
                combinator.getArguments().stream().map(Expression::nullable).collect(Collectors.toList()),
                isReturnNullable);
    }

    private FunctionCallExpr convertToStateCombinator(String name, FunctionCallExpr fnCall,
            List<Type> argTypes, List<Boolean> argNullables,
            boolean returnNullable) {
        Function aggFunction = fnCall.getFn();
        List<Type> arguments = Arrays.asList(aggFunction.getArgs());
        org.apache.doris.catalog.ScalarFunction fn = new org.apache.doris.catalog.ScalarFunction(
                new FunctionName(name), arguments,
                Expr.createAggStateType(aggFunction.getFunctionName().getFunction(),
                        argTypes, argNullables, returnNullable),
                aggFunction.hasVarArgs(), aggFunction.isUserVisible());
        fn.setNullableMode(NullableMode.ALWAYS_NOT_NULLABLE);
        fn.setBinaryType(TFunctionBinaryType.AGG_STATE);
        return new FunctionCallExpr(fn, new FunctionParams(fnCall.getChildren()), false);
    }

    @Override
    public Expr visitMergeCombinator(MergeCombinator combinator, PlanTranslatorContext context) {
        List<Expr> arguments = combinator.children().stream()
                .map(arg -> new SlotRef(arg.getDataType().toCatalogDataType(), arg.nullable()))
                .collect(ImmutableList.toImmutableList());
        return convertToMergeCombinator(combinator.getName(),
                new FunctionCallExpr(visitAggregateFunction(combinator.getNestedFunction(), context).getFn(),
                        new FunctionParams(false, arguments), combinator.nullable()));
    }

    private FunctionCallExpr convertToMergeCombinator(String name, FunctionCallExpr fnCall) {
        Function aggFunction = fnCall.getFn();
        aggFunction.setName(new FunctionName(name));
        aggFunction.setArgs(Arrays.asList(fnCall.getChildren().get(0).getType()));
        aggFunction.setBinaryType(TFunctionBinaryType.AGG_STATE);
        return fnCall;
    }

    @Override
    public Expr visitUnionCombinator(UnionCombinator combinator, PlanTranslatorContext context) {
        List<Expr> arguments = combinator.children().stream()
                .map(arg -> new SlotRef(arg.getDataType().toCatalogDataType(), arg.nullable()))
                .collect(ImmutableList.toImmutableList());
        return convertToUnionCombinator(combinator.getName(),
                new FunctionCallExpr(visitAggregateFunction(combinator.getNestedFunction(), context).getFn(),
                        new FunctionParams(false, arguments), combinator.nullable()));
    }

    private FunctionCallExpr convertToUnionCombinator(String name, FunctionCallExpr fnCall) {
        Function aggFunction = fnCall.getFn();
        aggFunction.setName(new FunctionName(name));
        aggFunction.setArgs(Arrays.asList(fnCall.getChildren().get(0).getType()));
        aggFunction.setBinaryType(TFunctionBinaryType.AGG_STATE);
        aggFunction.setNullableMode(NullableMode.ALWAYS_NOT_NULLABLE);
        aggFunction.setReturnType(fnCall.getChildren().get(0).getType());
        fnCall.setType(fnCall.getChildren().get(0).getType());
        return fnCall;
    }

    @Override
    public Expr visitForEachCombinator(ForEachCombinator combinator, PlanTranslatorContext context) {
        List<Expr> arguments = combinator.children().stream()
                .map(arg -> new SlotRef(arg.getDataType().toCatalogDataType(), arg.nullable()))
                .collect(ImmutableList.toImmutableList());
        return convertForEachCombinator(combinator.getName(),
                new FunctionCallExpr(visitAggregateFunction(combinator.getNestedFunction(), context).getFn(),
                        new FunctionParams(false, arguments), combinator.nullable()));
    }

    private FunctionCallExpr convertForEachCombinator(String name, FunctionCallExpr fnCall) {
        Function aggFunction = fnCall.getFn();
        aggFunction.setName(new FunctionName(name + "v2"));
        List<Type> argTypes = new ArrayList();
        for (Type type : aggFunction.getArgs()) {
            argTypes.add(new ArrayType(type));
        }
        aggFunction.setArgs(argTypes);
        aggFunction.setReturnType(new ArrayType(aggFunction.getReturnType(), true));
        aggFunction.setNullableMode(NullableMode.ALWAYS_NULLABLE);
        return fnCall;
    }

    @Override
    public Expr visitAggregateFunction(AggregateFunction function, PlanTranslatorContext context) {
        List<Expr> arguments = Lists.newArrayListWithCapacity(function.arity());
        for (Expression arg : function.children()) {
            arguments.add(new SlotRef(arg.getDataType().toCatalogDataType(), arg.nullable()));
        }
        List<Type> argTypes = Lists.newArrayListWithCapacity(function.arity());
        for (Expression arg : function.getArguments()) {
            if (!(arg instanceof OrderExpression)) {
                argTypes.add(arg.getDataType().toCatalogDataType());
            }
        }
        org.apache.doris.catalog.AggregateFunction catalogFunction = new org.apache.doris.catalog.AggregateFunction(
                new FunctionName(function.getName()),
                argTypes,
                function.getDataType().toCatalogDataType(), function.getIntermediateTypes().toCatalogDataType(),
                function.hasVarArguments(), null, "", "", null, "", null, "", null, false, false, false,
                TFunctionBinaryType.BUILTIN, true, true,
                function.nullable() ? NullableMode.ALWAYS_NULLABLE : NullableMode.ALWAYS_NOT_NULLABLE);

        return new FunctionCallExpr(catalogFunction,
                new FunctionParams(function.isDistinct(), arguments), function.nullable());
    }

    @Override
    public Expr visitJavaUdf(JavaUdf udf, PlanTranslatorContext context) {
        FunctionParams exprs = new FunctionParams(udf.children().stream()
                .map(expression -> expression.accept(this, context))
                .collect(Collectors.toList()));
        return new FunctionCallExpr(udf.getCatalogFunction(), exprs, udf.nullable());
    }

    @Override
    public Expr visitJavaUdtf(JavaUdtf udf, PlanTranslatorContext context) {
        FunctionParams exprs = new FunctionParams(udf.children().stream()
                .map(expression -> expression.accept(this, context))
                .collect(Collectors.toList()));
        return new FunctionCallExpr(udf.getCatalogFunction(), exprs, udf.nullable());
    }

    @Override
    public Expr visitJavaUdaf(JavaUdaf udaf, PlanTranslatorContext context) {
        FunctionParams exprs = new FunctionParams(udaf.isDistinct(), udaf.children().stream()
                .map(expression -> expression.accept(this, context))
                .collect(Collectors.toList()));
        return new FunctionCallExpr(udaf.getCatalogFunction(), exprs, udaf.nullable());
    }

    @Override
    public Expr visitPythonUdf(PythonUdf udf, PlanTranslatorContext context) {
        FunctionParams exprs = new FunctionParams(udf.children().stream()
                .map(expression -> expression.accept(this, context))
                .collect(Collectors.toList()));
        return new FunctionCallExpr(udf.getCatalogFunction(), exprs, udf.nullable());
    }

    @Override
    public Expr visitPythonUdaf(PythonUdaf udaf, PlanTranslatorContext context) {
        FunctionParams exprs = new FunctionParams(udaf.isDistinct(), udaf.children().stream()
                .map(expression -> expression.accept(this, context))
                .collect(Collectors.toList()));
        return new FunctionCallExpr(udaf.getCatalogFunction(), exprs, udaf.nullable());
    }

    @Override
    public Expr visitPythonUdtf(PythonUdtf udtf, PlanTranslatorContext context) {
        FunctionParams exprs = new FunctionParams(udtf.children().stream()
                .map(expression -> expression.accept(this, context))
                .collect(Collectors.toList()));
        return new FunctionCallExpr(udtf.getCatalogFunction(), exprs, udtf.nullable());
    }

    // TODO: Supports for `distinct`
    private Expr translateAggregateFunction(AggregateFunction function,
            List<Expression> currentPhaseArguments, List<Expr> aggFnArguments,
            AggregateParam aggregateParam, PlanTranslatorContext context) {
        List<Expr> currentPhaseCatalogArguments = Lists.newArrayListWithCapacity(currentPhaseArguments.size());
        for (Expression arg : currentPhaseArguments) {
            if (arg instanceof OrderExpression) {
                currentPhaseCatalogArguments.add(translateOrderExpression((OrderExpression) arg, context).getExpr());
            } else {
                currentPhaseCatalogArguments.add(arg.accept(this, context));
            }
        }

        List<OrderByElement> orderByElements = Lists.newArrayListWithCapacity(function.getArguments().size());
        for (Expression arg : function.getArguments()) {
            if (arg instanceof OrderExpression) {
                orderByElements.add(translateOrderExpression((OrderExpression) arg, context));
            }
        }

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
        FunctionCallExpr functionCallExpr = new FunctionCallExpr(catalogFunction, fnParams, aggFnParams,
                isMergeFn, currentPhaseCatalogArguments, function.nullable());
        functionCallExpr.setOrderByElements(orderByElements);
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
