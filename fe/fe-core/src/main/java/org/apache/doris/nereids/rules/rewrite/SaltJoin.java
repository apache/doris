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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.hint.Hint.HintStatus;
import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeNumbers;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Current capabilities and limitations of SaltJoin rewrite handling:
 * - Supports single-side skew in INNER JOIN, NOT support double-side (both tables) skew
 * - Supports left table skew and NOT support right table skew in LEFT JOIN
 * - Supports right table skew and Not support left table skew in RIGHT JOIN
 *
 * INNER JOIN and LEFT JOIN use case:
 * Applicable when left table is skewed and right table is too large for broadcast
 *
 * Here are some examples in rewrite:
 * case1:
 * LogicalJoin(type:inner, t1.a=t2.a, hint:skew(t1.a(null,1,2)))
 *   +--LogicalOlapScan(t1)
 *   +--LogicalOlapScan(t2)
 * ->
 * LogicalJoin (type:inner, t1.a=t2.a and r1=r2)
 *   |--LogicalProject (t1.a, if (t1.a IN (1, 2), random(0, ExplodeFactor - 1), DEFAULT_SALT_VALUE) AS r1))
 *   |  +--LogicalFilter(t1.a is not null)
 *   |    +--LogicalOlapScan(t1)
 *   +--LogicalProject (projections: t2.a, if(explodeNumber IS NULL, DEFAULT_SALT_VALUE, explodeNumber) as r2)
 *     +--LogicalJoin (type=right_outer_join, t2.a = skewValue)
 *       |--LogicalGenerate(generators=[explode_numbers(ExplodeFactor)], generatorOutput=[explodeNumber])
 *       |  +--LogicalUnion(outputs=[skewValue], constantExprsList(1,2))
 *       +--LogicalFilter(t2.a is not null)
 *         +--LogicalOlapScan(t2)
 *
 * case2:
 * LogicalJoin(type:inner, t1.a=t2.a, hint:skew(t1.a(1,2)))
 *   +--LogicalOlapScan(t1)
 *   +--LogicalOlapScan(t2)
 * ->
 * LogicalJoin (type:inner, t1.a=t2.a and r1=r2)
 *   |--LogicalProject (t1.a, if (t1.a IN (1, 2), random(0, ExplodeFactor - 1), DEFAULT_SALT_VALUE) AS r1))
 *   | +--LogicalOlapScan(t1)
 *   +--LogicalProject (projections: t2.a, if(explodeNumber IS NULL, DEFAULT_SALT_VALUE, explodeNumber) as r2)
 *     +--LogicalJoin (type=right_outer_join, t2.a = skewValue)
 *       |--LogicalGenerate(generators=[explode_numbers(ExplodeFactor)], generatorOutput=[explodeNumber])
 *       |  +--LogicalUnion(outputs=[skewValue], constantExprsList(1,2))
 *       +--LogicalOlapScan(t2)
 *
 * case3: not optimize, because rows will not be output in join when join key is null
 * LogicalJoin(type:inner, t1.a=t2.a, hint:skew(t1.a(null)))
 *   |--LogicalOlapScan(t1)
 *   +--LogicalOlapScan(t2)
 * ->
 * LogicalJoin(type:inner, t1.a=t2.a)
 *     |--LogicalOlapScan(t1)
 *     +--LogicalOlapScan(t2)
 * */
public class SaltJoin extends OneRewriteRuleFactory {
    private static final String RANDOM_COLUMN_NAME_LEFT = "r1";
    private static final String RANDOM_COLUMN_NAME_RIGHT = "r2";
    private static final String SKEW_VALUE_COLUMN_NAME = "skewValue";
    private static final String EXPLODE_NUMBER_COLUMN_NAME = "explodeColumn";
    private static final int SALT_FACTOR = 4;
    private static final int DEFAULT_SALT_VALUE = 0;

    @Override
    public Rule build() {
        return logicalJoin()
                .when(join -> join.getJoinType().isOneSideOuterJoin() || join.getJoinType().isInnerJoin())
                .when(join -> join.getDistributeHint() != null && join.getDistributeHint().getSkewInfo() != null)
                .whenNot(LogicalJoin::isMarkJoin)
                .whenNot(join -> join.getDistributeHint().isSuccessInSkewRewrite())
                .thenApply(SaltJoin::transform).toRule(RuleType.SALT_JOIN);
    }

    private static Plan transform(MatchingContext<LogicalJoin<Plan, Plan>> ctx) {
        LogicalJoin<Plan, Plan> join = ctx.root;
        DistributeHint hint = join.getDistributeHint();
        if (hint.distributeType != DistributeType.SHUFFLE_RIGHT) {
            return null;
        }
        Expression skewExpr = hint.getSkewExpr();
        if (!skewExpr.isSlot()) {
            return null;
        }
        if ((join.getJoinType().isLeftOuterJoin() || join.getJoinType().isInnerJoin())
                && !join.left().getOutput().contains((Slot) skewExpr)
                || join.getJoinType().isRightOuterJoin() && !join.right().getOutput().contains((Slot) skewExpr)) {
            return null;
        }
        int factor = getSaltFactor(ctx);
        Optional<Expression> literalType = TypeCoercionUtils.characterLiteralTypeCoercion(String.valueOf(factor),
                TinyIntType.INSTANCE);
        if (!literalType.isPresent()) {
            return null;
        }
        Expression leftSkewExpr = null;
        Expression rightSkewExpr = null;
        Expression skewConjunct = null;
        for (Expression conjunct : join.getHashJoinConjuncts()) {
            if (skewExpr.equals(conjunct.child(0)) || skewExpr.equals(conjunct.child(1))) {
                if (join.left().getOutputSet().contains((Slot) conjunct.child(0))
                        && join.right().getOutputSet().contains((Slot) conjunct.child(1))) {
                    skewConjunct = conjunct;
                } else if (join.left().getOutputSet().contains((Slot) conjunct.child(1))
                        && join.right().getOutputSet().contains((Slot) conjunct.child(0))) {
                    skewConjunct = ((ComparisonPredicate) conjunct).commute();
                } else {
                    return null;
                }
                leftSkewExpr = skewConjunct.child(0);
                rightSkewExpr = skewConjunct.child(1);
                break;
            }
        }
        if (leftSkewExpr == null || rightSkewExpr == null) {
            return null;
        }
        List<Expression> skewValues = join.getDistributeHint().getSkewValues();
        Set<Expression> skewValuesSet = new HashSet<>(skewValues);
        List<Expression> expandSideValues = getSaltedSkewValuesForExpandSide(skewConjunct, skewValuesSet);
        List<Expression> skewSideValues = getSaltedSkewValuesForSkewSide(skewConjunct, skewValuesSet, join);
        if (skewSideValues.isEmpty()) {
            return null;
        }
        DataType type = literalType.get().getDataType();
        LogicalProject<Plan> rightProject;
        LogicalProject<Plan> leftProject;
        switch (join.getJoinType()) {
            case INNER_JOIN:
            case LEFT_OUTER_JOIN:
                leftProject = addRandomSlot(leftSkewExpr, skewSideValues, join.left(), factor, type,
                        ctx.statementContext);
                rightProject = expandSkewValueRows(rightSkewExpr, expandSideValues, join.right(), factor, type,
                        ctx.statementContext);
                break;
            case RIGHT_OUTER_JOIN:
                leftProject = expandSkewValueRows(leftSkewExpr, expandSideValues, join.left(), factor, type,
                        ctx.statementContext);
                rightProject = addRandomSlot(rightSkewExpr, skewSideValues, join.right(), factor, type,
                        ctx.statementContext);
                break;
            default:
                return null;
        }
        EqualTo saltEqual = new EqualTo(leftProject.getProjects().get(leftProject.getProjects().size() - 1).toSlot(),
                rightProject.getProjects().get(rightProject.getProjects().size() - 1).toSlot());
        saltEqual = (EqualTo) TypeCoercionUtils.processComparisonPredicate(saltEqual);
        ImmutableList.Builder<Expression> newHashJoinConjuncts = ImmutableList.builderWithExpectedSize(
                join.getHashJoinConjuncts().size() + 1);
        newHashJoinConjuncts.addAll(join.getHashJoinConjuncts());
        newHashJoinConjuncts.add(saltEqual);
        hint.setStatus(HintStatus.SUCCESS);
        hint.setSkewInfo(hint.getSkewInfo().withSuccessInSaltJoin(true));
        return new LogicalJoin<>(join.getJoinType(), newHashJoinConjuncts.build(), join.getOtherJoinConjuncts(),
                hint, leftProject, rightProject, JoinReorderContext.EMPTY);
    }

    // Add a project on top of originPlan, which includes all the original columns plus a case when column.
    private static LogicalProject<Plan> addRandomSlot(Expression skewExpr, List<Expression> skewValues,
            Plan originPlan, int factor, DataType type, StatementContext ctx) {
        List<Expression> skewValuesExceptNull = skewValues.stream().filter(value -> !(value instanceof NullLiteral))
                .collect(ImmutableList.toImmutableList());
        Expression ifCondition = getIfCondition(skewExpr, skewValues, skewValuesExceptNull);
        Random random = new Random(new BigIntLiteral(0), new BigIntLiteral(factor - 1));
        Cast cast = new Cast(random, type);
        If ifExpr = new If(ifCondition, cast, Literal.convertToTypedLiteral(DEFAULT_SALT_VALUE, type));
        ImmutableList.Builder<NamedExpression> namedExpressionsBuilder = ImmutableList.builderWithExpectedSize(
                originPlan.getOutput().size() + 1);
        namedExpressionsBuilder.addAll(originPlan.getOutput());
        namedExpressionsBuilder.add(new Alias(ifExpr, RANDOM_COLUMN_NAME_LEFT + ctx.generateColumnName()));
        return new LogicalProject<>(Utils.fastToImmutableList(namedExpressionsBuilder.build()), originPlan);
    }

    // if saltedSkewValues is [1,2,null], then ifCondition is "skewExpr in [1,2] or skewExpr is null"
    // if saltedSkewValues is [1,2], then ifCondition is "skewExpr in [1,2]"
    // if saltedSkewValues is [null], then ifCondition is "skewExpr is null"
    private static @Nullable Expression getIfCondition(Expression skewExpr, List<Expression> skewValues,
            List<Expression> skewValuesExceptNull) {
        IsNull isNull = null;
        InPredicate in = null;
        if (skewValuesExceptNull.size() < skewValues.size()) {
            isNull = new IsNull(skewExpr);
        }
        if (!skewValuesExceptNull.isEmpty()) {
            in = new InPredicate(skewExpr, skewValuesExceptNull);
        }
        Expression predicate = null;
        if (isNull != null && in != null) {
            predicate = new Or(in, isNull);
        } else if (isNull != null && in == null) {
            predicate = isNull;
        } else if (isNull == null && in != null) {
            predicate = in;
        }
        return predicate;
    }

    private static LogicalProject<Plan> expandSkewValueRows(Expression skewExpr, List<Expression> saltedSkewValues,
            Plan originPlan, int factor, DataType type, StatementContext ctx) {
        String randomColumnName = RANDOM_COLUMN_NAME_RIGHT + ctx.generateColumnName();
        if (saltedSkewValues.isEmpty()) {
            ImmutableList.Builder<NamedExpression> namedExpressionsBuilder = ImmutableList.builderWithExpectedSize(
                    originPlan.getOutput().size() + 1);
            namedExpressionsBuilder.addAll(originPlan.getOutput());
            namedExpressionsBuilder.add(new Alias(Literal.convertToTypedLiteral(DEFAULT_SALT_VALUE, type),
                    randomColumnName));
            return new LogicalProject<>(namedExpressionsBuilder.build(), originPlan);
        }
        // construct LogicalUnion and LogicalGenerate
        // if skew values are: 1 and null, the equal sql is:
        // select skewValue, explodeColumn from (select 1 as skewValue union all select null) as t11
        // lateral view explode_numbers(1000) tmp1 as explodeColumn
        ImmutableList.Builder<List<NamedExpression>> constantExprsList = ImmutableList.builderWithExpectedSize(
                saltedSkewValues.size());
        boolean saltedSkewValuesHasNull = false;
        for (Expression skewValue : saltedSkewValues) {
            constantExprsList.add(ImmutableList.of(new Alias(skewValue, SKEW_VALUE_COLUMN_NAME
                    + ctx.generateColumnName())));
            if (skewValue instanceof NullLiteral) {
                saltedSkewValuesHasNull = true;
            }
        }
        List<NamedExpression> outputs = ImmutableList.of(new SlotReference(
                SKEW_VALUE_COLUMN_NAME + ctx.generateColumnName(), skewExpr.getDataType(), saltedSkewValuesHasNull));
        LogicalUnion union = new LogicalUnion(Qualifier.ALL, outputs, ImmutableList.of(), constantExprsList.build(),
                false, ImmutableList.of());
        List<Function> generators = ImmutableList.of(new ExplodeNumbers(new IntegerLiteral(factor)));
        SlotReference generateSlot = new SlotReference(EXPLODE_NUMBER_COLUMN_NAME + ctx.generateColumnName(),
                IntegerType.INSTANCE, true);
        LogicalGenerate<Plan> generate = new LogicalGenerate<>(generators, ImmutableList.of(generateSlot), union);
        ImmutableList.Builder<NamedExpression> projectsBuilder = ImmutableList.builderWithExpectedSize(
                union.getOutput().size() + 1);
        projectsBuilder.addAll(union.getOutput());
        projectsBuilder.add(new Alias(new Cast(generateSlot, type)));
        List<NamedExpression> projects = projectsBuilder.build();
        LogicalProject<Plan> project = new LogicalProject<>(projects, generate);
        // construct right join
        EqualPredicate equalTo;
        if (saltedSkewValuesHasNull) {
            equalTo = new NullSafeEqual(outputs.get(0), skewExpr);
        } else {
            equalTo = new EqualTo(outputs.get(0), skewExpr);
        }
        equalTo = (EqualPredicate) TypeCoercionUtils.processComparisonPredicate(equalTo);
        JoinReorderContext joinReorderContext = new JoinReorderContext();
        joinReorderContext.setLeadingJoin(true);
        LogicalJoin<Plan, Plan> rightJoin = new LogicalJoin<>(JoinType.RIGHT_OUTER_JOIN, ImmutableList.of(equalTo),
                project, originPlan, joinReorderContext);
        // construct upper project
        ImmutableList.Builder<NamedExpression> namedExpressionsBuilder = ImmutableList.builderWithExpectedSize(
                originPlan.getOutput().size() + 1);
        namedExpressionsBuilder.addAll(originPlan.getOutput());
        Slot castGeneratedSlot = projects.get(1).toSlot();
        If ifExpr = new If(new IsNull(castGeneratedSlot), Literal.convertToTypedLiteral(DEFAULT_SALT_VALUE, type),
                castGeneratedSlot);
        namedExpressionsBuilder.add(new Alias(ifExpr, randomColumnName));
        return new LogicalProject<>(namedExpressionsBuilder.build(), rightJoin);
    }

    private static int getSaltFactor(MatchingContext<LogicalJoin<Plan, Plan>> ctx) {
        int factor = ctx.connectContext.getStatementContext().getConnectContext()
                .getSessionVariable().skewRewriteJoinSaltExplodeFactor;
        if (factor == 0) {
            int beNumber = Math.max(1, ctx.connectContext.getEnv().getClusterInfo().getBackendsNumber(true));
            int parallelInstance = Math.max(1, ctx.connectContext.getSessionVariable().getParallelExecInstanceNum());
            factor = (int) Math.min((long) beNumber * parallelInstance * SALT_FACTOR, Integer.MAX_VALUE);
        }
        return Math.max(factor, 1);
    }

    private static List<Expression> getSaltedSkewValuesForExpandSide(Expression skewConjunct,
            Set<Expression> skewValuesSet) {
        if (skewConjunct instanceof NullSafeEqual) {
            return Utils.fastToImmutableList(skewValuesSet);
        } else if (skewConjunct instanceof EqualTo) {
            return skewValuesSet.stream().filter(value -> !(value instanceof NullLiteral))
                    .collect(ImmutableList.toImmutableList());
        }
        return ImmutableList.of();
    }

    private static List<Expression> getSaltedSkewValuesForSkewSide(Expression skewConjunct,
            Set<Expression> skewValuesSet, LogicalJoin<Plan, Plan> join) {
        if (skewConjunct instanceof NullSafeEqual) {
            return Utils.fastToImmutableList(skewValuesSet);
        } else if (skewConjunct instanceof EqualTo) {
            if (join.getJoinType().isInnerJoin()) {
                return skewValuesSet.stream().filter(value -> !(value instanceof NullLiteral))
                        .collect(ImmutableList.toImmutableList());
            } else {
                return Utils.fastToImmutableList(skewValuesSet);
            }
        }
        return ImmutableList.of();
    }

    private static LogicalJoin<Plan, Plan> addNotNull(LogicalJoin<Plan, Plan> join, Expression skewConjunct,
            Set<Expression> skewValuesSet) {
        if (skewConjunct instanceof NullSafeEqual) {
            return join;
        }
        boolean containsNull = skewValuesSet.stream().anyMatch(value -> value instanceof NullLiteral);
        if (!containsNull) {
            return join;
        }

        LogicalFilter<Plan> leftFilter =
                new LogicalFilter<>(ImmutableSet.of(new Not(new IsNull(skewConjunct.child(0)))), join.left());
        LogicalFilter<Plan> rightFilter =
                new LogicalFilter<>(ImmutableSet.of(new Not(new IsNull(skewConjunct.child(1)))), join.right());
        DistributeHint hint = join.getDistributeHint();
        switch (join.getJoinType()) {
            case INNER_JOIN:
                hint.setStatus(HintStatus.SUCCESS);
                hint.setSkewInfo(hint.getSkewInfo().withSuccessInSaltJoin(true));
                return join.withDistributeHintChildren(hint, leftFilter, rightFilter);
            case LEFT_OUTER_JOIN:
                hint.setStatus(HintStatus.SUCCESS);
                hint.setSkewInfo(hint.getSkewInfo().withSuccessInSaltJoin(true));
                return join.withDistributeHintChildren(hint, join.left(), rightFilter);
            case RIGHT_OUTER_JOIN:
                hint.setStatus(HintStatus.SUCCESS);
                hint.setSkewInfo(hint.getSkewInfo().withSuccessInSaltJoin(true));
                return join.withDistributeHintChildren(hint, leftFilter, join.right());
            default:
                return join;
        }
    }
}
