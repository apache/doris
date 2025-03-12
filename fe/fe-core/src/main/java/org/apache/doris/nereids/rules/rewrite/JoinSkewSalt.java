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

import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeNumbers;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * LogicalJoin(type:inner, t1.a=t2.a, hint:skew(t1.a(1,2)))
 *   +--LogicalOlapScan(t1)
 *   +--LogicalOlapScan(t2)
 * ->
 * LogicalJoin (type:inner, t1.a=t2.a and r1=r2)
 *   |--LogicalProject (t1.a, CASE WHEN t1.a IS NULL THEN random(0, 999) WHEN t1.a IN (1, 2) THEN random(0, 999) ELSE 0 END AS r1))
 *   | +--LogicalOlapScan(t1)
 *   +--LogicalProject (projections: t2.a, if(explodeNumber IS NULL, 0, explodeNumber) as r2)
 *     +--LogicalJoin (type=right_outer_join, t2.a=skewValue)
 *       |--LogicalGenerate(generators=[explode_numbers(1000)], generatorOutput=[explodeNumber])
 *       |  +--LogicalUnion(outputs=[skewValue], constantExprsList(1,2))
 *       +--LogicalOlapScan(t2)
 * */
public class JoinSkewSalt extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalJoin().thenApply(JoinSkewSalt::transform).toRule(RuleType.JOIN_SKEW_SALT);
    }

    private static Plan transform(MatchingContext<LogicalJoin<Plan, Plan>> ctx) {
        LogicalJoin<Plan, Plan> join = ctx.root;
        // 3.有mark join不能支持？
        // 5.skew是否可以使用右表的列？ sr不可
        int factor = ctx.connectContext.getStatementContext().getConnectContext()
                .getSessionVariable().joinSkewAddSaltExplodeFactor;
        if (join.getJoinType() != JoinType.INNER_JOIN && join.getJoinType() != JoinType.LEFT_OUTER_JOIN) {
            return null;
        }
        if (join.isMarkJoin()) {
            return null;
        }
        DistributeHint hint = join.getDistributeHint();
        Expression skewExpr = hint.getSkewExpr();
        if (hint.distributeType != DistributeType.SHUFFLE_RIGHT || skewExpr == null) {
            return null;
        }
        if (!skewExpr.isSlot()) {
            return null;
        }
        if (!join.left().getOutput().contains((Slot) skewExpr)) {
            return null;
        }

        Expression leftSkewExpr = null;
        Expression rightSkewExpr = null;
        for (Expression conjunct : join.getHashJoinConjuncts()) {
            if (skewExpr.equals(conjunct.child(0))) {
                leftSkewExpr = conjunct.child(0);
                rightSkewExpr = conjunct.child(1);
                break;
            } else if (skewExpr.equals(conjunct.child(1))) {
                leftSkewExpr = conjunct.child(1);
                rightSkewExpr = conjunct.child(1);
                break;
            }
        }
        if (leftSkewExpr == null || rightSkewExpr == null) {
            return null;
        }
        List<Expression> skewValues = join.getDistributeHint().getSkewValues();
        skewValues = skewValues.stream().filter(e -> !(e instanceof NullLiteral)).collect(Collectors.toList());

        LogicalProject<Plan> projectR = addSaltForRight(rightSkewExpr, skewValues, join.right(), factor);
        LogicalProject<Plan> project = addSaltForLeft(leftSkewExpr, skewValues, join.left(), factor);

        EqualTo saltEqual = new EqualTo(project.getProjects().get(project.getProjects().size() - 1).toSlot(),
                projectR.getProjects().get(projectR.getProjects().size() - 1).toSlot());
        saltEqual = (EqualTo) TypeCoercionUtils.processComparisonPredicate(saltEqual);
        List<Expression> newHashJoinConjuncts = new ArrayList<>(join.getHashJoinConjuncts());
        newHashJoinConjuncts.add(saltEqual);
        return new LogicalJoin<>(join.getJoinType(), newHashJoinConjuncts, join.getOtherJoinConjuncts(),
                new DistributeHint(hint.distributeType), project, projectR, JoinReorderContext.EMPTY);
    }

    private static LogicalProject<Plan> addSaltForLeft(Expression leftSkewExpr, List<Expression> skewValues,
            Plan originLeft, int factor) {
        // 3.为左边生成随机值
        // 上面加一个project,里面是原来的所有列加上一个case when列
        IsNull isNull = new IsNull(leftSkewExpr);
        Random random = new Random(new BigIntLiteral(0), new BigIntLiteral(factor - 1));
        List<WhenClause> whenClauses = new ArrayList<>();
        whenClauses.add(new WhenClause(isNull, random));
        if (!skewValues.isEmpty()) {
            InPredicate in = new InPredicate(leftSkewExpr, skewValues);
            whenClauses.add(new WhenClause(in, random));
        }
        CaseWhen caseWhen = new CaseWhen(whenClauses, new BigIntLiteral(0));
        List<NamedExpression> namedExpressions = new ArrayList<>(originLeft.getOutput());
        namedExpressions.add(new Alias(caseWhen, "r1"));
        return new LogicalProject<>(namedExpressions, originLeft);
    }

    private static LogicalProject<Plan> addSaltForRight(Expression rightSkewExpr, List<Expression> skewValues,
            Plan originRight, int factor) {
        List<List<NamedExpression>> constantExprsList = new ArrayList<>();
        List<NamedExpression> outputs = ImmutableList.of(new SlotReference("skewValue",
                rightSkewExpr.getDataType(), false));
        if (skewValues.isEmpty()) {
            List<NamedExpression> namedExpressions = new ArrayList<>(originRight.getOutput());
            namedExpressions.add(new Alias(new IntegerLiteral(0), "r2"));
            return new LogicalProject<>(namedExpressions, originRight);
        } else {
            for (Expression skewValue : skewValues) {
                constantExprsList.add(ImmutableList.of(new Alias(skewValue, "skewValue")));
            }
            LogicalUnion union = new LogicalUnion(Qualifier.ALL, outputs, ImmutableList.of(), constantExprsList,
                    false, ImmutableList.of());
            // construct LogicalGenerate
            List<Function> generators = ImmutableList.of(new ExplodeNumbers(new IntegerLiteral(factor)));
            SlotReference generateSlot = new SlotReference("explodeNumber", IntegerType.INSTANCE, false);
            LogicalGenerate<Plan> generate = new LogicalGenerate<>(generators, ImmutableList.of(generateSlot), union);

            // construct right join
            EqualTo equalTo = new EqualTo(outputs.get(0), rightSkewExpr);
            equalTo = (EqualTo) TypeCoercionUtils.processComparisonPredicate(equalTo);
            JoinReorderContext joinReorderContext = new JoinReorderContext();
            joinReorderContext.setLeadingJoin(true);
            LogicalJoin<Plan, Plan> rightJoin = new LogicalJoin<>(JoinType.RIGHT_OUTER_JOIN, ImmutableList.of(equalTo),
                    generate, originRight, joinReorderContext);
            // construct upper project
            List<NamedExpression> namedExpressions = new ArrayList<>(originRight.getOutput());
            If ifExpr = new If(new IsNull(generateSlot), new IntegerLiteral(0), generateSlot);
            namedExpressions.add(new Alias(ifExpr, "r2"));
            return new LogicalProject<>(namedExpressions, rightJoin);
        }
    }
}
