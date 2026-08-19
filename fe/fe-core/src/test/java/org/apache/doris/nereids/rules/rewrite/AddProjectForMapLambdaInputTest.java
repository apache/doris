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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Array;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapAll;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapApply;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapContainsKey;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapEntryArrayMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapFromEntries;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapKeys;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MapValues;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StrToMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.TransformValues;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

public class AddProjectForMapLambdaInputTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan studentScan
            = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.student);

    @Test
    void testMaterializeNondeterministicMapAsOneExpression() {
        Random random = new Random();
        CreateMap map = new CreateMap(random, new IntegerLiteral(10));
        LogicalProject<?> input = project(transformValues(map, new IntegerLiteral(0)), studentScan);

        LogicalProject<?> rewritten = (LogicalProject<?>) PlanChecker.from(
                        MemoTestUtils.createConnectContext(), input)
                .applyTopDown(new AddProjectForMapLambdaInput())
                .applyTopDown(new AddProjectForVolatileExpression())
                .applyTopDown(new MergeProjectable())
                .getPlan();

        LogicalProject<?> mapProject = (LogicalProject<?>) rewritten.child();
        Alias mapAlias = lastAlias(mapProject);
        Assertions.assertEquals(map, mapAlias.child());
        Assertions.assertEquals(studentScan, mapProject.child());
        assertTransformValuesUsesMapSlot(rewritten.getProjects().get(0).child(0), mapAlias.toSlot());
    }

    @Test
    void testMaterializeMapApplyInput() {
        SlotReference studentId = (SlotReference) studentScan.getOutput().get(0);
        CreateMap map = new CreateMap(studentId, new Random());
        MapFromEntries loweredMapApply = lowerMapApply(map);
        LogicalProject<?> input = project(loweredMapApply, studentScan);

        LogicalProject<?> rewritten = (LogicalProject<?>) PlanChecker.from(
                        MemoTestUtils.createConnectContext(), input)
                .applyTopDown(new AddProjectForMapLambdaInput())
                .applyTopDown(new MergeProjectable())
                .getPlan();

        LogicalProject<?> mapProject = (LogicalProject<?>) rewritten.child();
        Alias mapAlias = lastAlias(mapProject);
        Assertions.assertEquals(map, mapAlias.child());

        MapFromEntries result = (MapFromEntries) rewritten.getProjects().get(0).child(0);
        Lambda entryLambda = (Lambda) ((MapEntryArrayMap) result.child(0)).child(0);
        assertLambdaUsesMapSlot(entryLambda, mapAlias.toSlot());
    }

    @Test
    void testMaterializeDeterministicMapInFilter() {
        SlotReference studentName = (SlotReference) studentScan.getOutput().get(2);
        StrToMap map = new StrToMap(studentName);
        LogicalFilter<?> input = new LogicalFilter<>(ImmutableSet.of(
                new MapContainsKey(transformValues(map, new StringLiteral("value")),
                        new StringLiteral("key"))), studentScan);

        LogicalFilter<?> rewritten = (LogicalFilter<?>) PlanChecker.from(
                        MemoTestUtils.createConnectContext(), input)
                .applyTopDown(new AddProjectForMapLambdaInput())
                .getPlan();

        LogicalProject<?> mapProject = (LogicalProject<?>) rewritten.child();
        Alias mapAlias = lastAlias(mapProject);
        Assertions.assertEquals(map, mapAlias.child());
        MapContainsKey predicate = (MapContainsKey) rewritten.getConjuncts().iterator().next();
        assertTransformValuesUsesMapSlot(predicate.child(0), mapAlias.toSlot());
    }

    @Test
    void testMaterializeDirectMapEntryLambdaInput() {
        SlotReference studentId = (SlotReference) studentScan.getOutput().get(0);
        CreateMap map = new CreateMap(studentId, new IntegerLiteral(1));
        Lambda lambda = bindLambda("map_all", map, BooleanLiteral.TRUE);
        LogicalProject<?> input = project(new MapAll(lambda), studentScan);

        LogicalProject<?> rewritten = (LogicalProject<?>) PlanChecker.from(
                        MemoTestUtils.createConnectContext(), input)
                .applyTopDown(new AddProjectForMapLambdaInput())
                .getPlan();

        LogicalProject<?> mapProject = (LogicalProject<?>) rewritten.child();
        Alias mapAlias = lastAlias(mapProject);
        MapAll mapAll = (MapAll) rewritten.getProjects().get(0).child(0);
        Lambda rewrittenLambda = (Lambda) ((MapEntryArrayMap) mapAll.child(0)).child(0);
        assertLambdaUsesMapSlot(rewrittenLambda, mapAlias.toSlot());
    }

    @Test
    void testMapRuleDoesNotMaterializeUnrelatedLambdaBody() {
        SlotReference studentName = (SlotReference) studentScan.getOutput().get(2);
        StrToMap map = new StrToMap(studentName);
        Random random = new Random();
        Add lambdaBody = new Add(random, random);
        LogicalProject<?> input = project(transformValues(map, lambdaBody), studentScan);

        LogicalProject<?> rewritten = (LogicalProject<?>) PlanChecker.from(
                        MemoTestUtils.createConnectContext(), input)
                .applyTopDown(new AddProjectForMapLambdaInput())
                .getPlan();

        LogicalProject<?> mapProject = (LogicalProject<?>) rewritten.child();
        Assertions.assertEquals(studentScan.getOutput().size() + 1, mapProject.getProjects().size());
        Assertions.assertEquals(map, lastAlias(mapProject).child());
        TransformValues transformValues = (TransformValues) rewritten.getProjects().get(0).child(0);
        Lambda rewrittenLambda = (Lambda) ((MapEntryArrayMap) transformValues.child(1)).child(0);
        Assertions.assertEquals(lambdaBody, rewrittenLambda.getLambdaFunction());
    }

    @Test
    void testMaterializeJoinMapInputOnLeft() {
        LogicalOlapScan scoreScan
                = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.score);
        SlotReference studentId = (SlotReference) studentScan.getOutput().get(0);
        CreateMap map = new CreateMap(studentId, new IntegerLiteral(1));
        LogicalJoin<?, ?> input = joinWithMap(studentScan, scoreScan, map);

        LogicalJoin<?, ?> rewritten = (LogicalJoin<?, ?>) PlanChecker.from(
                        MemoTestUtils.createConnectContext(), input)
                .applyTopDown(new AddProjectForMapLambdaInput())
                .getPlan();

        LogicalProject<?> leftProject = (LogicalProject<?>) rewritten.left();
        Alias mapAlias = lastAlias(leftProject);
        Assertions.assertEquals(map, mapAlias.child());
        Assertions.assertEquals(scoreScan, rewritten.right());
        assertJoinTransformValuesUsesMapSlot(rewritten, mapAlias.toSlot());
    }

    @Test
    void testMaterializeJoinMapInputOnRight() {
        LogicalOlapScan scoreScan
                = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.score);
        SlotReference scoreId = (SlotReference) scoreScan.getOutput().get(0);
        Random random = new Random();
        CreateMap map = new CreateMap(new Add(scoreId, random), new IntegerLiteral(1));
        LogicalJoin<?, ?> input = joinWithMap(studentScan, scoreScan, map);

        LogicalJoin<?, ?> rewritten = (LogicalJoin<?, ?>) PlanChecker.from(
                        MemoTestUtils.createConnectContext(), input)
                .applyTopDown(new AddProjectForMapLambdaInput())
                .applyTopDown(new AddProjectForVolatileExpression())
                .getPlan();

        Assertions.assertEquals(studentScan, rewritten.left());
        LogicalProject<?> rightProject = (LogicalProject<?>) rewritten.right();
        Alias mapAlias = lastAlias(rightProject);
        Assertions.assertEquals(map, mapAlias.child());
        assertJoinTransformValuesUsesMapSlot(rewritten, mapAlias.toSlot());
    }

    @Test
    void testSkipJoinMapInputDependingOnBothSides() {
        LogicalOlapScan scoreScan
                = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.score);
        SlotReference studentId = (SlotReference) studentScan.getOutput().get(0);
        SlotReference scoreId = (SlotReference) scoreScan.getOutput().get(0);
        CreateMap map = new CreateMap(studentId, scoreId);
        LogicalJoin<?, ?> input = joinWithMap(studentScan, scoreScan, map);

        Plan rewritten = PlanChecker.from(MemoTestUtils.createConnectContext(), input)
                .applyTopDown(new AddProjectForMapLambdaInput())
                .getPlan();

        Assertions.assertEquals(input, rewritten);
    }

    @Test
    void testRejectVolatileJoinMapInputDependingOnBothSides() {
        LogicalOlapScan scoreScan
                = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.score);
        SlotReference studentId = (SlotReference) studentScan.getOutput().get(0);
        SlotReference scoreId = (SlotReference) scoreScan.getOutput().get(0);
        CreateMap map = new CreateMap(new Add(studentId, new Random()), scoreId);
        LogicalJoin<?, ?> input = joinWithMap(studentScan, scoreScan, map);

        Assertions.assertThrows(AnalysisException.class, () -> PlanChecker.from(
                        MemoTestUtils.createConnectContext(), input)
                .applyTopDown(new AddProjectForMapLambdaInput())
                .getPlan());
    }

    @Test
    void testMaterializeMapInputInNestedLambda() {
        SlotReference studentId = (SlotReference) studentScan.getOutput().get(0);
        CreateMap outerMap = new CreateMap(studentId, new IntegerLiteral(10));
        Lambda outerTemplate = new Lambda(ImmutableList.of("ok", "ov"), new IntegerLiteral(0));
        List<ArrayItemReference> outerArguments = outerTemplate.makeArguments(
                "transform_values", ImmutableList.of(outerMap));
        Slot outerKey = outerArguments.get(0).toSlot();
        Slot outerValue = outerArguments.get(1).toSlot();

        CreateMap innerMap = new CreateMap(new Add(outerKey, new Random()), outerValue);
        Lambda innerTemplate = new Lambda(ImmutableList.of("ik", "iv"), new IntegerLiteral(0));
        List<ArrayItemReference> innerArguments = innerTemplate.makeArguments(
                "transform_values", ImmutableList.of(innerMap));
        TransformValues innerTransform = new TransformValues(
                innerTemplate.withLambdaFunctionArguments(innerArguments.get(0).toSlot(), innerArguments));
        Lambda outerLambda = outerTemplate.withLambdaFunctionArguments(innerTransform, outerArguments);
        LogicalProject<?> input = project(new TransformValues(outerLambda), studentScan);

        LogicalProject<?> rewritten = (LogicalProject<?>) PlanChecker.from(
                        MemoTestUtils.createConnectContext(), input)
                .applyTopDown(new AddProjectForMapLambdaInput())
                .getPlan();

        TransformValues outerTransform = (TransformValues) rewritten.getProjects().get(0).child(0);
        Lambda rewrittenOuterLambda = (Lambda) ((MapEntryArrayMap) outerTransform.child(1)).child(0);
        Assertions.assertEquals(3, rewrittenOuterLambda.getLambdaArguments().size());
        ArrayItemReference hiddenArgument = rewrittenOuterLambda.getLambdaArgument(2);
        Assertions.assertInstanceOf(ArrayMap.class, hiddenArgument.getArrayExpression());
        ArrayMap materializer = (ArrayMap) hiddenArgument.getArrayExpression();
        Lambda materializerLambda = (Lambda) materializer.child(0);
        Assertions.assertTrue(materializerLambda.getLambdaFunction().containsType(Random.class));

        TransformValues rewrittenInnerTransform
                = (TransformValues) rewrittenOuterLambda.getLambdaFunction();
        Assertions.assertEquals(hiddenArgument.toSlot(), rewrittenInnerTransform.child(0));
        Lambda rewrittenInnerLambda
                = (Lambda) ((MapEntryArrayMap) rewrittenInnerTransform.child(1)).child(0);
        assertLambdaUsesMapSlot(rewrittenInnerLambda, hiddenArgument.toSlot());

        LogicalProject<?> rewrittenAgain = (LogicalProject<?>) PlanChecker.from(
                        MemoTestUtils.createConnectContext(), rewritten)
                .applyTopDown(new AddProjectForMapLambdaInput())
                .getPlan();
        TransformValues outerTransformAgain
                = (TransformValues) rewrittenAgain.getProjects().get(0).child(0);
        Lambda outerLambdaAgain
                = (Lambda) ((MapEntryArrayMap) outerTransformAgain.child(1)).child(0);
        Assertions.assertEquals(3, outerLambdaAgain.getLambdaArguments().size());
    }

    @Test
    void testMaterializeMapInputInRegularArrayMapLambda() {
        Array inputArray = new Array(new IntegerLiteral(1), new IntegerLiteral(2));
        Lambda outerTemplate = new Lambda(ImmutableList.of("x"), new IntegerLiteral(0));
        List<ArrayItemReference> outerArguments = outerTemplate.makeArguments(
                "array_map", ImmutableList.of(inputArray));
        Slot outerItem = outerArguments.get(0).toSlot();

        CreateMap innerMap = new CreateMap(new Random(), outerItem);
        Lambda innerTemplate = new Lambda(ImmutableList.of("k", "v"), new IntegerLiteral(0));
        List<ArrayItemReference> innerArguments = innerTemplate.makeArguments(
                "transform_values", ImmutableList.of(innerMap));
        TransformValues innerTransform = new TransformValues(
                innerTemplate.withLambdaFunctionArguments(innerArguments.get(0).toSlot(), innerArguments));
        Lambda outerLambda = outerTemplate.withLambdaFunctionArguments(innerTransform, outerArguments);
        LogicalProject<?> input = project(new ArrayMap(outerLambda), studentScan);

        LogicalProject<?> rewritten = (LogicalProject<?>) PlanChecker.from(
                        MemoTestUtils.createConnectContext(), input)
                .applyTopDown(new AddProjectForMapLambdaInput())
                .applyTopDown(new AddProjectForVolatileExpression())
                .getPlan();

        Assertions.assertEquals(studentScan, rewritten.child());
        ArrayMap rewrittenArrayMap = (ArrayMap) rewritten.getProjects().get(0).child(0);
        Lambda rewrittenOuterLambda = (Lambda) rewrittenArrayMap.child(0);
        Assertions.assertEquals(2, rewrittenOuterLambda.getLambdaArguments().size());
        ArrayItemReference hiddenArgument = rewrittenOuterLambda.getLambdaArgument(1);
        Assertions.assertInstanceOf(ArrayMap.class, hiddenArgument.getArrayExpression());
        ArrayMap materializer = (ArrayMap) hiddenArgument.getArrayExpression();
        Lambda materializerLambda = (Lambda) materializer.child(0);
        Assertions.assertTrue(materializerLambda.getLambdaFunction().containsType(Random.class));

        TransformValues rewrittenInnerTransform
                = (TransformValues) rewrittenOuterLambda.getLambdaFunction();
        Assertions.assertEquals(hiddenArgument.toSlot(), rewrittenInnerTransform.child(0));
        Lambda rewrittenInnerLambda
                = (Lambda) ((MapEntryArrayMap) rewrittenInnerTransform.child(1)).child(0);
        assertLambdaUsesMapSlot(rewrittenInnerLambda, hiddenArgument.toSlot());
    }

    @Test
    void testMaterializeNestedMapApplyInput() {
        Array inputArray = new Array(new IntegerLiteral(1), new IntegerLiteral(2));
        Lambda outerTemplate = new Lambda(ImmutableList.of("x"), new IntegerLiteral(0));
        List<ArrayItemReference> outerArguments = outerTemplate.makeArguments(
                "array_map", ImmutableList.of(inputArray));
        CreateMap innerMap = new CreateMap(new Random(), outerArguments.get(0).toSlot());
        MapFromEntries loweredMapApply = lowerMapApply(innerMap);
        Lambda outerLambda = outerTemplate.withLambdaFunctionArguments(
                loweredMapApply, outerArguments);
        LogicalProject<?> input = project(new ArrayMap(outerLambda), studentScan);

        LogicalProject<?> rewritten = (LogicalProject<?>) PlanChecker.from(
                        MemoTestUtils.createConnectContext(), input)
                .applyTopDown(new AddProjectForMapLambdaInput())
                .getPlan();

        ArrayMap rewrittenArrayMap = (ArrayMap) rewritten.getProjects().get(0).child(0);
        Lambda rewrittenOuterLambda = (Lambda) rewrittenArrayMap.child(0);
        // Keep x plus the materialized Map input used by the nested Map Lambda.
        Assertions.assertEquals(2, rewrittenOuterLambda.getLambdaArguments().size());
        ArrayItemReference mapArgument = rewrittenOuterLambda.getLambdaArgument(1);
        Assertions.assertInstanceOf(ArrayMap.class, mapArgument.getArrayExpression());
        Assertions.assertTrue(mapArgument.getArrayExpression().containsType(Random.class));

        MapFromEntries result = (MapFromEntries) rewrittenOuterLambda.getLambdaFunction();
        Lambda entryLambda = (Lambda) ((MapEntryArrayMap) result.child(0)).child(0);
        assertLambdaUsesMapSlot(entryLambda, mapArgument.toSlot());
    }

    private LogicalProject<?> project(Expression expression, Plan child) {
        return new LogicalProject<Plan>(ImmutableList.of(new Alias(expression)), child);
    }

    private TransformValues transformValues(Expression map, Expression body) {
        return new TransformValues(bindLambda("transform_values", map, body));
    }

    private MapFromEntries lowerMapApply(Expression map) {
        Lambda lambda = new Lambda(ImmutableList.of("k", "v"), new IntegerLiteral(0));
        List<ArrayItemReference> arguments = lambda.makeArguments("map_apply", ImmutableList.of(map));
        CreateStruct body = new CreateStruct(arguments.get(0).toSlot(), arguments.get(1).toSlot());
        MapApply mapApply = new MapApply(lambda.withLambdaFunctionArguments(body, arguments));
        return (MapFromEntries) mapApply.rewriteWhenAnalyze();
    }

    private Lambda bindLambda(String functionName, Expression map, Expression body) {
        Lambda lambda = new Lambda(ImmutableList.of("k", "v"), body);
        List<ArrayItemReference> arguments = lambda.makeArguments(functionName, ImmutableList.of(map));
        return lambda.withLambdaFunctionArguments(body, arguments);
    }

    private LogicalJoin<?, ?> joinWithMap(Plan left, Plan right, Expression map) {
        MapContainsKey predicate = new MapContainsKey(
                transformValues(map, new IntegerLiteral(0)), new IntegerLiteral(1));
        return new LogicalJoin<Plan, Plan>(
                JoinType.INNER_JOIN,
                ImmutableList.of(),
                ImmutableList.of(predicate),
                new DistributeHint(DistributeType.NONE),
                Optional.empty(),
                left,
                right,
                null);
    }

    private Alias lastAlias(LogicalProject<?> project) {
        return (Alias) project.getProjects().get(project.getProjects().size() - 1);
    }

    private void assertJoinTransformValuesUsesMapSlot(LogicalJoin<?, ?> join, Slot mapSlot) {
        MapContainsKey predicate = (MapContainsKey) join.getOtherJoinConjuncts().get(0);
        assertTransformValuesUsesMapSlot(predicate.child(0), mapSlot);
    }

    private void assertTransformValuesUsesMapSlot(Expression expression, Slot mapSlot) {
        TransformValues transformValues = (TransformValues) expression;
        Assertions.assertEquals(mapSlot, transformValues.child(0));
        Lambda lambda = (Lambda) ((MapEntryArrayMap) transformValues.child(1)).child(0);
        assertLambdaUsesMapSlot(lambda, mapSlot);
    }

    private void assertLambdaUsesMapSlot(Lambda lambda, Slot mapSlot) {
        Assertions.assertEquals(mapSlot,
                ((MapKeys) lambda.getLambdaArgument(0).getArrayExpression()).child(0));
        Assertions.assertEquals(mapSlot,
                ((MapValues) lambda.getLambdaArgument(1).getArrayExpression()).child(0));
    }

}
