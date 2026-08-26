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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class MapLambdaFunctionsTest extends TestWithFeService {

    private static final NereidsParser PARSER = new NereidsParser();

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("map_lambda_function_test");
        useDatabase("map_lambda_function_test");
        createTables(
                "CREATE TABLE map_lambda_left (id INT) DUPLICATE KEY(id) "
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1 "
                        + "PROPERTIES ('replication_num' = '1')",
                "CREATE TABLE map_lambda_right (id INT) DUPLICATE KEY(id) "
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1 "
                        + "PROPERTIES ('replication_num' = '1')",
                "CREATE TABLE map_lambda_filter (id INT, numeric_map MAP<INT, INT>) DUPLICATE KEY(id) "
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1 "
                        + "PROPERTIES ('replication_num' = '1')");
    }

    @Override
    protected void runBeforeEach() throws Exception {
        StatementScopeIdGenerator.clear();
    }

    @Test
    public void testMapLambdaWrappersAndTypes() {
        Expression mapApply = analyze("map_apply((k, v) -> struct(k + 1, v * 2), map(1, 10, 2, 20))");
        Assertions.assertTrue(mapApply instanceof MapFromEntries);
        assertMapEntryArray(mapApply.child(0));
        Assertions.assertEquals(MapType.of(SmallIntType.INSTANCE, SmallIntType.INSTANCE),
                mapApply.getDataType());

        Expression tupleMapApply = analyze(
                "map_apply((k, v) -> (k + 1, v * 2), map(1, 10, 2, 20))");
        Assertions.assertTrue(tupleMapApply instanceof MapFromEntries);
        assertMapEntryArray(tupleMapApply.child(0));
        Assertions.assertEquals(MapType.of(SmallIntType.INSTANCE, SmallIntType.INSTANCE),
                tupleMapApply.getDataType());

        Expression mapFilter = analyze("map_filter((k, v) -> v > 10, map(1, 10, 2, 20))");
        Assertions.assertTrue(mapFilter instanceof MapFromFilteredEntriesUnique);
        assertMapEntryArray(mapFilter.child(0));

        Expression mapFilterWithMask = analyze("map_filter(map(1, 10, 2, 20), "
                + "array_map((k, v) -> v > k, [1, 2], [10, 20]))");
        Assertions.assertTrue(mapFilterWithMask instanceof MapFilter);
        Assertions.assertTrue(mapFilterWithMask.child(1).getDataType() instanceof ArrayType);

        Expression transformKeys = analyze("transform_keys((k, v) -> k + 1, map(1, 10, 2, 20))");
        Assertions.assertTrue(transformKeys instanceof MapFromEntries);
        assertMapEntryArray(transformKeys.child(0));
        Assertions.assertEquals(MapType.of(SmallIntType.INSTANCE, TinyIntType.INSTANCE),
                transformKeys.getDataType());

        Expression transformValues = analyze(
                "transform_values((k, v) -> v + 1, map(1, 10, 2, 20))");
        Assertions.assertTrue(transformValues instanceof MapFromEntriesUnique);
        assertMapEntryArray(transformValues.child(0));
        Assertions.assertEquals(MapType.of(TinyIntType.INSTANCE, SmallIntType.INSTANCE),
                transformValues.getDataType());
    }

    @Test
    public void testMapExistsAndAllRewriteToArrayMatch() {
        Expression mapExists = analyze("map_exists((k, v) -> v > 10, map(1, 10, 2, 20))");
        Assertions.assertTrue(mapExists instanceof ArrayMatchAny);
        assertMapEntryArray(mapExists.child(0));

        Expression mapAll = analyze("map_all((k, v) -> v > 10, map(1, 10, 2, 20))");
        Assertions.assertTrue(mapAll instanceof ArrayMatchAll);
        assertMapEntryArray(mapAll.child(0));
    }

    @Test
    public void testNestedLambdaCanCaptureImmediateOuterScope() {
        Expression nested = analyze("map_exists((x, v) -> "
                + "array_match_any(x -> x > v, [1]), map(1, 10))");
        Assertions.assertTrue(nested instanceof ArrayMatchAny);
    }

    @Test
    public void testFreshEntryNameAvoidsUserLambdaArgumentCollision() {
        Expression mapExists = analyze("map_exists(($_map_entry_2_$, v) -> "
                + "$_map_entry_2_$ > 0, map(1, 10))");
        Assertions.assertTrue(mapExists instanceof ArrayMatchAny);
        ArrayMap arrayMap = (ArrayMap) mapExists.child(0);
        Lambda lambda = (Lambda) arrayMap.child(0);
        Assertions.assertNotEquals("$_map_entry_2_$", lambda.getLambdaArgumentName(0));
        assertMapEntryArray(arrayMap);
    }

    @Test
    public void testComputedMapIsAccepted() {
        SlotReference value = new SlotReference("value", IntegerType.INSTANCE);
        CreateMap computedMap = new CreateMap(Literal.of(1), value);
        ArrayItemReference entryArgument = new ArrayItemReference("entry", new MapEntries(computedMap));
        Lambda boundLambda = new Lambda(
                ImmutableList.of("entry"),
                new ElementAt(entryArgument.toSlot(), new IntegerLiteral(2)),
                ImmutableList.of(entryArgument));
        TransformValues transformValues = new TransformValues(boundLambda);

        Assertions.assertSame(computedMap, transformValues.child(0));

        Expression nondeterministicMap = analyze(
                "transform_values((k, v) -> v, map(cast(random() as int), 10))");
        Assertions.assertTrue(nondeterministicMap instanceof MapFromEntriesUnique);
        assertMapEntryArray(nondeterministicMap.child(0));
    }

    @Test
    public void testMapDependingOnBothJoinSidesCanBeTranslated() {
        translate("SELECT l.id, r.id FROM map_lambda_left l JOIN map_lambda_right r "
                + "ON map_exists((k, v) -> v > 0, map(l.id, r.id))");
        translate("SELECT l.id, r.id FROM map_lambda_left l JOIN map_lambda_right r "
                + "ON map_contains_key(transform_values((k, v) -> v, map(l.id, r.id)), l.id)");
    }

    @Test
    public void testComputedMapInNestedMapLambdaCanBeTranslated() {
        translate("SELECT transform_values((ok, ov) -> transform_values("
                + "(ik, iv) -> ik, map(ok + random(), ov)), map(1, 10))");
    }

    @Test
    public void testComputedMapInArrayMapLambdaCanBeTranslated() {
        translate("SELECT array_map(x -> map_keys("
                + "transform_values((k, v) -> k, map(uuid(), x)))[1], [1, 2])");
        translate("SELECT array_map(x -> map_keys("
                + "map_apply((k, v) -> struct(k, k), map(uuid(), x)))[1], [1, 2])");
    }

    @Test
    public void testPartiallyMaterializedMapLambdaInputsCanBeTranslated() {
        translate("SELECT transform_keys((k, v) -> 1, map(1, 10, 2, 20))");
        translate("SELECT transform_values((k, v) -> 1, map(1, 10, 2, 20))");
        translate("SELECT map_filter((k, v) -> k > 0, map(1, 10, 2, 20))");
        translate("SELECT map_filter(map(1, 10, 2, 20), "
                + "array_map((k, v) -> v > k, [1, 2], [10, 20]))");
        translate("SELECT count(map_filter(numeric_map, array_map("
                + "(k, v) -> v > k + id, map_keys(numeric_map), map_values(numeric_map)))) "
                + "FROM map_lambda_filter");
    }

    @Test
    public void testMapFromArraysCanBeAnalyzed() {
        Expression map = analyze("map_from_arrays([1, 2], [10, 20])");
        Assertions.assertTrue(map instanceof MapFromArrays);
        Assertions.assertEquals(
                MapType.of(TinyIntType.INSTANCE, TinyIntType.INSTANCE), map.getDataType());

        Assertions.assertThrows(RuntimeException.class,
                () -> analyze("map_from_arrays([1, 2], 10)"));

        AnalysisException complexKeyException = Assertions.assertThrows(AnalysisException.class,
                () -> analyze("map_from_arrays([[1]], [10])"));
        Assertions.assertTrue(complexKeyException.getMessage().contains(
                "MAP key type must be a primitive type"), complexKeyException::getMessage);

        Expression nestedNullValueMap = analyze("map_from_arrays([1], [[]])");
        Assertions.assertEquals(
                MapType.of(TinyIntType.INSTANCE, ArrayType.of(TinyIntType.INSTANCE)),
                nestedNullValueMap.getDataType());
    }

    @Test
    public void testMapFromEntriesCanBeAnalyzed() {
        Expression map = analyze("map_from_entries(array(struct(1, 10), struct(2, 20)))");
        Assertions.assertTrue(map instanceof MapFromEntries);
        Assertions.assertEquals(
                MapType.of(TinyIntType.INSTANCE, TinyIntType.INSTANCE), map.getDataType());

        Assertions.assertThrows(AnalysisException.class,
                () -> analyze("map_from_entries(1)"));
        Assertions.assertThrows(AnalysisException.class,
                () -> analyze("map_from_entries(array(struct(1)))"));

        AnalysisException complexKeyException = Assertions.assertThrows(AnalysisException.class,
                () -> analyze("map_from_entries(array(struct([1], 10)))"));
        Assertions.assertTrue(complexKeyException.getMessage().contains(
                "MAP key type must be a primitive type"), complexKeyException::getMessage);
    }

    @Test
    public void testMapLambdaRejectsInvalidArguments() {
        Assertions.assertThrows(RuntimeException.class,
                () -> analyze("map_filter(k -> k > 0, map(1, 10))"));
        Assertions.assertThrows(RuntimeException.class,
                () -> analyze("map_filter((k, v) -> v > 0, [1, 2])"));
        Assertions.assertThrows(RuntimeException.class,
                () -> analyze("map_filter((k, v) -> v > 0, map(1, 10), map(2, 20))"));
        Assertions.assertThrows(RuntimeException.class,
                () -> analyze("map_apply((k, v) -> if(k > 0, struct(k, v), null), map(1, 10))"));
        Assertions.assertThrows(RuntimeException.class,
                () -> analyze("map_apply((k, v) -> (k, v, k + v), map(1, 10))"));
    }

    private Expression analyze(String sql) {
        return ExpressionRewriteTestHelper.typeCoercion(PARSER.parseExpression(sql));
    }

    private void translate(String sql) {
        StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        PhysicalPlan plan = planner.planWithLock(PARSER.parseSingle(sql), PhysicalProperties.ANY);
        new PhysicalPlanTranslator(new PlanTranslatorContext(planner.getCascadesContext()))
                .translatePlan(plan);
    }

    private void assertMapEntryArray(Expression expression) {
        while (expression instanceof Cast) {
            expression = expression.child(0);
        }
        Assertions.assertTrue(expression instanceof ArrayMap);
        ArrayMap arrayMap = (ArrayMap) expression;
        Assertions.assertTrue(arrayMap.child(0) instanceof Lambda);

        Lambda lambda = (Lambda) arrayMap.child(0);
        List<ArrayItemReference> arguments = lambda.getLambdaArguments();
        Assertions.assertEquals(1, arguments.size());
        Assertions.assertTrue(arguments.get(0).getArrayExpression() instanceof MapEntries);
        Assertions.assertTrue(arguments.get(0).getName().startsWith("$_map_entry_"));
        Assertions.assertEquals(1,
                arrayMap.<MapEntries>collect(child -> child instanceof MapEntries).size());
        Assertions.assertTrue(arrayMap.<MapKeys>collect(child -> child instanceof MapKeys).isEmpty());
        Assertions.assertTrue(arrayMap.<MapValues>collect(child -> child instanceof MapValues).isEmpty());

        Assertions.assertTrue(arrayMap.withChildren(arrayMap.children()) instanceof ArrayMap);
    }

}
