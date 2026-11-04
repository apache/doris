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

package org.apache.doris.nereids.trees.expressions.functions.combinator;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ColumnToProtobuf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.expression.check.CheckCast;
import org.apache.doris.nereids.rules.expression.rules.ConvertAggStateCast;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.AvgMap;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctSum;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctSum0;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum0;
import org.apache.doris.nereids.trees.expressions.functions.agg.SumMap;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.proto.Types;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TTypeNodeType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class DecimalAggregateStateTest {
    private List<AggregateFunction> aggregates(int precision, int scale) {
        DataType decimal = DecimalV3Type.createDecimalV3TypeNotCheck256(precision, scale);
        SlotReference scalar = new SlotReference("v", decimal, true);
        SlotReference map = new SlotReference("m", MapType.of(IntegerType.INSTANCE, decimal), false);
        return ImmutableList.of(new Sum(scalar), new Sum0(scalar), new Avg(scalar),
                new MultiDistinctSum(scalar), new MultiDistinctSum0(scalar), new SumMap(map), new AvgMap(map));
    }

    private AggStateType analyzeState(String sql) {
        return ((AggStateType) new NereidsParser().parseDataType(sql).conversion()).resolveDeclaration();
    }

    @Test
    void testDeclarationBindsAtAnalysis() {
        ConnectContext context = MemoTestUtils.createConnectContext();
        context.getSessionVariable().enableDecimal256 = false;
        AggStateType parsed = (AggStateType) new NereidsParser()
                .parseDataType("agg_state<avg(decimal(18,2))>").conversion();
        Assertions.assertThrows(AnalysisException.class, parsed::getResultType);
        context.getSessionVariable().enableDecimal256 = true;
        AggStateType analyzed = parsed.resolveDeclaration();
        Assertions.assertEquals(DecimalV3Type.createDecimalV3Type(76, 4), analyzed.getResultType());
        Assertions.assertSame(analyzed, analyzed.resolveDeclaration());
        AggStateType declaration = AggStateType.declaration(
                "avg", parsed.getSubTypes(), parsed.getSubTypeNullables(), true);
        Assertions.assertEquals(declaration, parsed);
        Assertions.assertEquals(analyzed, declaration.resolveDeclaration());
    }

    @Test
    void testResultSignaturesAndPersistence() throws Exception {
        ConnectContext context = MemoTestUtils.createConnectContext();
        for (boolean enabled : new boolean[] {false, true}) {
            for (int precision : new int[] {9, 18, 38}) {
                for (int scale : new int[] {2, 6}) {
                    for (AggregateFunction nested : aggregates(precision, scale)) {
                        context.getSessionVariable().enableDecimal256 = enabled;
                        DataType expected = nested.getDataType();
                        AggStateType state = (AggStateType) StateCombinator.create(nested).getDataType();
                        Assertions.assertEquals(state, new CombineCombinator(nested.children(), nested).getDataType());
                        String declaration = "agg_state<" + nested.getName() + "("
                                + nested.child(0).getDataType().toSql()
                                + (nested.child(0).nullable() ? "" : " not null") + ")>";
                        Assertions.assertEquals(state, analyzeState(declaration).conversion());
                        Type catalog = state.toCatalogDataType();
                        Assertions.assertEquals(catalog, GsonUtils.GSON.fromJson(
                                GsonUtils.GSON.toJson(catalog, Type.class), Type.class));
                        Assertions.assertEquals(state, analyzeState(state.toSql()).conversion());
                        context.getSessionVariable().enableDecimal256 = !enabled;
                        SlotReference slot = new SlotReference("s", state, false);
                        Assertions.assertEquals(expected, new MergeCombinator(ImmutableList.of(slot), nested).getDataType());
                        Assertions.assertEquals(expected,
                                new FinalizeCombinator(ImmutableList.of(slot), nested).getDataType());
                        Assertions.assertEquals(expected, state.getResultType());
                        Column column = new Column("s", catalog, false, AggregateType.GENERIC, false, null, "");
                        Types.PTypeDesc pb = ColumnToProtobuf.toPb(column, ImmutableSet.of(), ImmutableList.of())
                                .getAggStateResultType();
                        int valueIndex = expected instanceof MapType ? 2 : 0;
                        Assertions.assertEquals(valueIndex + 1, pb.getTypesCount());
                        Assertions.assertEquals(enabled ? 76 : 38,
                                pb.getTypes(valueIndex).getScalarType().getPrecision());
                        if (expected instanceof MapType) {
                            Assertions.assertEquals(TTypeNodeType.MAP.getValue(), pb.getTypes(0).getType());
                            Assertions.assertEquals(ImmutableList.of(true, true),
                                    pb.getTypes(0).getContainsNullsList());
                        }
                    }
                }
            }
        }
    }

    @Test
    void testNonDecimalSignaturesAndComplexMetadata() throws Exception {
        MemoTestUtils.createConnectContext();
        for (String[] types : new String[][] {
                {"count(int)", "bigint"}, {"sum(int)", "bigint"}, {"avg(int)", "double"},
                {"collect_list(int)", "array<int>"}, {"percentile(double,double)", "double"},
                {"percentile_reservoir(double,double)", "double"},
                {"any_value(struct<k:int,v:array<string>>)", "struct<k:int,v:array<string>>"}}) {
            AggStateType state = analyzeState("agg_state<" + types[0] + ">");
            DataType expected = new NereidsParser().parseDataType(types[1]).conversion();
            Assertions.assertEquals(expected, state.getResultType());
            Assertions.assertEquals(state, DataType.fromCatalogType(state.toCatalogDataType()));
            Assertions.assertTrue(state.toCatalogDataType().toThrift().isSetAggStateResultType());
            Column column = new Column("s", state.toCatalogDataType(), false, AggregateType.GENERIC,
                    false, null, "");
            Types.PTypeDesc pb = ColumnToProtobuf.toPb(column, ImmutableSet.of(), ImmutableList.of())
                    .getAggStateResultType();
            Assertions.assertTrue(pb.getTypesCount() > 0);
            if (types[0].startsWith("any_value")) {
                Assertions.assertEquals(2, pb.getTypes(0).getStructFieldsCount());
                Assertions.assertEquals("k", pb.getTypes(0).getStructFields(0).getName());
                Assertions.assertEquals("v", pb.getTypes(0).getStructFields(1).getName());
                Assertions.assertTrue(pb.getTypes(0).getStructFields(1).getContainsNull());
            }
        }
    }

    @Test
    void testMissingResultSignatureIsRejected() {
        AggStateType state = new AggStateType("avg", ImmutableList.of(IntegerType.INSTANCE),
                ImmutableList.of(true), true);
        Assertions.assertThrows(AnalysisException.class, state::getResultType);
    }

    @Test
    void testRawArgumentConversionDoesNotPermitSerializedStateCasts() {
        ConnectContext context = MemoTestUtils.createConnectContext();
        context.getSessionVariable().enableDecimal256 = true;
        for (AggregateFunction nested : aggregates(3, 1)) {
            StateCombinator producer = StateCombinator.create(nested);
            boolean map = nested.getName().endsWith("_map");
            DataType input = DecimalV3Type.createDecimalV3Type(76, 6);
            if (map) {
                input = MapType.of(IntegerType.INSTANCE, input);
            }
            AggregateFunction widened = (AggregateFunction) nested.withChildren(new SlotReference("v", input, true));
            DataType target = StateCombinator.create(widened).getDataType();
            Assertions.assertTrue(CheckCast.checkWithLooseAggState(producer.getDataType(), target, false));
            Assertions.assertFalse(CheckCast.check(producer.getDataType(), target, false));
            Expression rewritten = ConvertAggStateCast.convert(new Cast(producer, target));
            Assertions.assertEquals(target, rewritten.child(0).getDataType());
            Assertions.assertFalse(CheckCast.checkWithLooseAggState(
                    new CombineCombinator(nested.children(), nested).getDataType(), target, false));
            Cast storedCast = new Cast(new SlotReference("s", producer.getDataType(), false), target);
            Assertions.assertSame(storedCast, ConvertAggStateCast.convert(storedCast));
        }
    }

    @Test
    void testMapKeyMetadata() throws Exception {
        ConnectContext context = MemoTestUtils.createConnectContext();
        context.getSessionVariable().enableDecimal256 = true;
        SlotReference input = new SlotReference("m", MapType.of(VarcharType.createVarcharType(17),
                DecimalV3Type.createDecimalV3Type(18, 2)), false);
        AggStateType state = (AggStateType) StateCombinator.create(new SumMap(input)).getDataType();
        Column column = new Column("s", state.toCatalogDataType(), false, AggregateType.GENERIC, false, null, "");
        Types.PTypeDesc pb = ColumnToProtobuf.toPb(column, ImmutableSet.of(), ImmutableList.of())
                .getAggStateResultType();
        Assertions.assertEquals(17, pb.getTypes(1).getScalarType().getLen());
    }

    @Test
    void testVersionGateIncludesAllStates() {
        ConnectContext context = MemoTestUtils.createConnectContext();
        int version = Config.be_exec_version;
        try {
            Config.be_exec_version = Config.AGG_STATE_RESULT_TYPE_MIN_BE_EXEC_VERSION - 1;
            Assertions.assertThrows(AnalysisException.class, () -> StateCombinator.create(
                    new Count(new SlotReference("v", IntegerType.INSTANCE, true))));
            for (boolean enabled : new boolean[] {false, true}) {
                context.getSessionVariable().enableDecimal256 = enabled;
                for (AggregateFunction nested : aggregates(18, 2)) {
                    Assertions.assertThrows(AnalysisException.class, () -> StateCombinator.create(nested));
                }
            }
        } finally {
            Config.be_exec_version = version;
        }
    }
}
