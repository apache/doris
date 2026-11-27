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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.functions.ComputeSignatureHelper;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.ImplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
import org.apache.doris.nereids.trees.expressions.functions.agg.ArrayAgg;
import org.apache.doris.nereids.trees.expressions.functions.agg.CollectList;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.window.LastValue;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.typecoercion.TypeCheckResult;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.FileType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

public class FileKeyInputTypeTest {
    private static final List<DataType> FILE_TYPES = ImmutableList.of(
            FileType.INSTANCE, ArrayType.of(FileType.INSTANCE),
            MapType.of(StringType.INSTANCE, FileType.INSTANCE),
            new StructType(ImmutableList.of(new StructField("f", FileType.INSTANCE, true, ""))),
            new StructType(ImmutableList.of(new StructField("f", ArrayType.of(FileType.INSTANCE), true, ""))),
            DataType.convertFromString("AGG_STATE<count(FILE)>"),
            DataType.convertFromString("AGG_STATE<any_value(ARRAY<FILE>)>"));

    @Test
    public void testKeyUseCapability() {
        for (DataType type : FILE_TYPES) {
            for (DataType.KeyUse keyUse : DataType.KeyUse.values()) {
                Assertions.assertFalse(type.allowsKeyUse(keyUse), keyUse + " " + type.toSql());
                AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                        () -> type.checkKeyUse(keyUse, "ORDER BY"), keyUse + " " + type.toSql());
                Assertions.assertTrue(error.getMessage().startsWith("ORDER BY does not support type "),
                        error.getMessage());
            }
        }
        for (String sql : ImmutableList.of("INT", "STRING", "ARRAY<INT>", "MAP<STRING,INT>",
                "STRUCT<v:INT>", "AGG_STATE<sum(INT)>")) {
            DataType type = DataType.convertFromString(sql);
            for (DataType.KeyUse keyUse : DataType.KeyUse.values()) {
                Assertions.assertTrue(type.allowsKeyUse(keyUse), keyUse + " " + sql);
                Assertions.assertDoesNotThrow(() -> type.checkKeyUse(keyUse, "ORDER BY"), keyUse + " " + sql);
            }
        }
    }

    @Test
    public void testInSubqueryRejectsFileOnEitherSide() {
        for (DataType type : FILE_TYPES) {
            InSubquery compareFile = inSubquery(new SlotReference("f", type), IntegerType.INSTANCE);
            assertRejected(compareFile, "IN does not support type ");
            InSubquery outputFile = inSubquery(new SlotReference("n", IntegerType.INSTANCE), type);
            assertRejected(outputFile, "IN does not support type ");
        }
    }

    @Test
    public void testWindowRejectsFilePartitionAndOrderKeys() {
        for (DataType type : FILE_TYPES) {
            SlotReference key = new SlotReference("f", type);
            WindowExpression partition = new WindowExpression(new RowNumber(),
                    ImmutableList.of(key), ImmutableList.of());
            assertRejected(partition, "window PARTITION BY does not support type ");
            WindowExpression order = new WindowExpression(new RowNumber(), ImmutableList.of(),
                    ImmutableList.of(new OrderExpression(new OrderKey(key, true, true))));
            assertRejected(order, "ORDER BY does not support type ");
        }
    }

    @Test
    public void testOrderExpressionAndAggregateDistinctRejectFileKeys() {
        for (DataType type : FILE_TYPES) {
            SlotReference key = new SlotReference("f", type);
            assertRejected(new OrderExpression(new OrderKey(key, true, true)), "ORDER BY does not support type ");
            assertRejected(new CollectList(true, key), "DISTINCT does not support type ");
            assertRejected(new ArrayAgg(true, key), "DISTINCT does not support type ");
            Assertions.assertTrue(new Count(true, key).checkInputDataTypes().success(), type.toSql());
            Assertions.assertTrue(new AnyValue(true, key).checkInputDataTypes().success(), type.toSql());
        }
    }

    @Test
    public void testRecursiveValidationRejectsFileKeys() {
        SlotReference file = new SlotReference("f", FileType.INSTANCE);
        Alias subquery = new Alias(inSubquery(file, FileType.INSTANCE), "matched");
        assertRejected(subquery, "IN does not support type ");
        Alias window = new Alias(new WindowExpression(new RowNumber(),
                ImmutableList.of(file), ImmutableList.of()), "ranked");
        assertRejected(window, "window PARTITION BY does not support type ");
    }

    @Test
    public void testOrdinaryKeysAndFileWindowResultsRemainSupported() {
        for (DataType type : ImmutableList.of(IntegerType.INSTANCE, StringType.INSTANCE)) {
            SlotReference key = new SlotReference("k", type);
            Assertions.assertTrue(inSubquery(key, type).checkInputDataTypes().success());
            WindowExpression window = new WindowExpression(new LastValue(new SlotReference("f", FileType.INSTANCE)),
                    ImmutableList.of(key), ImmutableList.of(new OrderExpression(new OrderKey(key, true, true))));
            Assertions.assertTrue(window.checkInputDataTypes().success());
            Assertions.assertEquals(FileType.INSTANCE, window.getDataType());
        }
    }

    @Test
    public void testConstrainedAnyDataTypeSignatures() {
        List<AnyDataType> constrainedTemplates = ImmutableList.of(
                AnyDataType.ORDERING_WITHOUT_INDEX,
                AnyDataType.GROUP_EQUALITY_WITHOUT_INDEX,
                AnyDataType.ordering(0),
                AnyDataType.groupEquality(0));
        Assertions.assertNotEquals(AnyDataType.ordering(0), AnyDataType.groupEquality(0));
        Assertions.assertNotEquals(AnyDataType.ordering(0), new AnyDataType(0));
        Assertions.assertEquals("ORDERABLEANY#0", AnyDataType.ordering(0).toSql());
        Assertions.assertEquals("EQUATABLEANY", AnyDataType.GROUP_EQUALITY_WITHOUT_INDEX.toSql());

        for (AnyDataType template : constrainedTemplates) {
            FunctionSignature sig = FunctionSignature.ret(IntegerType.INSTANCE).args(template);
            FunctionSignature arraySig = FunctionSignature.ret(IntegerType.INSTANCE).args(ArrayType.of(template));
            for (DataType fileType : FILE_TYPES) {
                Assertions.assertFalse(template.acceptsType(fileType), template + " " + fileType.toSql());
                Assertions.assertFalse(ImplicitlyCastableSignature.isImplicitlyCastable(template, fileType));
                Assertions.assertFalse(ExplicitlyCastableSignature.isExplicitlyCastable(template, fileType));
                Assertions.assertFalse(ExplicitlyCastableSignature.isExplicitlyCastable(
                        ArrayType.of(template), ArrayType.of(fileType)));
                Assertions.assertEquals(Optional.empty(), TypeCoercionUtils.implicitCast(fileType, template));
                List<Expression> args = ImmutableList.of(new SlotReference("f", fileType));
                List<Expression> arrayArgs = ImmutableList.of(new SlotReference("a", ArrayType.of(fileType)));
                if (template.getIndex() < 0) {
                    Assertions.assertThrows(AnalysisException.class,
                            () -> ComputeSignatureHelper.implementAnyDataTypeWithOutIndex(sig, args));
                    Assertions.assertThrows(AnalysisException.class,
                            () -> ComputeSignatureHelper.implementAnyDataTypeWithOutIndex(arraySig, arrayArgs));
                } else {
                    Assertions.assertThrows(AnalysisException.class,
                            () -> ComputeSignatureHelper.implementAnyDataTypeWithIndex(sig, args));
                    Assertions.assertThrows(AnalysisException.class,
                            () -> ComputeSignatureHelper.implementAnyDataTypeWithIndex(arraySig, arrayArgs));
                }
            }
            for (DataType allowed : ImmutableList.of(NullType.INSTANCE, IntegerType.INSTANCE,
                    DataType.convertFromString("ARRAY<INT>"), DataType.convertFromString("AGG_STATE<sum(INT)>"))) {
                Assertions.assertTrue(template.acceptsType(allowed), template + " " + allowed.toSql());
                Assertions.assertTrue(ImplicitlyCastableSignature.isImplicitlyCastable(template, allowed));
                Assertions.assertTrue(ExplicitlyCastableSignature.isExplicitlyCastable(template, allowed));
                Assertions.assertEquals(Optional.of(allowed), TypeCoercionUtils.implicitCast(allowed, template));
                List<Expression> args = allowed instanceof NullType
                        ? ImmutableList.of(new NullLiteral())
                        : ImmutableList.of(new SlotReference("v", allowed));
                Assertions.assertDoesNotThrow(() -> template.getIndex() < 0
                        ? ComputeSignatureHelper.implementAnyDataTypeWithOutIndex(sig, args)
                        : ComputeSignatureHelper.implementAnyDataTypeWithIndex(sig, args));
            }
        }
    }

    private void assertRejected(Expression expression, String expectedMessage) {
        TypeCheckResult result = expression.checkInputDataTypes();
        Assertions.assertTrue(result.failed());
        Assertions.assertTrue(result.getMessage().startsWith(expectedMessage), result.getMessage());
    }

    private InSubquery inSubquery(Expression compare, DataType outputType) {
        LogicalOneRowRelation query = new LogicalOneRowRelation(StatementScopeIdGenerator.newRelationId(),
                ImmutableList.of(new Alias(new SlotReference("output", outputType), "output")));
        return new InSubquery(compare, query, false);
    }
}
