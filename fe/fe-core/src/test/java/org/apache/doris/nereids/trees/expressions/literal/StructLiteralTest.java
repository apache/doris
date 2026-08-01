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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.nereids.rules.expression.check.CheckCast;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.TryCast;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateNamedStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateStruct;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class StructLiteralTest {

    @Test
    public void testInferFieldNullabilityForRequiredTarget() {
        StructLiteral literal = new StructLiteral(ImmutableList.of(
                new IntegerLiteral(20), new NullLiteral(StringType.INSTANCE)));
        StructType literalType = (StructType) literal.getDataType();

        Assertions.assertFalse(literalType.getFields().get(0).isNullable());
        Assertions.assertTrue(literalType.getFields().get(1).isNullable());

        StructType requiredTarget = new StructType(ImmutableList.of(
                new StructField("required_metric", BigIntType.INSTANCE, false, ""),
                new StructField("required_label", StringType.INSTANCE, true, "")));
        Assertions.assertTrue(CheckCast.check(literalType, requiredTarget, false));

        StructType nullableTarget = new StructType(ImmutableList.of(
                new StructField("metric", BigIntType.INSTANCE, true, ""),
                new StructField("label", StringType.INSTANCE, true, "")));
        StructLiteral nonNullLiteral = new StructLiteral(ImmutableList.of(
                new IntegerLiteral(10), new StringLiteral("value")));
        Assertions.assertTrue(CheckCast.check(nonNullLiteral.getDataType(), nullableTarget, false));
        Assertions.assertFalse(CheckCast.check(nullableTarget, nonNullLiteral.getDataType(), false));
    }

    @Test
    public void testNamedStructInfersValueNullability() {
        CreateNamedStruct required = new CreateNamedStruct(
                new StringLiteral("metric"), new IntegerLiteral(10));
        StructType requiredType = (StructType) required.customSignature().returnType;
        Assertions.assertFalse(requiredType.getFields().get(0).isNullable());

        CreateNamedStruct nullable = new CreateNamedStruct(
                new StringLiteral("metric"), new NullLiteral(BigIntType.INSTANCE));
        StructType nullableType = (StructType) nullable.customSignature().returnType;
        Assertions.assertTrue(nullableType.getFields().get(0).isNullable());
    }

    @Test
    public void testStructFunctionsKeepStrictPreCastChildrenRequired() {
        SlotReference requiredString = new SlotReference("metric", StringType.INSTANCE, false);
        Cast cast = new Cast(requiredString, IntegerType.INSTANCE);
        TryCast tryCast = new TryCast(requiredString, IntegerType.INSTANCE);

        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(true);
            // Expression nullability is an immutable plan property; strict-mode refinement belongs
            // only to the struct type being constructed for the current session.
            Assertions.assertTrue(cast.nullable());

            CreateStruct struct = new CreateStruct(cast);
            StructType structType = (StructType) struct.getSignatures().get(0).returnType;
            Assertions.assertFalse(structType.getFields().get(0).isNullable());

            CreateNamedStruct namedStruct = new CreateNamedStruct(new StringLiteral("metric"), cast);
            StructType namedStructType = (StructType) namedStruct.customSignature().returnType;
            Assertions.assertFalse(namedStructType.getFields().get(0).isNullable());

            CreateNamedStruct namedTryStruct = new CreateNamedStruct(new StringLiteral("metric"), tryCast);
            StructType namedTryStructType = (StructType) namedTryStruct.customSignature().returnType;
            Assertions.assertTrue(namedTryStructType.getFields().get(0).isNullable());
        }

        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(false);
            CreateStruct struct = new CreateStruct(cast);
            StructType structType = (StructType) struct.getSignatures().get(0).returnType;
            Assertions.assertTrue(structType.getFields().get(0).isNullable());
        }
    }
}
