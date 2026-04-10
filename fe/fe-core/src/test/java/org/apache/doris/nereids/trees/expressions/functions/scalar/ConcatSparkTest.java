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

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.AnyDataType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Unit tests for {@link ConcatSpark}.
 */
public class ConcatSparkTest {

    @Test
    public void testSignaturesCount() {
        Assertions.assertEquals(3, ConcatSpark.SIGNATURES.size());
    }

    @Test
    public void testFunctionName() {
        ConcatSpark func = new ConcatSpark(new VarcharLiteral("a"), new VarcharLiteral("b"));
        Assertions.assertEquals("concat_spark", func.getName());
    }

    @Test
    public void testArity() {
        ConcatSpark two = new ConcatSpark(new VarcharLiteral("a"), new VarcharLiteral("b"));
        Assertions.assertEquals(2, two.arity());

        ConcatSpark one = new ConcatSpark(new VarcharLiteral("x"));
        Assertions.assertEquals(1, one.arity());
    }

    @Test
    public void testVarcharSignature() {
        VarcharLiteral a = new VarcharLiteral("a");
        VarcharLiteral b = new VarcharLiteral("b");
        ConcatSpark func = new ConcatSpark(a, b);

        FunctionSignature signature = func.getSignature();

        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, signature.returnType);
        Assertions.assertTrue(signature.hasVarArgs);
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, signature.getVarArgType().orElse(null));
    }

    @Test
    public void testStringSignature() {
        StringLiteral a = new StringLiteral("a");
        StringLiteral b = new StringLiteral("b");
        ConcatSpark func = new ConcatSpark(a, b);

        FunctionSignature signature = func.getSignature();

        Assertions.assertEquals(StringType.INSTANCE, signature.returnType);
        Assertions.assertTrue(signature.hasVarArgs);
        Assertions.assertEquals(StringType.INSTANCE, signature.getVarArgType().orElse(null));
    }

    @Test
    public void testArraySignature() {
        ArrayType arrInt = ArrayType.of(IntegerType.INSTANCE);
        SlotReference left = new SlotReference("c1", arrInt);
        SlotReference right = new SlotReference("c2", arrInt);
        ConcatSpark func = new ConcatSpark(left, right);

        FunctionSignature signature = func.getSignature();

        Assertions.assertEquals(arrInt, signature.returnType);
        Assertions.assertTrue(signature.hasVarArgs);
        Assertions.assertEquals(arrInt, signature.getVarArgType().orElse(null));
    }

    @Test
    public void testStringConcatReturnTypeAndNullable() {
        ConcatSpark func = new ConcatSpark(new StringLiteral("a"), new StringLiteral("b"));
        Assertions.assertEquals(StringType.INSTANCE, func.getDataType());
        Assertions.assertFalse(func.nullable());
    }

    @Test
    public void testArrayConcatReturnTypeAndNullable() {
        ArrayType arrInt = ArrayType.of(IntegerType.INSTANCE);
        ConcatSpark func = new ConcatSpark(new SlotReference("a", arrInt), new SlotReference("b", arrInt));
        Assertions.assertEquals(arrInt, func.getDataType());
        Assertions.assertTrue(func.nullable());
    }

    @Test
    public void testWithChildren() {
        VarcharLiteral a = new VarcharLiteral("a");
        VarcharLiteral b = new VarcharLiteral("b");
        ConcatSpark original = new ConcatSpark(a, b);

        VarcharLiteral a2 = new VarcharLiteral("x");
        VarcharLiteral b2 = new VarcharLiteral("y");
        ConcatSpark replaced = original.withChildren(ImmutableList.of(a2, b2));

        Assertions.assertNotSame(original, replaced);
        Assertions.assertEquals(2, replaced.children().size());
        Assertions.assertEquals(a2, replaced.child(0));
        Assertions.assertEquals(b2, replaced.child(1));
    }

    @Test
    public void testWithChildrenEmptyThrows() {
        ConcatSpark func = new ConcatSpark(new VarcharLiteral("a"));
        List<Expression> empty = ImmutableList.of();
        Assertions.assertThrows(IllegalArgumentException.class, () -> func.withChildren(empty));
    }

    @Test
    public void testGetSignatures() {
        ConcatSpark func = new ConcatSpark(new VarcharLiteral("a"), new VarcharLiteral("b"));
        Assertions.assertEquals(ConcatSpark.SIGNATURES, func.getSignatures());
    }

    @Test
    public void testSignatureOrderArrayFirst() {
        List<FunctionSignature> signatures = ConcatSpark.SIGNATURES;
        Assertions.assertEquals(
                FunctionSignature.retArgType(0).varArgs(ArrayType.of(new AnyDataType(0))),
                signatures.get(0));
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, signatures.get(1).returnType);
        Assertions.assertEquals(StringType.INSTANCE, signatures.get(2).returnType);
    }
}
