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
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Unit tests for {@link TypeOf}.
 */
public class TypeOfTest {

    @Test
    public void testFunctionName() {
        TypeOf typeOf = new TypeOf(new BigIntLiteral(42L));
        Assertions.assertEquals("typeof", typeOf.getName());
    }

    @Test
    public void testArity() {
        TypeOf typeOf = new TypeOf(new StringLiteral("hello"));
        Assertions.assertEquals(1, typeOf.arity());
    }

    @Test
    public void testReturnType() {
        TypeOf typeOf = new TypeOf(new BigIntLiteral(42L));
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, typeOf.getDataType());
    }

    @Test
    public void testNotNullable() {
        TypeOf typeOf = new TypeOf(new NullLiteral());
        Assertions.assertFalse(typeOf.nullable());
    }

    @Test
    public void testSignaturesCount() {
        TypeOf typeOf = new TypeOf(new BigIntLiteral(42L));
        Assertions.assertEquals(1, typeOf.getSignatures().size());
    }

    @Test
    public void testSignatureContent() {
        TypeOf typeOf = new TypeOf(new BigIntLiteral(42L));
        List<FunctionSignature> signatures = typeOf.getSignatures();

        Assertions.assertEquals(1, signatures.size());
        FunctionSignature sig = signatures.get(0);
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, sig.returnType);
        Assertions.assertEquals(1, sig.argumentsTypes.size());
    }

    @Test
    public void testStaticSignatures() {
        Assertions.assertEquals(1, TypeOf.SIGNATURES.size());
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, TypeOf.SIGNATURES.get(0).returnType);
    }

    @Test
    public void testWithChildren() {
        BigIntLiteral originalArg = new BigIntLiteral(1L);
        TypeOf original = new TypeOf(originalArg);

        StringLiteral newArg = new StringLiteral("new");
        TypeOf newTypeOf = original.withChildren(ImmutableList.of(newArg));

        Assertions.assertNotSame(original, newTypeOf);
        Assertions.assertEquals(1, newTypeOf.children().size());
        Assertions.assertEquals(newArg, newTypeOf.child(0));
    }

    @Test
    public void testWithChildrenEmptyThrows() {
        TypeOf typeOf = new TypeOf(new BigIntLiteral(1L));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> typeOf.withChildren(ImmutableList.of()));
    }

    @Test
    public void testWithChildrenTwoThrows() {
        TypeOf typeOf = new TypeOf(new BigIntLiteral(1L));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> typeOf.withChildren(ImmutableList.of(
                        new BigIntLiteral(1L),
                        new StringLiteral("extra"))));
    }

    @Test
    public void testAcceptsAnyDataTypeBigInt() {
        TypeOf typeOf = new TypeOf(new BigIntLiteral(42L));
        Assertions.assertDoesNotThrow(() -> typeOf.checkLegalityBeforeTypeCoercion());
    }

    @Test
    public void testAcceptsAnyDataTypeDouble() {
        TypeOf typeOf = new TypeOf(new DoubleLiteral(3.14));
        Assertions.assertDoesNotThrow(() -> typeOf.checkLegalityBeforeTypeCoercion());
    }

    @Test
    public void testAcceptsAnyDataTypeString() {
        TypeOf typeOf = new TypeOf(new StringLiteral("hello"));
        Assertions.assertDoesNotThrow(() -> typeOf.checkLegalityBeforeTypeCoercion());
    }

    @Test
    public void testAcceptsAnyDataTypeVarchar() {
        TypeOf typeOf = new TypeOf(new VarcharLiteral("hello"));
        Assertions.assertDoesNotThrow(() -> typeOf.checkLegalityBeforeTypeCoercion());
    }

    @Test
    public void testAcceptsAnyDataTypeBoolean() {
        TypeOf typeOf = new TypeOf(BooleanLiteral.FALSE);
        Assertions.assertDoesNotThrow(() -> typeOf.checkLegalityBeforeTypeCoercion());
    }

    @Test
    public void testAcceptsAnyDataTypeNull() {
        TypeOf typeOf = new TypeOf(new NullLiteral());
        Assertions.assertDoesNotThrow(() -> typeOf.checkLegalityBeforeTypeCoercion());
    }

    @Test
    public void testAcceptsSlotReference() {
        SlotReference slot = new SlotReference("col", BigIntType.INSTANCE);
        TypeOf typeOf = new TypeOf(slot);
        Assertions.assertEquals(1, typeOf.children().size());
        Assertions.assertEquals(slot, typeOf.child(0));
    }

}
