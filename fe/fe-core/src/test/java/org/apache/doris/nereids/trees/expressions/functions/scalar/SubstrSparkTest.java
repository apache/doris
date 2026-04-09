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
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Unit tests for {@link SubstrSpark}.
 */
public class SubstrSparkTest {

    @Test
    public void testNameAndSignatures() {
        SubstrSpark f = new SubstrSpark(new StringLiteral("a"), new IntegerLiteral(1));
        Assertions.assertEquals("substr_spark", f.getName());
        Assertions.assertEquals(4, f.getSignatures().size());
    }

    @Test
    public void testTwoArgUsesMaxLengthChild() {
        SlotReference col = new SlotReference("s", VarcharType.SYSTEM_DEFAULT);
        SubstrSpark f = new SubstrSpark(col, new IntegerLiteral(-1));
        Assertions.assertEquals(3, f.children().size());
        Assertions.assertTrue(f.getLength().isPresent());
    }

    @Test
    public void testThreeArg() {
        SlotReference col = new SlotReference("s", VarcharType.SYSTEM_DEFAULT);
        SubstrSpark f = new SubstrSpark(col, new IntegerLiteral(1), new IntegerLiteral(3));
        Assertions.assertEquals(3, f.children().size());
        Assertions.assertEquals(col, f.getSource());
        Assertions.assertInstanceOf(IntegerLiteral.class, f.getPosition());
        Assertions.assertEquals(1, ((IntegerLiteral) f.getPosition()).getIntValue());
        Expression len = f.getLength().orElseThrow(AssertionError::new);
        Assertions.assertInstanceOf(IntegerLiteral.class, len);
        Assertions.assertEquals(3, ((IntegerLiteral) len).getIntValue());
    }

    @Test
    public void testComputeSignatureWithConstantLength() {
        SlotReference col = new SlotReference("s", VarcharType.SYSTEM_DEFAULT);
        SubstrSpark f = new SubstrSpark(col, new IntegerLiteral(1), new IntegerLiteral(12));
        FunctionSignature bound = f.getSignatures().stream()
                .filter(sig -> sig.argumentsTypes.size() == 3
                        && sig.argumentsTypes.get(0).equals(VarcharType.SYSTEM_DEFAULT))
                .findFirst()
                .orElseThrow(AssertionError::new);
        FunctionSignature computed = f.computeSignature(bound);
        Assertions.assertEquals(VarcharType.createVarcharType(12), computed.returnType);
    }

    @Test
    public void testWithChildrenTwoArg() {
        SlotReference c1 = new SlotReference("s", VarcharType.SYSTEM_DEFAULT);
        IntegerLiteral p = new IntegerLiteral(2);
        SubstrSpark original = new SubstrSpark(c1, p);

        SlotReference c2 = new SlotReference("t", StringType.INSTANCE);
        IntegerLiteral p2 = new IntegerLiteral(3);
        SubstrSpark replaced = original.withChildren(ImmutableList.of(c2, p2));

        Assertions.assertNotSame(original, replaced);
        Assertions.assertEquals(c2, replaced.getSource());
        Assertions.assertInstanceOf(IntegerLiteral.class, replaced.getPosition());
        Assertions.assertEquals(3, ((IntegerLiteral) replaced.getPosition()).getIntValue());
    }

    @Test
    public void testWithChildrenThreeArg() {
        SlotReference c1 = new SlotReference("s", VarcharType.SYSTEM_DEFAULT);
        SubstrSpark original = new SubstrSpark(c1, new IntegerLiteral(1), new IntegerLiteral(2));

        SlotReference c2 = new SlotReference("t", StringType.INSTANCE);
        List<Expression> kids = ImmutableList.of(c2, new IntegerLiteral(2), new IntegerLiteral(4));
        SubstrSpark replaced = original.withChildren(kids);

        Assertions.assertEquals(c2, replaced.getSource());
        Assertions.assertInstanceOf(IntegerLiteral.class, replaced.getPosition());
        Assertions.assertEquals(2, ((IntegerLiteral) replaced.getPosition()).getIntValue());
        Expression len = replaced.getLength().orElseThrow(AssertionError::new);
        Assertions.assertInstanceOf(IntegerLiteral.class, len);
        Assertions.assertEquals(4, ((IntegerLiteral) len).getIntValue());
    }

    @Test
    public void testWithChildrenInvalidArity() {
        SubstrSpark f = new SubstrSpark(new StringLiteral("a"), new IntegerLiteral(1));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> f.withChildren(ImmutableList.of(new StringLiteral("a"))));
    }

    @Test
    public void testStringTypeThreeArgSignatureShape() {
        FunctionSignature sig = new SubstrSpark(new StringLiteral("a"), new IntegerLiteral(1), new IntegerLiteral(1))
                .getSignatures()
                .stream()
                .filter(s -> s.argumentsTypes.get(0).equals(StringType.INSTANCE)
                        && s.argumentsTypes.size() == 3)
                .findFirst()
                .orElseThrow(AssertionError::new);
        Assertions.assertEquals(StringType.INSTANCE, sig.returnType);
        Assertions.assertEquals(IntegerType.INSTANCE, sig.argumentsTypes.get(1));
        Assertions.assertEquals(IntegerType.INSTANCE, sig.argumentsTypes.get(2));
    }
}
