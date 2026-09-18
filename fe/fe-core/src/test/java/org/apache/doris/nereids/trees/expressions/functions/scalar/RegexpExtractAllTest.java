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
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Unit tests for the optional group-index argument of regexp_extract_all
 * and regexp_extract_all_array.
 */
public class RegexpExtractAllTest {

    @Test
    public void testTwoAndThreeArgumentForms() {
        Expression str = new VarcharLiteral("abc");
        Expression pattern = new VarcharLiteral("(b)");
        Expression index = new BigIntLiteral(2);

        RegexpExtractAll twoArg = new RegexpExtractAll(str, pattern);
        Assertions.assertEquals("regexp_extract_all", twoArg.getName());
        Assertions.assertEquals(2, twoArg.arity());

        RegexpExtractAll threeArg = new RegexpExtractAll(str, pattern, index);
        Assertions.assertEquals(3, threeArg.arity());
        Assertions.assertEquals(BigIntType.INSTANCE, threeArg.child(2).getDataType());
    }

    @Test
    public void testSignatures() {
        Expression str = new VarcharLiteral("abc");
        Expression pattern = new VarcharLiteral("(b)");

        List<FunctionSignature> signatures = new RegexpExtractAll(str, pattern).getSignatures();
        Assertions.assertEquals(4, signatures.size());
        // two-argument forms come first for backward compatibility
        Assertions.assertEquals(2, signatures.get(0).argumentsTypes.size());
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, signatures.get(0).returnType);
        Assertions.assertEquals(3, signatures.get(2).argumentsTypes.size());
        Assertions.assertEquals(BigIntType.INSTANCE, signatures.get(2).getArgType(2));
    }

    @Test
    public void testWithChildren() {
        Expression str = new VarcharLiteral("abc");
        Expression pattern = new VarcharLiteral("(b)");
        Expression index = new BigIntLiteral(2);

        RegexpExtractAll func = new RegexpExtractAll(str, pattern);
        RegexpExtractAll twoChildren = func.withChildren(ImmutableList.of(str, pattern));
        Assertions.assertNotSame(func, twoChildren);
        Assertions.assertEquals(2, twoChildren.arity());

        RegexpExtractAll threeChildren = func.withChildren(ImmutableList.of(str, pattern, index));
        Assertions.assertEquals(3, threeChildren.arity());

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> func.withChildren(ImmutableList.of(str)));
    }

    @Test
    public void testArrayVariant() {
        Expression str = new VarcharLiteral("abc");
        Expression pattern = new VarcharLiteral("(b)");
        Expression index = new BigIntLiteral(0);

        RegexpExtractAllArray func = new RegexpExtractAllArray(str, pattern);
        Assertions.assertEquals("regexp_extract_all_array", func.getName());
        Assertions.assertEquals(2, func.arity());
        Assertions.assertEquals(4, func.getSignatures().size());

        RegexpExtractAllArray threeChildren = func.withChildren(ImmutableList.of(str, pattern, index));
        Assertions.assertEquals(3, threeChildren.arity());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> func.withChildren(ImmutableList.of(str, pattern, index, index)));
    }
}
