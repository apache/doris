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

import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class OrdTest {
    @Test
    void testAliasesAndSignatures() {
        FunctionRegistry registry = new FunctionRegistry();
        for (String name : ImmutableList.of("ord", "unicode")) {
            for (DataType type : ImmutableList.of(StringType.INSTANCE, VarcharType.SYSTEM_DEFAULT)) {
                for (boolean nullable : new boolean[] {false, true}) {
                    ImmutableList<Expression> arguments = ImmutableList.of(new SlotReference("s", type, nullable));
                    Expression function = registry.findFunctionBuilder(name, arguments).build(name, arguments).first;
                    Assertions.assertInstanceOf(Ord.class, function);
                    Ord ord = (Ord) function;
                    Assertions.assertEquals("ord", ord.getName());
                    Assertions.assertEquals(IntegerType.INSTANCE, ord.getDataType());
                    Assertions.assertEquals(type, ord.getSignature().getArgType(0));
                    Assertions.assertEquals(nullable, ord.nullable());
                }
            }
        }
    }

    @Test
    void testWithChildren() {
        Ord original = new Ord(new SlotReference("s", StringType.INSTANCE, false));
        Assertions.assertEquals(IntegerType.INSTANCE, original.getDataType());
        SlotReference replacement = new SlotReference("nullable_s", StringType.INSTANCE, true);
        Ord rewritten = original.withChildren(ImmutableList.of(replacement));
        Assertions.assertSame(replacement, rewritten.child(0));
        Assertions.assertEquals(original.getSignature(), rewritten.getSignature());
        Assertions.assertTrue(rewritten.nullable());
        Assertions.assertFalse(original.nullable());
        Assertions.assertThrows(IllegalArgumentException.class, () -> original.withChildren(ImmutableList.of()));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> original.withChildren(ImmutableList.of(replacement, replacement)));
    }
}
