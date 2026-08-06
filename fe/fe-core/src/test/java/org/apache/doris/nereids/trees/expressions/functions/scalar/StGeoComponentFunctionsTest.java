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
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Unit tests for ST_NumGeometries, ST_NumPoints, and ST_Geometries scalar functions.
 */
public class StGeoComponentFunctionsTest {

    @Test
    public void testStNumGeometriesBasicProperties() {
        Expression arg = new VarcharLiteral("test");
        StNumGeometries func = new StNumGeometries(arg);

        Assertions.assertEquals("st_numgeometries", func.getName());
        Assertions.assertEquals(1, func.arity());
        Assertions.assertTrue(func.nullable());

        List<FunctionSignature> signatures = func.getSignatures();
        Assertions.assertEquals(1, signatures.size());
        Assertions.assertEquals(BigIntType.INSTANCE, signatures.get(0).returnType);
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, signatures.get(0).getArgType(0));
    }

    @Test
    public void testStNumGeometriesWithChildren() {
        Expression arg = new VarcharLiteral("test");
        StNumGeometries func = new StNumGeometries(arg);

        Expression newArg = new VarcharLiteral("new_test");
        StNumGeometries newFunc = func.withChildren(ImmutableList.of(newArg));

        Assertions.assertNotSame(func, newFunc);
        Assertions.assertEquals("st_numgeometries", newFunc.getName());
        Assertions.assertEquals(1, newFunc.arity());
    }

    @Test
    public void testStNumPointsBasicProperties() {
        Expression arg = new VarcharLiteral("test");
        StNumPoints func = new StNumPoints(arg);

        Assertions.assertEquals("st_numpoints", func.getName());
        Assertions.assertEquals(1, func.arity());
        Assertions.assertTrue(func.nullable());

        List<FunctionSignature> signatures = func.getSignatures();
        Assertions.assertEquals(1, signatures.size());
        Assertions.assertEquals(BigIntType.INSTANCE, signatures.get(0).returnType);
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, signatures.get(0).getArgType(0));
    }

    @Test
    public void testStNumPointsWithChildren() {
        Expression arg = new VarcharLiteral("test");
        StNumPoints func = new StNumPoints(arg);

        Expression newArg = new VarcharLiteral("new_test");
        StNumPoints newFunc = func.withChildren(ImmutableList.of(newArg));

        Assertions.assertNotSame(func, newFunc);
        Assertions.assertEquals("st_numpoints", newFunc.getName());
        Assertions.assertEquals(1, newFunc.arity());
    }

    @Test
    public void testStGeometriesBasicProperties() {
        Expression arg = new VarcharLiteral("test");
        StGeometries func = new StGeometries(arg);

        Assertions.assertEquals("st_geometries", func.getName());
        Assertions.assertEquals(1, func.arity());
        Assertions.assertTrue(func.nullable());

        List<FunctionSignature> signatures = func.getSignatures();
        Assertions.assertEquals(1, signatures.size());
        Assertions.assertTrue(signatures.get(0).returnType instanceof ArrayType);
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, signatures.get(0).getArgType(0));
    }

    @Test
    public void testStGeometriesWithChildren() {
        Expression arg = new VarcharLiteral("test");
        StGeometries func = new StGeometries(arg);

        Expression newArg = new VarcharLiteral("new_test");
        StGeometries newFunc = func.withChildren(ImmutableList.of(newArg));

        Assertions.assertNotSame(func, newFunc);
        Assertions.assertEquals("st_geometries", newFunc.getName());
        Assertions.assertEquals(1, newFunc.arity());
    }

    @Test
    public void testStGeometriesReturnType() {
        Expression arg = new VarcharLiteral("test");
        StGeometries func = new StGeometries(arg);

        List<FunctionSignature> signatures = func.getSignatures();
        FunctionSignature sig = signatures.get(0);

        // Return type should be Array<Varchar>
        Assertions.assertTrue(sig.returnType instanceof ArrayType);
        ArrayType arrayType = (ArrayType) sig.returnType;
        Assertions.assertTrue(arrayType.getItemType() instanceof VarcharType);
    }

}
