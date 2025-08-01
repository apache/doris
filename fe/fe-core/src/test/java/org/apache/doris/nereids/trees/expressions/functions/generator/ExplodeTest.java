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

package org.apache.doris.nereids.trees.expressions.functions.generator;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ExplodeTest {

    /////////////////////////////////////////
    // GetSignatures
    /////////////////////////////////////////

    @Test
    public void testGetSignatures() {
        // build explode(array<int>, array<str>) expression
        Expression[] args = {SlotReference.of("int", ArrayType.of(IntegerType.INSTANCE)),
            SlotReference.of("str", ArrayType.of(StringType.INSTANCE))};
        Explode explode = new Explode(args);

        // check signature
        List<FunctionSignature> signatures = explode.getSignatures();
        Assertions.assertEquals(1, signatures.size());
        FunctionSignature signature = signatures.get(0);
        Assertions.assertEquals(2, signature.argumentsTypes.size());
        Assertions.assertTrue(signature.argumentsTypes.get(0).isArrayType());
        Assertions.assertTrue(((ArrayType) signature.argumentsTypes.get(0)).getItemType().isIntegerType());
        Assertions.assertTrue(signature.argumentsTypes.get(1).isArrayType());
        Assertions.assertTrue(((ArrayType) signature.argumentsTypes.get(1)).getItemType().isStringType());
        Assertions.assertTrue(signature.returnType.isStructType());
        StructType returnType = (StructType) signature.returnType;
        Assertions.assertEquals(2, returnType.getFields().size());
        Assertions.assertEquals(IntegerType.INSTANCE, returnType.getFields().get(0).getDataType());
        Assertions.assertEquals(StringType.INSTANCE, returnType.getFields().get(1).getDataType());
    }

    @Test
    public void testGetSignaturesWithNull() {
        // build explode(null, array<int>) expression
        Expression[] args = { SlotReference.of("null", NullType.INSTANCE), SlotReference.of("int", ArrayType.of(IntegerType.INSTANCE))};
        Explode explode = new Explode(args);

        // check signature
        List<FunctionSignature> signatures = explode.getSignatures();
        Assertions.assertEquals(1, signatures.size());
        FunctionSignature signature = signatures.get(0);
        Assertions.assertEquals(2, signature.argumentsTypes.size());
        Assertions.assertTrue(signature.argumentsTypes.get(0).isArrayType());
        Assertions.assertTrue(((ArrayType) signature.argumentsTypes.get(0)).getItemType().isNullType());
        Assertions.assertTrue(signature.argumentsTypes.get(1).isArrayType());
        Assertions.assertTrue(((ArrayType) signature.argumentsTypes.get(1)).getItemType().isIntegerType());
        Assertions.assertTrue(signature.returnType.isStructType());
        StructType returnType = (StructType) signature.returnType;
        Assertions.assertEquals(2, returnType.getFields().size());
        Assertions.assertEquals(NullType.INSTANCE, returnType.getFields().get(0).getDataType());
        Assertions.assertEquals(IntegerType.INSTANCE, returnType.getFields().get(1).getDataType());
    }

    @Test
    public void testGetSignaturesWithInvalidArgument() {
        // build explode(int)
        Expression[] args = { SlotReference.of("int", IntegerType.INSTANCE) };
        Explode explode = new Explode(args);

        Assertions.assertThrows(AnalysisException.class, explode::getSignatures);
    }

}
