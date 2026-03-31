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
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Unit tests for {@link JsonExtractSpark}.
 */
public class JsonExtractSparkTest {

    @Test
    public void testSignaturesCount() {
        Assertions.assertEquals(2, JsonExtractSpark.SIGNATURES.size());
    }

    @Test
    public void testVarcharSignature() {
        SlotReference varcharColumn = new SlotReference("json_col", VarcharType.SYSTEM_DEFAULT);
        VarcharLiteral pathLiteral = new VarcharLiteral("$.key");
        JsonExtractSpark func = new JsonExtractSpark(varcharColumn, pathLiteral);

        FunctionSignature signature = func.getSignature();

        Assertions.assertEquals(StringType.INSTANCE, signature.returnType);
        Assertions.assertEquals(2, signature.argumentsTypes.size());
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, signature.argumentsTypes.get(0));
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, signature.argumentsTypes.get(1));
    }

    @Test
    public void testStringSignature() {
        SlotReference stringColumn = new SlotReference("json_col", StringType.INSTANCE);
        StringLiteral pathLiteral = new StringLiteral("$.key");
        JsonExtractSpark func = new JsonExtractSpark(stringColumn, pathLiteral);

        FunctionSignature signature = func.getSignature();

        Assertions.assertEquals(StringType.INSTANCE, signature.returnType);
        Assertions.assertEquals(2, signature.argumentsTypes.size());
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, signature.argumentsTypes.get(0));
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, signature.argumentsTypes.get(1));
    }

    @Test
    public void testWithChildren() {
        SlotReference jsonColumn = new SlotReference("json_col", VarcharType.SYSTEM_DEFAULT);
        VarcharLiteral pathLiteral = new VarcharLiteral("$.key");
        JsonExtractSpark original = new JsonExtractSpark(jsonColumn, pathLiteral);

        SlotReference newJsonColumn = new SlotReference("new_json_col", VarcharType.SYSTEM_DEFAULT);
        VarcharLiteral newPathLiteral = new VarcharLiteral("$.newKey");
        List<Expression> newChildren = ImmutableList.of(newJsonColumn, newPathLiteral);

        JsonExtractSpark newFunc = original.withChildren(newChildren);

        Assertions.assertNotSame(original, newFunc);
        Assertions.assertEquals(2, newFunc.children().size());
        Assertions.assertEquals(newJsonColumn, newFunc.child(0));
        Assertions.assertEquals(newPathLiteral, newFunc.child(1));
    }

    @Test
    public void testWithChildrenInvalidSize() {
        SlotReference jsonColumn = new SlotReference("json_col", VarcharType.SYSTEM_DEFAULT);
        VarcharLiteral pathLiteral = new VarcharLiteral("$.key");
        JsonExtractSpark func = new JsonExtractSpark(jsonColumn, pathLiteral);

        List<Expression> oneChild = ImmutableList.of(jsonColumn);
        Assertions.assertThrows(IllegalArgumentException.class, () -> func.withChildren(oneChild));

        List<Expression> threeChildren = ImmutableList.of(jsonColumn, pathLiteral, pathLiteral);
        Assertions.assertThrows(IllegalArgumentException.class, () -> func.withChildren(threeChildren));
    }

    @Test
    public void testFunctionName() {
        SlotReference jsonColumn = new SlotReference("json_col", VarcharType.SYSTEM_DEFAULT);
        VarcharLiteral pathLiteral = new VarcharLiteral("$.key");
        JsonExtractSpark func = new JsonExtractSpark(jsonColumn, pathLiteral);

        Assertions.assertEquals("json_extract_spark", func.getName());
    }

    @Test
    public void testGetSignatures() {
        SlotReference jsonColumn = new SlotReference("json_col", VarcharType.SYSTEM_DEFAULT);
        VarcharLiteral pathLiteral = new VarcharLiteral("$.key");
        JsonExtractSpark func = new JsonExtractSpark(jsonColumn, pathLiteral);

        List<FunctionSignature> signatures = func.getSignatures();

        Assertions.assertEquals(JsonExtractSpark.SIGNATURES, signatures);
        Assertions.assertEquals(2, signatures.size());
    }

    @Test
    public void testSignatureOrderPreservation() {
        // Test that signatures are returned in the expected order
        List<FunctionSignature> signatures = JsonExtractSpark.SIGNATURES;

        // Third signature: (VARCHAR, VARCHAR) -> STRING
        FunctionSignature sig0 = signatures.get(0);
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, sig0.argumentsTypes.get(0));
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, sig0.argumentsTypes.get(1));

        // Fourth signature: (STRING, STRING) -> STRING
        FunctionSignature sig1 = signatures.get(1);
        Assertions.assertEquals(StringType.INSTANCE, sig1.argumentsTypes.get(0));
        Assertions.assertEquals(StringType.INSTANCE, sig1.argumentsTypes.get(1));

        // All signatures should return STRING
        for (FunctionSignature sig : signatures) {
            Assertions.assertEquals(StringType.INSTANCE, sig.returnType);
        }
    }
}
