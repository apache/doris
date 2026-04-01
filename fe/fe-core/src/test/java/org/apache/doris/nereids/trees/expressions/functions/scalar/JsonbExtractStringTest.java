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
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Unit tests for {@link JsonbExtractString}.
 * Tests all function signatures including:
 * - (JSON, VARCHAR) -> STRING
 * - (JSON, STRING) -> STRING
 * - (VARCHAR, VARCHAR) -> STRING (new signature for on-demand parsing optimization)
 * - (STRING, STRING) -> STRING (new signature for on-demand parsing optimization)
 */
public class JsonbExtractStringTest {

    @Test
    public void testSignaturesCount() {
        // Verify that SIGNATURES contains exactly 4 signatures
        Assertions.assertEquals(4, JsonbExtractString.SIGNATURES.size());
    }

    @Test
    public void testJsonAndVarcharSignature() {
        // Test signature: (JSON, VARCHAR) -> STRING
        SlotReference jsonColumn = new SlotReference("json_col", JsonType.INSTANCE);
        VarcharLiteral pathLiteral = new VarcharLiteral("$.key");
        JsonbExtractString func = new JsonbExtractString(jsonColumn, pathLiteral);

        FunctionSignature signature = func.getSignature();

        Assertions.assertEquals(StringType.INSTANCE, signature.returnType);
        Assertions.assertEquals(2, signature.argumentsTypes.size());
        Assertions.assertEquals(JsonType.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, signature.argumentsTypes.get(1));
    }

    @Test
    public void testJsonAndStringSignature() {
        SlotReference jsonColumn = new SlotReference("json_col", JsonType.INSTANCE);
        StringLiteral pathLiteral = new StringLiteral("$.key");
        JsonbExtractString func = new JsonbExtractString(jsonColumn, pathLiteral);

        FunctionSignature signature = func.getSignature();

        Assertions.assertEquals(StringType.INSTANCE, signature.returnType);
        Assertions.assertEquals(2, signature.argumentsTypes.size());
        Assertions.assertEquals(JsonType.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, signature.argumentsTypes.get(1));
    }

    @Test
    public void testVarcharAndVarcharSignature() {
        // Test signature: (VARCHAR, VARCHAR) -> STRING
        // This is a new signature added for on-demand parsing optimization
        SlotReference varcharColumn = new SlotReference("varchar_col", VarcharType.SYSTEM_DEFAULT);
        VarcharLiteral pathLiteral = new VarcharLiteral("$.key");
        JsonbExtractString func = new JsonbExtractString(varcharColumn, pathLiteral);

        FunctionSignature signature = func.getSignature();

        Assertions.assertEquals(StringType.INSTANCE, signature.returnType);
        Assertions.assertEquals(2, signature.argumentsTypes.size());
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, signature.argumentsTypes.get(0));
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, signature.argumentsTypes.get(1));
    }

    @Test
    public void testStringAndStringSignature() {
        SlotReference stringColumn = new SlotReference("string_col", StringType.INSTANCE);
        StringLiteral pathLiteral = new StringLiteral("$.key");
        JsonbExtractString func = new JsonbExtractString(stringColumn, pathLiteral);

        FunctionSignature signature = func.getSignature();

        Assertions.assertEquals(StringType.INSTANCE, signature.returnType);
        Assertions.assertEquals(2, signature.argumentsTypes.size());
        // ExplicitlyCastableSignature matches first compatible signature (VARCHAR, VARCHAR)
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, signature.argumentsTypes.get(0));
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, signature.argumentsTypes.get(1));
    }

    @Test
    public void testWithChildren() {
        // Test withChildren method
        SlotReference jsonColumn = new SlotReference("json_col", JsonType.INSTANCE);
        VarcharLiteral pathLiteral = new VarcharLiteral("$.key");
        JsonbExtractString original = new JsonbExtractString(jsonColumn, pathLiteral);

        SlotReference newJsonColumn = new SlotReference("new_json_col", JsonType.INSTANCE);
        VarcharLiteral newPathLiteral = new VarcharLiteral("$.newKey");
        List<Expression> newChildren = ImmutableList.of(newJsonColumn, newPathLiteral);

        JsonbExtractString newFunc = original.withChildren(newChildren);

        Assertions.assertNotSame(original, newFunc);
        Assertions.assertEquals(2, newFunc.children().size());
        Assertions.assertEquals(newJsonColumn, newFunc.child(0));
        Assertions.assertEquals(newPathLiteral, newFunc.child(1));
    }

    @Test
    public void testWithChildrenInvalidSize() {
        // Test that withChildren throws exception for invalid number of children
        SlotReference jsonColumn = new SlotReference("json_col", JsonType.INSTANCE);
        VarcharLiteral pathLiteral = new VarcharLiteral("$.key");
        JsonbExtractString func = new JsonbExtractString(jsonColumn, pathLiteral);

        // Test with 1 child (should fail)
        List<Expression> oneChild = ImmutableList.of(jsonColumn);
        Assertions.assertThrows(IllegalArgumentException.class, () -> func.withChildren(oneChild));

        // Test with 3 children (should fail)
        List<Expression> threeChildren = ImmutableList.of(jsonColumn, pathLiteral, pathLiteral);
        Assertions.assertThrows(IllegalArgumentException.class, () -> func.withChildren(threeChildren));
    }

    @Test
    public void testGetSignatures() {
        // Test that getSignatures returns the SIGNATURES list
        SlotReference jsonColumn = new SlotReference("json_col", JsonType.INSTANCE);
        VarcharLiteral pathLiteral = new VarcharLiteral("$.key");
        JsonbExtractString func = new JsonbExtractString(jsonColumn, pathLiteral);

        List<FunctionSignature> signatures = func.getSignatures();

        Assertions.assertEquals(JsonbExtractString.SIGNATURES, signatures);
        Assertions.assertEquals(4, signatures.size());
    }

    @Test
    public void testFunctionName() {
        // Test that the function name is correct
        SlotReference jsonColumn = new SlotReference("json_col", JsonType.INSTANCE);
        VarcharLiteral pathLiteral = new VarcharLiteral("$.key");
        JsonbExtractString func = new JsonbExtractString(jsonColumn, pathLiteral);

        Assertions.assertEquals("jsonb_extract_string", func.getName());
    }

    @Test
    public void testSignatureOrderPreservation() {
        // Test that signatures are returned in the expected order
        List<FunctionSignature> signatures = JsonbExtractString.SIGNATURES;

        // First signature: (JSON, VARCHAR) -> STRING
        FunctionSignature sig0 = signatures.get(0);
        Assertions.assertEquals(JsonType.INSTANCE, sig0.argumentsTypes.get(0));
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, sig0.argumentsTypes.get(1));

        // Second signature: (JSON, STRING) -> STRING
        FunctionSignature sig1 = signatures.get(1);
        Assertions.assertEquals(JsonType.INSTANCE, sig1.argumentsTypes.get(0));
        Assertions.assertEquals(StringType.INSTANCE, sig1.argumentsTypes.get(1));

        // Third signature: (VARCHAR, VARCHAR) -> STRING (new)
        FunctionSignature sig2 = signatures.get(2);
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, sig2.argumentsTypes.get(0));
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, sig2.argumentsTypes.get(1));

        // Fourth signature: (STRING, STRING) -> STRING (new)
        FunctionSignature sig3 = signatures.get(3);
        Assertions.assertEquals(StringType.INSTANCE, sig3.argumentsTypes.get(0));
        Assertions.assertEquals(StringType.INSTANCE, sig3.argumentsTypes.get(1));

        // All signatures should return STRING
        for (FunctionSignature sig : signatures) {
            Assertions.assertEquals(StringType.INSTANCE, sig.returnType);
        }
    }

    @Test
    public void testRewriteWhenAnalyzeForJsonInput() {
        SlotReference jsonColumn = new SlotReference("json_col", JsonType.INSTANCE);
        VarcharLiteral pathLiteral = new VarcharLiteral("$.key");
        JsonbExtractString func = new JsonbExtractString(jsonColumn, pathLiteral);

        Expression rewritten = func.rewriteWhenAnalyze();

        Assertions.assertInstanceOf(Cast.class, rewritten);
        Assertions.assertInstanceOf(JsonbExtract.class, ((Cast) rewritten).child());
    }

    @Test
    public void testRewriteWhenAnalyzeForStringInput() {
        SlotReference stringColumn = new SlotReference("string_col", StringType.INSTANCE);
        StringLiteral pathLiteral = new StringLiteral("$.key");
        JsonbExtractString func = new JsonbExtractString(stringColumn, pathLiteral);

        Expression rewritten = func.rewriteWhenAnalyze();

        Assertions.assertSame(func, rewritten);
        Assertions.assertInstanceOf(JsonbExtractString.class, rewritten);
    }
}
