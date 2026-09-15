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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JsonbExtractStringTest {

    @Test
    public void testRewriteWhenAnalyzeJsonInputRewritesToCast() {
        SlotReference jsonColumn = new SlotReference("json_col", JsonType.INSTANCE);
        VarcharLiteral pathLiteral = new VarcharLiteral("$.key");
        JsonbExtractString func = new JsonbExtractString(jsonColumn, pathLiteral);

        Expression rewritten = func.rewriteWhenAnalyze();

        Assertions.assertInstanceOf(Cast.class, rewritten);
        Assertions.assertInstanceOf(JsonbExtract.class, ((Cast) rewritten).child());
    }

    @Test
    public void testRewriteWhenAnalyzeVarcharInputKeepsSelf() {
        SlotReference varcharColumn = new SlotReference("varchar_col", VarcharType.SYSTEM_DEFAULT);
        VarcharLiteral pathLiteral = new VarcharLiteral("$.key");
        JsonbExtractString func = new JsonbExtractString(varcharColumn, pathLiteral);

        Expression rewritten = func.rewriteWhenAnalyze();

        Assertions.assertSame(func, rewritten);
    }

    @Test
    public void testRewriteWhenAnalyzeStringInputKeepsSelf() {
        SlotReference stringColumn = new SlotReference("string_col", StringType.INSTANCE);
        StringLiteral pathLiteral = new StringLiteral("$.key");
        JsonbExtractString func = new JsonbExtractString(stringColumn, pathLiteral);

        Expression rewritten = func.rewriteWhenAnalyze();

        Assertions.assertSame(func, rewritten);
    }

    @Test
    public void testVarcharInputSelectsNonJsonSignature() {
        SlotReference varcharColumn = new SlotReference("varchar_col", VarcharType.SYSTEM_DEFAULT);
        VarcharLiteral pathLiteral = new VarcharLiteral("$.key");
        JsonbExtractString func = new JsonbExtractString(varcharColumn, pathLiteral);

        FunctionSignature signature = func.getSignature();

        Assertions.assertFalse(signature.getArgType(0).isJsonType());
        Assertions.assertEquals(StringType.INSTANCE, signature.returnType);
    }

    @Test
    public void testJsonInputSelectsJsonSignature() {
        SlotReference jsonColumn = new SlotReference("json_col", JsonType.INSTANCE);
        VarcharLiteral pathLiteral = new VarcharLiteral("$.key");
        JsonbExtractString func = new JsonbExtractString(jsonColumn, pathLiteral);

        FunctionSignature signature = func.getSignature();

        Assertions.assertTrue(signature.getArgType(0).isJsonType());
    }
}
