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
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class JsonExtractNoQuotesTest {

    @Test
    public void testConstructorPreservesNameAndChildren() {
        StringLiteral json = new StringLiteral("{\"a\":1}");
        StringLiteral path = new StringLiteral("$.a");
        JsonExtractNoQuotes fn = new JsonExtractNoQuotes(json, path);

        Assertions.assertEquals("json_extract_no_quotes", fn.getName());
        Assertions.assertEquals(2, fn.arity());
        Assertions.assertSame(json, fn.child(0));
        Assertions.assertSame(path, fn.child(1));
    }

    @Test
    public void testConstructorAcceptsExtraVarArgs() {
        StringLiteral json = new StringLiteral("{\"a\":{\"b\":1}}");
        StringLiteral path0 = new StringLiteral("$.a");
        StringLiteral path1 = new StringLiteral("$.a.b");
        StringLiteral path2 = new StringLiteral("$.a.b.c");

        JsonExtractNoQuotes fn = new JsonExtractNoQuotes(json, path0, path1, path2);
        Assertions.assertEquals(4, fn.arity());
        Assertions.assertSame(path2, fn.child(3));
    }

    @Test
    public void testGetSignaturesMatchesReturnAndArgs() {
        JsonExtractNoQuotes fn = new JsonExtractNoQuotes(
                new StringLiteral("{}"), new StringLiteral("$.a"));

        List<FunctionSignature> signatures = fn.getSignatures();
        Assertions.assertEquals(1, signatures.size());
        FunctionSignature signature = signatures.get(0);
        Assertions.assertEquals(JsonType.INSTANCE, signature.returnType);
        Assertions.assertTrue(signature.hasVarArgs);
        // First arg is JSON, remaining var args are VARCHAR paths.
        Assertions.assertEquals(JsonType.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, signature.argumentsTypes.get(1));
    }

    @Test
    public void testAlwaysNullableTrait() {
        JsonExtractNoQuotes fn = new JsonExtractNoQuotes(
                new StringLiteral("{}"), new StringLiteral("$.a"));

        Assertions.assertTrue(fn instanceof AlwaysNullable);
        Assertions.assertTrue(fn instanceof ExplicitlyCastableSignature);
        Assertions.assertTrue(fn.nullable());
    }

    @Test
    public void testWithChildrenKeepsNameAndReplacesChildren() {
        JsonExtractNoQuotes original = new JsonExtractNoQuotes(
                new StringLiteral("{\"a\":1}"), new StringLiteral("$.a"));

        StringLiteral newJson = new StringLiteral("{\"b\":2}");
        StringLiteral newPath = new StringLiteral("$.b");
        JsonExtractNoQuotes rebuilt = original.withChildren(ImmutableList.of(newJson, newPath));

        Assertions.assertNotSame(original, rebuilt);
        Assertions.assertEquals("json_extract_no_quotes", rebuilt.getName());
        Assertions.assertEquals(2, rebuilt.arity());
        Assertions.assertSame(newJson, rebuilt.child(0));
        Assertions.assertSame(newPath, rebuilt.child(1));
    }

    @Test
    public void testWithChildrenRejectsLessThanTwoArgs() {
        JsonExtractNoQuotes original = new JsonExtractNoQuotes(
                new StringLiteral("{\"a\":1}"), new StringLiteral("$.a"));

        // Preconditions.checkArgument(children.size() >= 2) — dropping a child violates the
        // varargs signature (which requires at least the JSON value + one path), so we must
        // fail fast rather than silently return a malformed expression.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> original.withChildren(ImmutableList.of(new StringLiteral("{}"))));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> original.withChildren(ImmutableList.of()));
    }

    @Test
    public void testAcceptDispatchesToJsonExtractNoQuotesVisitor() {
        JsonExtractNoQuotes fn = new JsonExtractNoQuotes(
                new StringLiteral("{}"), new StringLiteral("$.a"));

        RecordingVisitor visitor = new RecordingVisitor();
        String result = fn.accept(visitor, "ctx");
        Assertions.assertEquals("visited:ctx", result);
        Assertions.assertSame(fn, visitor.captured);
    }

    private static final class RecordingVisitor extends ExpressionVisitor<String, String> {
        private JsonExtractNoQuotes captured;

        @Override
        public String visit(Expression expr, String context) {
            return "fallback:" + context;
        }

        @Override
        public String visitJsonExtractNoQuotes(JsonExtractNoQuotes expr, String context) {
            captured = expr;
            return "visited:" + context;
        }
    }
}
