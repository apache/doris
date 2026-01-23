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

import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for Search scalar function
 */
public class SearchTest {

    @Test
    public void testSearchFunctionCreation() {
        StringLiteral dslLiteral = new StringLiteral("title:hello");
        Search searchFunc = new Search(dslLiteral);

        Assertions.assertNotNull(searchFunc);
        Assertions.assertEquals("search", searchFunc.getName());
        Assertions.assertEquals(BooleanType.INSTANCE, searchFunc.getDataType());
        Assertions.assertEquals(1, searchFunc.children().size());
        Assertions.assertEquals(dslLiteral, searchFunc.children().get(0));
    }

    @Test
    public void testGetDslString() {
        String dsl = "content:\"machine learning\"";
        StringLiteral dslLiteral = new StringLiteral(dsl);
        Search searchFunc = new Search(dslLiteral);

        Assertions.assertEquals(dsl, searchFunc.getDslString());
    }

    @Test
    public void testGetQsPlan() {
        String dsl = "title:hello AND content:world";
        StringLiteral dslLiteral = new StringLiteral(dsl);
        Search searchFunc = new Search(dslLiteral);

        SearchDslParser.QsPlan plan = searchFunc.getQsPlan();
        Assertions.assertNotNull(plan);
        Assertions.assertNotNull(plan.getRoot());
        Assertions.assertEquals(SearchDslParser.QsClauseType.AND, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getFieldBindings().size());
    }

    @Test
    public void testWithChildren() {
        StringLiteral originalDsl = new StringLiteral("title:hello");
        StringLiteral newDsl = new StringLiteral("content:world");
        Search originalFunc = new Search(originalDsl);

        Search newFunc = (Search) originalFunc.withChildren(java.util.Arrays.asList(newDsl));

        Assertions.assertNotNull(newFunc);
        Assertions.assertEquals("content:world", newFunc.getDslString());
        Assertions.assertEquals(1, newFunc.children().size());
        Assertions.assertEquals(newDsl, newFunc.children().get(0));
    }

    @Test
    public void testFoldable() {
        StringLiteral dslLiteral = new StringLiteral("title:hello");
        Search searchFunc = new Search(dslLiteral);

        // Search function should not be foldable as it depends on table data
        Assertions.assertFalse(searchFunc.foldable());
    }

    @Test
    public void testNullable() {
        StringLiteral dslLiteral = new StringLiteral("title:hello");
        Search searchFunc = new Search(dslLiteral);

        // Search function implements AlwaysNotNullable
        Assertions.assertFalse(searchFunc.nullable());
    }

    @Test
    public void testEquals() {
        StringLiteral dsl1 = new StringLiteral("title:hello");
        StringLiteral dsl2 = new StringLiteral("title:hello");
        StringLiteral dsl3 = new StringLiteral("content:world");

        Search search1 = new Search(dsl1);
        Search search2 = new Search(dsl2);
        Search search3 = new Search(dsl3);

        Assertions.assertEquals(search1, search2);
        Assertions.assertEquals(search1.hashCode(), search2.hashCode());

        Assertions.assertFalse(search1.equals(search3));
    }

    @Test
    public void testToString() {
        StringLiteral dslLiteral = new StringLiteral("title:hello");
        Search searchFunc = new Search(dslLiteral);

        String str = searchFunc.toString();
        Assertions.assertTrue(str.contains("search"));
        Assertions.assertTrue(str.contains("title:hello"));
    }

    @Test
    public void testVisitorPattern() {
        StringLiteral dslLiteral = new StringLiteral("title:hello");
        Search searchFunc = new Search(dslLiteral);

        // Create a simple visitor that counts Search functions
        ExpressionVisitor<Integer, Void> visitor = new ExpressionVisitor<Integer, Void>() {
            @Override
            public Integer visit(org.apache.doris.nereids.trees.expressions.Expression expr, Void context) {
                return expr.accept(this, context);
            }

            @Override
            public Integer visitSearch(Search search, Void context) {
                return 1;
            }
        };

        Integer result = searchFunc.accept(visitor, null);
        Assertions.assertEquals(Integer.valueOf(1), result);
    }

    @Test
    public void testComplexDslParsing() {
        String complexDsl = "(title:\"machine learning\" OR content:AI) AND NOT category:spam";
        StringLiteral dslLiteral = new StringLiteral(complexDsl);
        Search searchFunc = new Search(dslLiteral);

        SearchDslParser.QsPlan plan = searchFunc.getQsPlan();
        Assertions.assertNotNull(plan);
        Assertions.assertEquals(SearchDslParser.QsClauseType.AND, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());

        // Should detect 3 unique fields: title, content, category
        Assertions.assertEquals(3, plan.getFieldBindings().size());
    }

    @Test
    public void testInvalidDslHandling() {
        String invalidDsl = "invalid:syntax AND";
        StringLiteral dslLiteral = new StringLiteral(invalidDsl);
        Search searchFunc = new Search(dslLiteral);

        // Should throw RuntimeException due to invalid DSL
        try {
            searchFunc.getQsPlan();
            Assertions.assertTrue(false, "Expected exception for invalid DSL");
        } catch (RuntimeException e) {
            Assertions.assertTrue(e.getMessage().contains("Invalid search DSL"));
        }
    }
}
