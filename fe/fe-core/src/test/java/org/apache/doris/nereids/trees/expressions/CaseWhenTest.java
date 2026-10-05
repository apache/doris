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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class CaseWhenTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        createTable("create table t1 (\n"
                + "    a int,\n"
                + "    b int\n"
                + ")\n"
                + "distributed by hash(a) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");
    }

    @Test
    void testSimpleCaseValueField() {
        IntegerLiteral value = new IntegerLiteral(1);
        WhenClause wc1 = new WhenClause(new IntegerLiteral(1), new IntegerLiteral(10));
        WhenClause wc2 = new WhenClause(new IntegerLiteral(2), new IntegerLiteral(20));
        IntegerLiteral defaultVal = new IntegerLiteral(99);

        // Simple case with value and default
        CaseWhen withValue = new CaseWhen(value, ImmutableList.of(wc1, wc2), defaultVal);
        Assertions.assertTrue(withValue.getValue().isPresent());
        Assertions.assertEquals(value, withValue.getValue().get());
        Assertions.assertEquals(2, withValue.getWhenClauses().size());
        Assertions.assertTrue(withValue.getDefaultValue().isPresent());
        Assertions.assertEquals(defaultVal, withValue.getDefaultValue().get());

        // Children layout: [value, WhenClause1, WhenClause2, defaultValue]
        List<Expression> children = withValue.children();
        Assertions.assertEquals(4, children.size());
        Assertions.assertEquals(value, children.get(0));
        Assertions.assertInstanceOf(WhenClause.class, children.get(1));
        Assertions.assertInstanceOf(WhenClause.class, children.get(2));
        Assertions.assertEquals(defaultVal, children.get(3));

        // Simple case without default
        CaseWhen withValueNoDefault = new CaseWhen(value, ImmutableList.of(wc1, wc2));
        Assertions.assertTrue(withValueNoDefault.getValue().isPresent());
        Assertions.assertFalse(withValueNoDefault.getDefaultValue().isPresent());
        Assertions.assertEquals(3, withValueNoDefault.children().size());

        // Searched case (no value)
        CaseWhen searchedCase = new CaseWhen(ImmutableList.of(wc1, wc2));
        Assertions.assertFalse(searchedCase.getValue().isPresent());
        Assertions.assertEquals(2, searchedCase.children().size());

        // Searched case with default
        CaseWhen searchedWithDefault = new CaseWhen(ImmutableList.of(wc1, wc2), defaultVal);
        Assertions.assertFalse(searchedWithDefault.getValue().isPresent());
        Assertions.assertTrue(searchedWithDefault.getDefaultValue().isPresent());
        Assertions.assertEquals(3, searchedWithDefault.children().size());
    }

    @Test
    void testSimpleCaseWithChildren() {
        IntegerLiteral value = new IntegerLiteral(1);
        WhenClause wc1 = new WhenClause(new IntegerLiteral(1), new IntegerLiteral(10));
        WhenClause wc2 = new WhenClause(new IntegerLiteral(2), new IntegerLiteral(20));
        IntegerLiteral defaultVal = new IntegerLiteral(99);

        // withChildren roundtrip for simple case with default
        CaseWhen original = new CaseWhen(value, ImmutableList.of(wc1, wc2), defaultVal);
        CaseWhen rebuilt = original.withChildren(original.children());
        Assertions.assertTrue(rebuilt.getValue().isPresent());
        Assertions.assertEquals(2, rebuilt.getWhenClauses().size());
        Assertions.assertTrue(rebuilt.getDefaultValue().isPresent());
        Assertions.assertEquals(original.children().size(), rebuilt.children().size());

        // withChildren roundtrip for simple case without default
        CaseWhen noDefault = new CaseWhen(value, ImmutableList.of(wc1, wc2));
        CaseWhen rebuiltNoDefault = noDefault.withChildren(noDefault.children());
        Assertions.assertTrue(rebuiltNoDefault.getValue().isPresent());
        Assertions.assertFalse(rebuiltNoDefault.getDefaultValue().isPresent());

        // withChildren roundtrip for searched case
        CaseWhen searched = new CaseWhen(ImmutableList.of(wc1, wc2), defaultVal);
        CaseWhen rebuiltSearched = searched.withChildren(searched.children());
        Assertions.assertFalse(rebuiltSearched.getValue().isPresent());
        Assertions.assertTrue(rebuiltSearched.getDefaultValue().isPresent());
        Assertions.assertEquals(2, rebuiltSearched.getWhenClauses().size());
    }

    @Test
    void testSimpleCaseToSql() {
        IntegerLiteral value = new IntegerLiteral(1);
        WhenClause wc1 = new WhenClause(new IntegerLiteral(1), new IntegerLiteral(10));
        WhenClause wc2 = new WhenClause(new IntegerLiteral(2), new IntegerLiteral(20));
        IntegerLiteral defaultVal = new IntegerLiteral(99);

        // Simple case: CASE value WHEN cond THEN result ... ELSE default END
        CaseWhen simpleCaseWhen = new CaseWhen(value, ImmutableList.of(wc1, wc2), defaultVal);
        String sql = simpleCaseWhen.toSql();
        Assertions.assertTrue(sql.startsWith("CASE 1"), "Simple case SQL should start with 'CASE 1', got: " + sql);
        Assertions.assertTrue(sql.contains("WHEN"), "SQL should contain WHEN: " + sql);
        Assertions.assertTrue(sql.contains("THEN"), "SQL should contain THEN: " + sql);
        Assertions.assertTrue(sql.contains("ELSE"), "SQL should contain ELSE: " + sql);
        Assertions.assertTrue(sql.endsWith("END"), "SQL should end with END: " + sql);

        // Searched case: CASE WHEN cond THEN result ... END
        CaseWhen searchedCaseWhen = new CaseWhen(ImmutableList.of(wc1, wc2));
        String searchedSql = searchedCaseWhen.toSql();
        Assertions.assertTrue(searchedSql.startsWith("CASE WHEN"),
                "Searched case SQL should start with 'CASE WHEN', got: " + searchedSql);
        Assertions.assertFalse(searchedSql.contains("ELSE"),
                "Searched case without default should not contain ELSE: " + searchedSql);

        // toString should behave similarly
        String str = simpleCaseWhen.toString();
        Assertions.assertTrue(str.startsWith("CASE 1"), "toString should start with 'CASE 1', got: " + str);
        Assertions.assertTrue(str.endsWith("END"), "toString should end with END: " + str);
    }

    @Test
    void testParseSimpleCase() {
        // Parse a simple case expression and verify it produces a CaseWhen with value
        String sql = "select case a when 1 then 2 else 3 end from t1";
        // Nereids planner is enabled by default; verify the SQL can be parsed and analyzed
        Assertions.assertDoesNotThrow(() -> getSQLPlanner(sql));
    }

    @Test
    void testSimpleCaseWithSubqueryPlanning() {
        // This is the key bug test: simple case with a subquery as value.
        // Previously, the parser would inline the value into each WhenClause
        // as EqualTo(subquery, literal), causing the same subquery RelationId
        // to appear multiple times, leading to "groupExpression already exists
        // in memo" errors during planning.
        String sql = "select case (select sum(b) from t1) "
                + "when 1 then 2 "
                + "when 3 then 4 "
                + "else 5 end from t1";
        // This should not throw - verifying it can be planned
        Assertions.assertDoesNotThrow(() -> getSQLPlanner(sql),
                "Simple case with subquery value should plan successfully without duplicate RelationId error");
    }

    @Test
    void testSearchedCaseStillWorks() {
        // Regression check: searched case (no value) should still work correctly
        String sql = "select case when a = 1 then 2 when a = 3 then 4 else 5 end from t1";
        Assertions.assertDoesNotThrow(() -> getSQLPlanner(sql),
                "Searched case should still work correctly");
    }

    @Test
    void testSimpleCaseWithColumnRef() {
        // Non-subquery simple case should also work
        String sql = "select case a when 1 then 'one' when 2 then 'two' else 'other' end from t1";
        Assertions.assertDoesNotThrow(() -> getSQLPlanner(sql),
                "Simple case with column reference should plan successfully");
    }
}
