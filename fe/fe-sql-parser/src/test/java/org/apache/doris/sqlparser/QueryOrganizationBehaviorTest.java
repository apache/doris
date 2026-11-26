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

package org.apache.doris.sqlparser;

import org.apache.doris.nereids.DorisParser.QueryOrganizationContext;
import org.apache.doris.nereids.DorisParser.SingleStatementContext;
import org.apache.doris.nereids.exceptions.ParseException;

import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

class QueryOrganizationBehaviorTest {

    @ParameterizedTest(name = "owners: {0}, ansi={1}")
    @MethodSource("organizationOwners")
    void createsOneNonEmptyOwnerPerClauseGroup(String description, boolean ansi, String sql, int expectedOwners) {
        SingleStatementContext statement = new DorisSqlParser(false, ansi).parseStatement(sql);
        Assertions.assertEquals(expectedOwners, countOrganizationOwners(statement));
    }

    private static Stream<Arguments> organizationOwners() {
        return Stream.of(
                Arguments.of("plain select", false, "SELECT 1", 0),
                Arguments.of("plain select", true, "SELECT 1", 0),
                Arguments.of("ordered select", false, "SELECT 1 ORDER BY 1", 1),
                Arguments.of("ordered select", true, "SELECT 1 ORDER BY 1", 1),
                Arguments.of("set tail", false, "SELECT 1 UNION ALL SELECT 2 LIMIT 1", 1),
                Arguments.of("set tail", true, "SELECT 1 UNION ALL SELECT 2 LIMIT 1", 1),
                Arguments.of("nested clauses", false, "(SELECT 1 ORDER BY 1) LIMIT 1", 2),
                Arguments.of("nested clauses", true, "(SELECT 1 ORDER BY 1) LIMIT 1", 2),
                Arguments.of("legacy split clauses", false, "SELECT 1 LIMIT 1 ORDER BY 1", 2));
    }

    private static int countOrganizationOwners(ParseTree tree) {
        int owners = 0;
        if (tree instanceof QueryOrganizationContext) {
            QueryOrganizationContext organization = (QueryOrganizationContext) tree;
            Assertions.assertTrue(organization.sortClause() != null || organization.limitClause() != null);
            owners++;
        }
        for (int index = 0; index < tree.getChildCount(); index++) {
            owners += countOrganizationOwners(tree.getChild(index));
        }
        return owners;
    }

    @ParameterizedTest(name = "{0}, ansi={1}")
    @MethodSource("acceptedStatements")
    void preservesAcceptedQueryOrganizationForms(String description, boolean ansi, String sql) {
        Assertions.assertNotNull(new DorisSqlParser(false, ansi).parseStatement(sql));
    }

    private static Stream<Arguments> acceptedStatements() {
        return Stream.of(
                Arguments.of("ordered select", false, "SELECT 1 ORDER BY 1"),
                Arguments.of("ordered select", true, "SELECT 1 ORDER BY 1"),
                Arguments.of("simple limit", false, "SELECT 1 LIMIT 3"),
                Arguments.of("simple limit", true, "SELECT 1 LIMIT 3"),
                Arguments.of("limit offset", false, "SELECT 1 LIMIT 3 OFFSET 2"),
                Arguments.of("limit offset", true, "SELECT 1 LIMIT 3 OFFSET 2"),
                Arguments.of("comma limit", false, "SELECT 1 LIMIT 2, 3"),
                Arguments.of("comma limit", true, "SELECT 1 LIMIT 2, 3"),
                Arguments.of("union tail", false, "SELECT 1 UNION ALL SELECT 2 LIMIT 1"),
                Arguments.of("union tail", true, "SELECT 1 UNION ALL SELECT 2 LIMIT 1"),
                Arguments.of("intersect tail", false, "SELECT 1 INTERSECT SELECT 2 ORDER BY 1"),
                Arguments.of("intersect tail", true, "SELECT 1 INTERSECT SELECT 2 ORDER BY 1"),
                Arguments.of("except tail", false, "SELECT 1 EXCEPT SELECT 2 LIMIT 1"),
                Arguments.of("except tail", true, "SELECT 1 EXCEPT SELECT 2 LIMIT 1"),
                Arguments.of("parenthesized set", false,
                        "(SELECT 1 UNION ALL SELECT 2) ORDER BY 1 LIMIT 1"),
                Arguments.of("parenthesized set", true,
                        "(SELECT 1 UNION ALL SELECT 2) ORDER BY 1 LIMIT 1"),
                Arguments.of("parenthesized operand", false,
                        "SELECT 1 UNION ALL (SELECT 2 ORDER BY 1 LIMIT 1)"),
                Arguments.of("parenthesized operand", true,
                        "SELECT 1 UNION ALL (SELECT 2 ORDER BY 1 LIMIT 1)"),
                Arguments.of("inline values", false,
                        "VALUES (1), (2) ORDER BY 1 LIMIT 1"),
                Arguments.of("inline values", true,
                        "VALUES (1), (2) ORDER BY 1 LIMIT 1"),
                Arguments.of("CTE", false,
                        "WITH c AS (SELECT 1 ORDER BY 1 LIMIT 1) SELECT * FROM c LIMIT 1"),
                Arguments.of("CTE", true,
                        "WITH c AS (SELECT 1 ORDER BY 1 LIMIT 1) SELECT * FROM c LIMIT 1"),
                Arguments.of("derived query", false,
                        "SELECT * FROM (SELECT 1 ORDER BY 1 LIMIT 1) t ORDER BY 1"),
                Arguments.of("derived query", true,
                        "SELECT * FROM (SELECT 1 ORDER BY 1 LIMIT 1) t ORDER BY 1"),
                Arguments.of("IN subquery", false,
                        "SELECT * FROM t WHERE id IN (SELECT id FROM t ORDER BY id LIMIT 1)"),
                Arguments.of("IN subquery", true,
                        "SELECT * FROM t WHERE id IN (SELECT id FROM t ORDER BY id LIMIT 1)"),
                Arguments.of("legacy operand clauses", false,
                        "SELECT 1 ORDER BY 1 UNION ALL SELECT 2 LIMIT 1"),
                Arguments.of("legacy split clauses", false,
                        "SELECT 1 LIMIT 1 ORDER BY 1"));
    }

    @ParameterizedTest(name = "rejects: {0}, ansi={1}")
    @MethodSource("rejectedStatements")
    void preservesRejectedQueryOrganizationForms(String description, boolean ansi, String sql, int errorPosition) {
        ParseException exception = Assertions.assertThrows(ParseException.class,
                () -> new DorisSqlParser(false, ansi).parseStatement(sql));
        Assertions.assertTrue(exception.getMessage().contains("line 1, pos " + errorPosition), exception::getMessage);
    }

    private static Stream<Arguments> rejectedStatements() {
        return Stream.of(
                Arguments.of("ANSI unparenthesized operand order", true,
                        "SELECT 1 ORDER BY 1 UNION ALL SELECT 2", 9),
                Arguments.of("order after limit", true, "SELECT 1 LIMIT 1 ORDER BY 1", 9),
                Arguments.of("truncated order", false, "SELECT 1 ORDER BY", 9),
                Arguments.of("truncated order", true, "SELECT 1 ORDER BY", 17),
                Arguments.of("truncated limit", false, "SELECT 1 LIMIT", 9),
                Arguments.of("truncated limit", true, "SELECT 1 LIMIT", 14),
                Arguments.of("truncated trailing limit", false, "SELECT 1 ORDER BY 1 LIMIT", 20),
                Arguments.of("truncated trailing limit", true, "SELECT 1 ORDER BY 1 LIMIT", 20),
                Arguments.of("truncated VALUES limit", false, "VALUES (1) ORDER BY 1 LIMIT", 22),
                Arguments.of("truncated VALUES limit", true, "VALUES (1) ORDER BY 1 LIMIT", 22),
                Arguments.of("truncated legacy second clause", false, "SELECT 1 LIMIT 1 ORDER BY", 25));
    }
}
