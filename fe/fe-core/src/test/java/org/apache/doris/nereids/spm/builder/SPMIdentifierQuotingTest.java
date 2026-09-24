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

package org.apache.doris.nereids.spm.builder;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Identifier quoting of the frozen SQL: every metadata component (catalog / db / table /
 * column) is emitted as an identifier, never as raw SQL text. Doris allows quoted names
 * containing operators, so a column {@code `a-b`} frozen as {@code SELECT a-b} would be
 * re-parsed at replay as the subtraction {@code a - b} and could return a different
 * value; embedded backticks are doubled so the quoted form round-trips.
 *
 * Plain names stay verbatim so ordinary schemas keep byte-identical frozen SQL.
 */
public class SPMIdentifierQuotingTest {

    @Test
    public void testPlainIdentifiersStayVerbatim() {
        Assertions.assertEquals("abc", SPMPlan2SQLBuilder.quoteIdentifier("abc"));
        Assertions.assertEquals("_a9", SPMPlan2SQLBuilder.quoteIdentifier("_a9"));
        Assertions.assertNull(SPMPlan2SQLBuilder.quoteIdentifier(null));
    }

    @Test
    public void testSpecialCharacterIdentifiersAreQuoted() {
        Assertions.assertEquals("`a-b`", SPMPlan2SQLBuilder.quoteIdentifier("a-b"),
                "a minus sign must not survive as an operator");
        Assertions.assertEquals("`a b`", SPMPlan2SQLBuilder.quoteIdentifier("a b"));
        Assertions.assertEquals("`9col`", SPMPlan2SQLBuilder.quoteIdentifier("9col"),
                "a name starting with a digit is not a plain identifier");
        Assertions.assertEquals("`a+b`", SPMPlan2SQLBuilder.quoteIdentifier("a+b"));
    }

    @Test
    public void testEmbeddedBacktickIsDoubled() {
        Assertions.assertEquals("`a``b`", SPMPlan2SQLBuilder.quoteIdentifier("a`b"),
                "an embedded backtick must be escaped by doubling");
    }

    @Test
    public void testQualifiedNameQuotesEveryComponent() {
        Assertions.assertEquals("internal.db1.t1",
                SPMPlan2SQLBuilder.quoteQualifiedName("internal.db1.t1"),
                "plain components stay verbatim");
        Assertions.assertEquals("internal.db1.`my-table`",
                SPMPlan2SQLBuilder.quoteQualifiedName("internal.db1.my-table"));
        Assertions.assertEquals("`my-cat`.`my-db`.`my-table`",
                SPMPlan2SQLBuilder.quoteQualifiedName("my-cat.my-db.my-table"),
                "every component is quoted independently");
        Assertions.assertEquals("t1", SPMPlan2SQLBuilder.quoteQualifiedName("t1"));
        Assertions.assertNull(SPMPlan2SQLBuilder.quoteQualifiedName(null));
        Assertions.assertEquals("", SPMPlan2SQLBuilder.quoteQualifiedName(""));
    }
}
