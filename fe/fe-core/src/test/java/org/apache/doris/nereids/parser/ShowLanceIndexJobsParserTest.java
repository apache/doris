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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ShowLanceIndexJobCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowLanceIndexJobsCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * Parser coverage for SHOW LANCE INDEX JOBS / SHOW LANCE INDEX JOB: all FROM and WHERE
 * forms, plus the guarantee that the new LANCE token stays non-reserved and can still be
 * used as an identifier. Illegal WHERE shapes that the grammar accepts as generic
 * expressions are rejected by the command, covered in ShowLanceIndexJobsCommandTest.
 */
public class ShowLanceIndexJobsParserTest extends ParserTestBase {
    private final NereidsParser parser = new NereidsParser();

    private ShowLanceIndexJobsCommand parseJobs(String sql) {
        Plan plan = parser.parseSingle(sql);
        return Assertions.assertInstanceOf(ShowLanceIndexJobsCommand.class, plan);
    }

    private static void assertEqualToPredicate(Expression where, String slotName, String literalValue) {
        EqualTo equalTo = Assertions.assertInstanceOf(EqualTo.class, where);
        UnboundSlot slot = Assertions.assertInstanceOf(UnboundSlot.class, equalTo.child(0));
        Assertions.assertEquals(Arrays.asList(slotName), slot.getNameParts());
        // The parser hands out VarcharLiteral for ordinary-length strings; assert the common
        // string-literal base so the test states the grammar contract, not the length heuristic.
        StringLikeLiteral literal = Assertions.assertInstanceOf(StringLikeLiteral.class, equalTo.child(1));
        Assertions.assertEquals(literalValue, literal.getStringValue());
    }

    @Test
    public void testShowLanceIndexJobsBare() {
        ShowLanceIndexJobsCommand command = parseJobs("SHOW LANCE INDEX JOBS");
        Assertions.assertNull(command.getNameParts());
        Assertions.assertNull(command.getWhereClause());
    }

    @Test
    public void testShowLanceIndexJobsFromDb() {
        ShowLanceIndexJobsCommand command = parseJobs("SHOW LANCE INDEX JOBS FROM db1");
        Assertions.assertEquals(Arrays.asList("db1"), command.getNameParts());

        command = parseJobs("SHOW LANCE INDEX JOBS IN db1");
        Assertions.assertEquals(Arrays.asList("db1"), command.getNameParts());
    }

    @Test
    public void testShowLanceIndexJobsFromCatalogDb() {
        ShowLanceIndexJobsCommand command = parseJobs("SHOW LANCE INDEX JOBS FROM ctl1.db1");
        Assertions.assertEquals(Arrays.asList("ctl1", "db1"), command.getNameParts());
    }

    @Test
    public void testShowLanceIndexJobsWhere() {
        ShowLanceIndexJobsCommand command = parseJobs("SHOW LANCE INDEX JOBS WHERE TableName = \"tbl1\"");
        assertEqualToPredicate(command.getWhereClause(), "TableName", "tbl1");

        command = parseJobs("SHOW LANCE INDEX JOBS WHERE State = \"PENDING\"");
        assertEqualToPredicate(command.getWhereClause(), "State", "PENDING");
    }

    @Test
    public void testShowLanceIndexJobsWhereLikeParses() {
        // The LIKE syntax parses into a Like expression; rejecting it is a command-level
        // narrowing (ShowLanceIndexJobsCommandTest covers the rejection with hand-built
        // expressions), so this pins the grammar side of that split.
        ShowLanceIndexJobsCommand command = parseJobs("SHOW LANCE INDEX JOBS WHERE TableName LIKE \"t%\"");
        Like like = Assertions.assertInstanceOf(Like.class, command.getWhereClause());
        UnboundSlot slot = Assertions.assertInstanceOf(UnboundSlot.class, like.child(0));
        Assertions.assertEquals(Arrays.asList("TableName"), slot.getNameParts());
        StringLikeLiteral pattern = Assertions.assertInstanceOf(StringLikeLiteral.class, like.child(1));
        Assertions.assertEquals("t%", pattern.getStringValue());
    }

    @Test
    public void testShowLanceIndexJobsWhereReversedLiteralParses() {
        // A generic boolean expression: the grammar does not order slot vs literal, the command
        // does (the reversed shape is one of the WHERE forms the command rejects).
        ShowLanceIndexJobsCommand command = parseJobs("SHOW LANCE INDEX JOBS WHERE \"t\" = TableName");
        EqualTo equalTo = Assertions.assertInstanceOf(EqualTo.class, command.getWhereClause());
        StringLikeLiteral literal = Assertions.assertInstanceOf(StringLikeLiteral.class, equalTo.child(0));
        Assertions.assertEquals("t", literal.getStringValue());
        UnboundSlot slot = Assertions.assertInstanceOf(UnboundSlot.class, equalTo.child(1));
        Assertions.assertEquals(Arrays.asList("TableName"), slot.getNameParts());
    }

    @Test
    public void testShowLanceIndexJobsInCatalogDbWithCombinedWhere() {
        ShowLanceIndexJobsCommand command = parseJobs("SHOW LANCE INDEX JOBS IN ctl1.db1 "
                + "WHERE TableName = \"t\" AND State = \"PENDING\"");
        Assertions.assertEquals(Arrays.asList("ctl1", "db1"), command.getNameParts());
        And where = Assertions.assertInstanceOf(And.class, command.getWhereClause());
        assertEqualToPredicate(where.child(0), "TableName", "t");
        assertEqualToPredicate(where.child(1), "State", "PENDING");
    }

    @Test
    public void testShowLanceIndexJobsLowercaseParses() {
        // The lexer stream is case-insensitive; the new keywords must behave like the old ones.
        ShowLanceIndexJobsCommand command = parseJobs(
                "show lance index jobs from ctl1.db1 where TableName = \"t\" and State = \"pending\"");
        Assertions.assertEquals(Arrays.asList("ctl1", "db1"), command.getNameParts());
        Assertions.assertNotNull(command.getWhereClause());
    }

    @Test
    public void testShowLanceIndexJobParses() {
        Plan plan = parser.parseSingle("SHOW LANCE INDEX JOB 123");
        ShowLanceIndexJobCommand command = Assertions.assertInstanceOf(ShowLanceIndexJobCommand.class, plan);
        Assertions.assertEquals(123L, command.getJobId());
    }

    @Test
    public void testFromWithTooManyPartsStillParses() {
        // The grammar accepts any multipart identifier; the command rejects more than two parts.
        ShowLanceIndexJobsCommand command = parseJobs("SHOW LANCE INDEX JOBS FROM ctl1.db1.tbl1");
        Assertions.assertEquals(3, command.getNameParts().size());
    }

    @Test
    public void testIllegalWhereShapesStillParse() {
        // The grammar parses any boolean expression; shape validation lives in the command.
        Assertions.assertNotNull(parseJobs(
                "SHOW LANCE INDEX JOBS WHERE TableName = \"t\" OR State = \"PENDING\"").getWhereClause());
        Assertions.assertNotNull(parseJobs(
                "SHOW LANCE INDEX JOBS WHERE Foo = \"t\"").getWhereClause());
        Assertions.assertNotNull(parseJobs(
                "SHOW LANCE INDEX JOBS WHERE TableName = \"t\" AND tablename = \"t\"").getWhereClause());
        Assertions.assertNotNull(parseJobs(
                "SHOW LANCE INDEX JOBS WHERE State = \"BOGUS\"").getWhereClause());
    }

    @Test
    public void testLanceStaysUsableAsIdentifier() {
        // LANCE is added as a non-reserved keyword: it must still work as a column name.
        Plan plan = parser.parseSingle("SELECT lance FROM t");
        UnboundResultSink<?> sink = Assertions.assertInstanceOf(UnboundResultSink.class, plan);
        Assertions.assertInstanceOf(LogicalProject.class, sink.child());

        Assertions.assertNotNull(parser.parseSingle("SELECT lance FROM t WHERE lance > 0"));
    }

    @Test
    public void testLanceStaysUsableAsDatabaseAndCatalogName() {
        // Non-reserved in qualified names too: db and catalog parts named lance parse as such,
        // not as the LANCE token of the SHOW LANCE INDEX JOBS prefix.
        ShowLanceIndexJobsCommand command = parseJobs("SHOW LANCE INDEX JOBS FROM lance");
        Assertions.assertEquals(Arrays.asList("lance"), command.getNameParts());

        command = parseJobs("SHOW LANCE INDEX JOBS FROM lance.lance");
        Assertions.assertEquals(Arrays.asList("lance", "lance"), command.getNameParts());
    }
}
