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
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ResolveLanceIndexJobCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Parser coverage for RESOLVE LANCE INDEX JOB jobId AS FORCE_RELEASE COMMENT 'note': all
 * quote forms, plus the guarantee that the new RESOLVE and FORCE_RELEASE tokens stay
 * non-reserved and can still be used as identifiers. Malformed statements are rejected at
 * the grammar layer; note content validation (empty/over-long) lives in the command and is
 * covered in ResolveLanceIndexJobCommandTest.
 */
public class ResolveLanceIndexJobParserTest extends ParserTestBase {
    private final NereidsParser parser = new NereidsParser();

    private ResolveLanceIndexJobCommand parseResolve(String sql) {
        Plan plan = parser.parseSingle(sql);
        return Assertions.assertInstanceOf(ResolveLanceIndexJobCommand.class, plan);
    }

    @Test
    public void testResolveSingleQuotedComment() {
        ResolveLanceIndexJobCommand command = parseResolve(
                "RESOLVE LANCE INDEX JOB 42 AS FORCE_RELEASE COMMENT 'worker timed out'");
        Assertions.assertEquals(42L, command.getJobId());
        Assertions.assertEquals("worker timed out", command.getComment());
    }

    @Test
    public void testResolveDoubleQuotedComment() {
        ResolveLanceIndexJobCommand command = parseResolve(
                "RESOLVE LANCE INDEX JOB 42 AS FORCE_RELEASE COMMENT \"release the fence\"");
        Assertions.assertEquals(42L, command.getJobId());
        Assertions.assertEquals("release the fence", command.getComment());
    }

    @Test
    public void testResolveLargeJobId() {
        ResolveLanceIndexJobCommand command = parseResolve(
                "RESOLVE LANCE INDEX JOB 9223372036854775807 AS FORCE_RELEASE COMMENT 'note'");
        Assertions.assertEquals(Long.MAX_VALUE, command.getJobId());
        Assertions.assertEquals("note", command.getComment());
    }

    @Test
    public void testResolveCommentWithSpacesAndPunctuation() {
        String note = "note: worker-7 timed out; releasing fence (ticket #12).";
        ResolveLanceIndexJobCommand command = parseResolve(
                "RESOLVE LANCE INDEX JOB 7 AS FORCE_RELEASE COMMENT '" + note + "'");
        Assertions.assertEquals(7L, command.getJobId());
        Assertions.assertEquals(note, command.getComment());
    }

    @Test
    public void testResolveLowercaseParses() {
        // The lexer stream is case-insensitive; the new keywords must behave like the old ones.
        ResolveLanceIndexJobCommand command = parseResolve(
                "resolve lance index job 43 as force_release comment 'note'");
        Assertions.assertEquals(43L, command.getJobId());
        Assertions.assertEquals("note", command.getComment());
    }

    @Test
    public void testEmptyCommentStillParses() {
        // The grammar accepts an empty string literal; the command rejects it with
        // ERR_LANCE_INDEX_INVALID (covered in ResolveLanceIndexJobCommandTest).
        ResolveLanceIndexJobCommand command = parseResolve(
                "RESOLVE LANCE INDEX JOB 7 AS FORCE_RELEASE COMMENT ''");
        Assertions.assertEquals("", command.getComment());
    }

    @Test
    public void testResolveAndForceReleaseStayUsableAsIdentifiers() {
        // Both new tokens are non-reserved: they must still work as column names.
        Plan plan = parser.parseSingle("SELECT resolve FROM t");
        UnboundResultSink<?> sink = Assertions.assertInstanceOf(UnboundResultSink.class, plan);
        Assertions.assertInstanceOf(LogicalProject.class, sink.child());

        Assertions.assertNotNull(parser.parseSingle("SELECT force_release FROM t"));
        Assertions.assertNotNull(parser.parseSingle("SELECT resolve FROM t WHERE resolve > 0"));
        Assertions.assertNotNull(parser.parseSingle("SELECT t.force_release FROM t"));
    }

    @Test
    public void testResolveAndForceReleaseStayUsableAsAliases() {
        // Non-reserved keywords must also survive as table and column aliases.
        Assertions.assertNotNull(parser.parseSingle("SELECT 1 FROM t AS resolve"));
        Assertions.assertNotNull(parser.parseSingle("SELECT 1 FROM t resolve"));
        Assertions.assertNotNull(parser.parseSingle("SELECT 1 AS force_release FROM t"));
        Assertions.assertNotNull(parser.parseSingle("SELECT 1 FROM t AS force_release"));
    }

    @Test
    public void testMissingCommentClauseRejected() {
        // COMMENT is mandatory in the grammar, not merely defaulted in the command.
        Assertions.assertThrows(ParseException.class,
                () -> parser.parseSingle("RESOLVE LANCE INDEX JOB 42 AS FORCE_RELEASE"));
    }

    @Test
    public void testNonStringCommentRejected() {
        Assertions.assertThrows(ParseException.class,
                () -> parser.parseSingle("RESOLVE LANCE INDEX JOB 42 AS FORCE_RELEASE COMMENT 123"));
    }

    @Test
    public void testWrongAsLiteralRejected() {
        // The grammar pins the single token FORCE_RELEASE: a split or foreign spelling
        // must not parse.
        Assertions.assertThrows(ParseException.class,
                () -> parser.parseSingle("RESOLVE LANCE INDEX JOB 42 AS FORCE COMMENT 'x'"));
        Assertions.assertThrows(ParseException.class,
                () -> parser.parseSingle("RESOLVE LANCE INDEX JOB 42 AS FORCE RELEASE COMMENT 'x'"));
        Assertions.assertThrows(ParseException.class,
                () -> parser.parseSingle("RESOLVE LANCE INDEX JOB 42 AS RELEASE COMMENT 'x'"));
    }

    @Test
    public void testMissingAsForceReleaseRejected() {
        Assertions.assertThrows(ParseException.class,
                () -> parser.parseSingle("RESOLVE LANCE INDEX JOB 42 COMMENT 'x'"));
    }

    @Test
    public void testMissingJobIdRejected() {
        Assertions.assertThrows(ParseException.class,
                () -> parser.parseSingle("RESOLVE LANCE INDEX JOB AS FORCE_RELEASE COMMENT 'x'"));
    }
}
