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
        Assertions.assertNotNull(command.getWhereClause());

        command = parseJobs("SHOW LANCE INDEX JOBS WHERE State = \"PENDING\"");
        Assertions.assertNotNull(command.getWhereClause());

        command = parseJobs("SHOW LANCE INDEX JOBS FROM ctl1.db1 "
                + "WHERE TableName = \"tbl1\" AND State = \"PENDING\"");
        Assertions.assertEquals(Arrays.asList("ctl1", "db1"), command.getNameParts());
        Assertions.assertNotNull(command.getWhereClause());
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
}
