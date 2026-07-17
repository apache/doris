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

import org.apache.doris.analysis.ColumnPath;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.AlterTableCommand;
import org.apache.doris.nereids.trees.plans.commands.info.AddColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.AlterTableOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyColumnCommentOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.RenameColumnOp;
import org.apache.doris.qe.SqlModeHelper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

public class IcebergNestedSchemaEvolutionParserTest {

    private final NereidsParser parser = new NereidsParser();

    @Test
    public void testParseNestedAddColumnPaths() {
        assertSingleClausePath("ALTER TABLE t ADD COLUMN s.c STRING NULL",
                AddColumnOp.class, "s.c");
        assertSingleClausePath("ALTER TABLE t ADD COLUMN s.first_col INT NULL FIRST",
                AddColumnOp.class, "s.first_col");
        assertSingleClausePath("ALTER TABLE t ADD COLUMN s.after_col INT NULL AFTER a",
                AddColumnOp.class, "s.after_col");
        assertSingleClausePath("ALTER TABLE t ADD COLUMN arr.element.y INT NULL",
                AddColumnOp.class, "arr.element.y");
        assertSingleClausePath("ALTER TABLE t ADD COLUMN m.value.y INT NULL",
                AddColumnOp.class, "m.value.y");
    }

    @Test
    public void testParseNestedModifyDropRenamePaths() {
        assertSingleClausePath("ALTER TABLE t MODIFY COLUMN s.a BIGINT",
                ModifyColumnOp.class, "s.a");
        assertSingleClausePath("ALTER TABLE t MODIFY COLUMN s.a BIGINT AFTER b",
                ModifyColumnOp.class, "s.a");
        assertSingleClausePath("ALTER TABLE t MODIFY COLUMN arr.element BIGINT",
                ModifyColumnOp.class, "arr.element");
        assertSingleClausePath("ALTER TABLE t MODIFY COLUMN m.value BIGINT",
                ModifyColumnOp.class, "m.value");
        assertSingleClausePath("ALTER TABLE t MODIFY COLUMN s.a COMMENT 'nested comment'",
                ModifyColumnCommentOp.class, "s.a");
        assertSingleClausePath("ALTER TABLE t DROP COLUMN s.c",
                DropColumnOp.class, "s.c");
        assertSingleClausePath("ALTER TABLE t DROP COLUMN arr.element.y",
                DropColumnOp.class, "arr.element.y");
        assertSingleClausePath("ALTER TABLE t DROP COLUMN m.value.y",
                DropColumnOp.class, "m.value.y");
        assertSingleClausePath("ALTER TABLE t RENAME COLUMN s.c TO c2",
                RenameColumnOp.class, "s.c");
        assertSingleClausePath("ALTER TABLE t RENAME COLUMN arr.element.y TO y2",
                RenameColumnOp.class, "arr.element.y");
        assertSingleClausePath("ALTER TABLE t RENAME COLUMN m.value.y TO y2",
                RenameColumnOp.class, "m.value.y");
    }

    @Test
    public void testLegacyStringConstructorsKeepDottedTopLevelNames() {
        DropColumnOp drop = new DropColumnOp("top.level", null, Collections.emptyMap());
        RenameColumnOp rename = new RenameColumnOp("top.level", "renamed");
        ModifyColumnCommentOp comment = new ModifyColumnCommentOp("top.level", "comment");

        Assertions.assertFalse(drop.getColumnPath().isNested());
        Assertions.assertFalse(rename.getColumnPath().isNested());
        Assertions.assertFalse(comment.getColumnPath().isNested());
        Assertions.assertEquals("top.level", drop.getColumnPath().getFullPath());
        Assertions.assertEquals("top.level", rename.getColumnPath().getFullPath());
        Assertions.assertEquals("top.level", comment.getColumnPath().getFullPath());
    }

    @Test
    public void testQuotedNestedIdentifiersAreNormalized() {
        assertSingleClausePath("ALTER TABLE t MODIFY COLUMN info.`Metric` BIGINT",
                ModifyColumnOp.class, "info.Metric");
        assertSingleClausePath("ALTER TABLE t MODIFY COLUMN m_scalar.`key` BIGINT",
                ModifyColumnOp.class, "m_scalar.key");
        assertSingleClausePath("ALTER TABLE t MODIFY COLUMN info.`Metric``Name` COMMENT 'quoted'",
                ModifyColumnCommentOp.class, "info.Metric`Name");

        AddColumnOp add = assertSingleClausePath(
                "ALTER TABLE t ADD COLUMN info.`New``Field` INT NULL AFTER `Old``Field`",
                AddColumnOp.class, "info.New`Field");
        Assertions.assertEquals("Old`Field", add.getColPos().getLastCol());
        AddColumnOp reparsedAdd = assertSingleClausePath(
                "ALTER TABLE t " + add.toSql(), AddColumnOp.class, "info.New`Field");
        Assertions.assertEquals("Old`Field", reparsedAdd.getColPos().getLastCol());

        RenameColumnOp rename = assertSingleClausePath(
                "ALTER TABLE t RENAME COLUMN info.`Metric``Name` TO `New``Metric`",
                RenameColumnOp.class, "info.Metric`Name");
        Assertions.assertEquals("New`Metric", rename.getNewColName());
        RenameColumnOp reparsedRename = assertSingleClausePath(
                "ALTER TABLE t " + rename.toSql(), RenameColumnOp.class, "info.Metric`Name");
        Assertions.assertEquals("New`Metric", reparsedRename.getNewColName());
    }

    @Test
    public void testModifyColumnRoundTripPreservesNullabilityIntent() {
        ModifyColumnOp omitted = assertSingleClausePath(
                "ALTER TABLE t MODIFY COLUMN info.metric BIGINT",
                ModifyColumnOp.class, "info.metric");
        Assertions.assertFalse(omitted.getColumnDef()
                .translateToCatalogStyleForSchemaChange().isNullableSpecified());
        Assertions.assertFalse(omitted.toSql().contains(" BIGINT NULL "));

        ModifyColumnOp reparsedOmitted = assertSingleClausePath(
                "ALTER TABLE t " + omitted.toSql(), ModifyColumnOp.class, "info.metric");
        Assertions.assertFalse(reparsedOmitted.getColumnDef()
                .translateToCatalogStyleForSchemaChange().isNullableSpecified());

        ModifyColumnOp nullable = assertSingleClausePath(
                "ALTER TABLE t MODIFY COLUMN info.metric BIGINT NULL",
                ModifyColumnOp.class, "info.metric");
        ModifyColumnOp reparsedNullable = assertSingleClausePath(
                "ALTER TABLE t " + nullable.toSql(), ModifyColumnOp.class, "info.metric");
        Assertions.assertTrue(reparsedNullable.getColumnDef()
                .translateToCatalogStyleForSchemaChange().isNullableSpecified());
    }

    @Test
    public void testModifyColumnCommentRoundTripEscapesQuotesAndBackslashes() {
        assertCommentRoundTrip(false);
        assertCommentRoundTrip(true);
    }

    @Test
    public void testColumnDefinitionWithPathDecodesDefaultAndCommentLiterals() {
        assertColumnDefinitionLiteralDecoding(false);
        assertColumnDefinitionLiteralDecoding(true);
    }

    private void assertColumnDefinitionLiteralDecoding(boolean noBackslashEscapes) {
        try (MockedStatic<SqlModeHelper> mockedSqlMode = Mockito.mockStatic(SqlModeHelper.class)) {
            mockedSqlMode.when(SqlModeHelper::hasNoBackSlashEscapes).thenReturn(noBackslashEscapes);

            String sqlPath = noBackslashEscapes ? "C:\\tmp\\" : "C:\\\\tmp\\\\";
            AddColumnOp add = assertSingleClausePath(
                    "ALTER TABLE t ADD COLUMN info.owner STRING DEFAULT 'owner''s " + sqlPath
                            + "' COMMENT \"a\"\"b " + sqlPath + "\"",
                    AddColumnOp.class, "info.owner");

            Assertions.assertEquals("owner's C:\\tmp\\", add.getColumnDef()
                    .translateToCatalogStyleForSchemaChange().getDefaultValue());
            Assertions.assertEquals("a\"b C:\\tmp\\", add.getColumnDef().getComment());
        }
    }

    private void assertCommentRoundTrip(boolean noBackslashEscapes) {
        try (MockedStatic<SqlModeHelper> mockedSqlMode = Mockito.mockStatic(SqlModeHelper.class)) {
            mockedSqlMode.when(SqlModeHelper::hasNoBackSlashEscapes).thenReturn(noBackslashEscapes);

            String expectedComment = "owner's \"field\" C:\\tmp\\";
            ModifyColumnCommentOp comment = new ModifyColumnCommentOp(
                    ColumnPath.fromDotName("info.metric"), expectedComment);
            String renderedSql = comment.toSql();
            Assertions.assertTrue(renderedSql.contains("\"\"field\"\""));
            Assertions.assertTrue(renderedSql.contains(noBackslashEscapes
                    ? "C:\\tmp\\" : "C:\\\\tmp\\\\"));

            ModifyColumnCommentOp reparsed = assertSingleClausePath(
                    "ALTER TABLE t " + renderedSql, ModifyColumnCommentOp.class, "info.metric");
            Assertions.assertEquals(expectedComment, reparsed.getComment());

            ModifyColumnCommentOp doubledSingleQuote = assertSingleClausePath(
                    "ALTER TABLE t MODIFY COLUMN info.metric COMMENT 'owner''s'",
                    ModifyColumnCommentOp.class, "info.metric");
            Assertions.assertEquals("owner's", doubledSingleQuote.getComment());
            ModifyColumnCommentOp doubledDoubleQuote = assertSingleClausePath(
                    "ALTER TABLE t MODIFY COLUMN info.metric COMMENT \"a\"\"b\"",
                    ModifyColumnCommentOp.class, "info.metric");
            Assertions.assertEquals("a\"b", doubledDoubleQuote.getComment());
        }
    }

    private <T extends AlterTableOp> T assertSingleClausePath(String sql, Class<T> clauseClass,
            String expectedPath) {
        Plan plan = parser.parseSingle(sql);
        Assertions.assertInstanceOf(AlterTableCommand.class, plan);
        List<AlterTableOp> clauses = ((AlterTableCommand) plan).getOps();
        Assertions.assertEquals(1, clauses.size());
        AlterTableOp clause = clauses.get(0);
        Assertions.assertInstanceOf(clauseClass, clause);
        if (clause instanceof AddColumnOp) {
            Assertions.assertEquals(expectedPath, ((AddColumnOp) clause).getColumnPath().getFullPath());
        } else if (clause instanceof ModifyColumnOp) {
            Assertions.assertEquals(expectedPath, ((ModifyColumnOp) clause).getColumnPath().getFullPath());
        } else if (clause instanceof ModifyColumnCommentOp) {
            Assertions.assertEquals(expectedPath, ((ModifyColumnCommentOp) clause).getColumnPath().getFullPath());
        } else if (clause instanceof DropColumnOp) {
            Assertions.assertEquals(expectedPath, ((DropColumnOp) clause).getColumnPath().getFullPath());
        } else if (clause instanceof RenameColumnOp) {
            Assertions.assertEquals(expectedPath, ((RenameColumnOp) clause).getColumnPath().getFullPath());
        }
        return clauseClass.cast(clause);
    }
}
