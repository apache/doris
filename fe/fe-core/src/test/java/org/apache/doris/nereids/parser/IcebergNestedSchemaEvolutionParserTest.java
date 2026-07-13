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

import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.AlterTableCommand;
import org.apache.doris.nereids.trees.plans.commands.info.AddColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.AlterTableOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyColumnCommentOp;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyColumnOp;
import org.apache.doris.nereids.trees.plans.commands.info.RenameColumnOp;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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

    private <T extends AlterTableOp> void assertSingleClausePath(String sql, Class<T> clauseClass,
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
    }
}
