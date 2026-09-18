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

import org.apache.doris.analysis.IndexDef;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.AlterTableCommand;
import org.apache.doris.nereids.trees.plans.commands.info.AlterTableOp;
import org.apache.doris.nereids.trees.plans.commands.info.CreateIndexOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropIndexOp;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * Parser coverage for the Lance index DDL grammar: top-level CREATE [OR REPLACE] INDEX
 * ... USING ANN/BTREE/BITMAP and top-level DROP INDEX, plus the guarantees that the
 * ALTER TABLE ADD INDEX grammar rule is unchanged.
 */
public class CreateIndexParserTest extends ParserTestBase {
    private final NereidsParser parser = new NereidsParser();

    private CreateIndexOp parseCreateIndexOp(String sql) {
        Plan plan = parser.parseSingle(sql);
        AlterTableCommand command = Assertions.assertInstanceOf(AlterTableCommand.class, plan);
        Assertions.assertEquals(1, command.getNereidsOps().size());
        AlterTableOp op = command.getNereidsOps().get(0);
        return Assertions.assertInstanceOf(CreateIndexOp.class, op);
    }

    private DropIndexOp parseDropIndexOp(String sql) {
        Plan plan = parser.parseSingle(sql);
        AlterTableCommand command = Assertions.assertInstanceOf(AlterTableCommand.class, plan);
        Assertions.assertEquals(1, command.getNereidsOps().size());
        AlterTableOp op = command.getNereidsOps().get(0);
        return Assertions.assertInstanceOf(DropIndexOp.class, op);
    }

    @Test
    public void testCreateAnnIndexParses() {
        CreateIndexOp op = parseCreateIndexOp(
                "CREATE INDEX idx ON ctl.db.tbl (v) USING ANN "
                        + "PROPERTIES(\"index_type\"=\"IVF_PQ\", \"metric\"=\"l2\", "
                        + "\"num_partitions\"=\"256\", \"num_sub_vectors\"=\"16\") COMMENT 'ann index'");
        Assertions.assertFalse(op.isAlter());
        IndexDefinition def = op.getIndexDef();
        Assertions.assertEquals("idx", def.getIndexName());
        Assertions.assertEquals(IndexDef.IndexType.ANN, def.getIndexType());
        Assertions.assertNull(def.getLanceIndexType());
        Assertions.assertFalse(def.isOrReplace());
        Assertions.assertEquals(Collections.singletonList("v"), def.getCols());
        Map<String, String> properties = def.getProperties();
        Assertions.assertEquals(4, properties.size());
        Assertions.assertEquals("IVF_PQ", properties.get("index_type"));
        Assertions.assertEquals("l2", properties.get("metric"));
        Assertions.assertEquals("256", properties.get("num_partitions"));
        Assertions.assertEquals("16", properties.get("num_sub_vectors"));
    }

    @Test
    public void testCreateBtreeIndexParses() {
        CreateIndexOp op = parseCreateIndexOp("CREATE INDEX idx ON db.tbl (c) USING BTREE");
        IndexDefinition def = op.getIndexDef();
        Assertions.assertNull(def.getIndexType());
        Assertions.assertEquals("BTREE", def.getLanceIndexType());
        Assertions.assertFalse(def.isOrReplace());
        Assertions.assertEquals(Collections.singletonList("c"), def.getCols());
        Assertions.assertTrue(def.getProperties().isEmpty());
    }

    @Test
    public void testCreateBitmapIndexParses() {
        CreateIndexOp op = parseCreateIndexOp("CREATE INDEX idx ON db.tbl (c) USING BITMAP");
        IndexDefinition def = op.getIndexDef();
        Assertions.assertNull(def.getIndexType());
        Assertions.assertEquals("BITMAP", def.getLanceIndexType());
        Assertions.assertFalse(def.isOrReplace());
    }

    @Test
    public void testLowercaseUsingClausesParse() {
        // The lexer stream is case-insensitive; the new tokens must behave like the old ones.
        CreateIndexOp btreeOp = parseCreateIndexOp("create index idx on db.tbl (c) using btree");
        Assertions.assertEquals("BTREE", btreeOp.getIndexDef().getLanceIndexType());
        CreateIndexOp bitmapOp = parseCreateIndexOp("create index idx on db.tbl (c) using bitmap");
        Assertions.assertEquals("BITMAP", bitmapOp.getIndexDef().getLanceIndexType());
    }

    @Test
    public void testCreateOrReplaceIndexParses() {
        CreateIndexOp op = parseCreateIndexOp("CREATE OR REPLACE INDEX idx ON db.tbl (c) USING BTREE");
        IndexDefinition def = op.getIndexDef();
        Assertions.assertTrue(def.isOrReplace());
        Assertions.assertEquals("BTREE", def.getLanceIndexType());
        Assertions.assertEquals(
                "CREATE OR REPLACE INDEX idx ON `db`.`tbl` (`c`) USING BTREE COMMENT ''", op.toSql());
    }

    @Test
    public void testCreateIndexIfNotExistsParses() {
        CreateIndexOp op = parseCreateIndexOp(
                "CREATE INDEX IF NOT EXISTS idx ON db.tbl (v) USING ANN "
                        + "PROPERTIES(\"index_type\"=\"IVF_PQ\", \"num_partitions\"=\"256\", "
                        + "\"num_sub_vectors\"=\"16\")");
        Assertions.assertFalse(op.getIndexDef().isOrReplace());
        Assertions.assertEquals(IndexDef.IndexType.ANN, op.getIndexDef().getIndexType());
    }

    @Test
    public void testOrReplaceAndIfNotExistsAreExclusive() {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> parser.parseSingle("CREATE OR REPLACE INDEX IF NOT EXISTS idx ON db.tbl (c) USING BTREE"));
        Assertions.assertEquals("[OR REPLACE] and [IF NOT EXISTS] cannot used at the same time",
                exception.getMessage());
    }

    @Test
    public void testInternalUsingClausesStillParse() {
        // Previously-parseable internal index SQL must keep working unchanged.
        CreateIndexOp noUsing = parseCreateIndexOp("CREATE INDEX idx ON db.tbl (c)");
        Assertions.assertEquals(IndexDef.IndexType.INVERTED, noUsing.getIndexDef().getIndexType());
        Assertions.assertNull(noUsing.getIndexDef().getLanceIndexType());

        CreateIndexOp inverted = parseCreateIndexOp("CREATE INDEX idx ON db.tbl (c) USING INVERTED");
        Assertions.assertEquals(IndexDef.IndexType.INVERTED, inverted.getIndexDef().getIndexType());

        CreateIndexOp ngram = parseCreateIndexOp(
                "CREATE INDEX idx ON db.tbl (c) USING NGRAM_BF PROPERTIES(\"gram_size\"=\"3\", \"bf_size\"=\"10000\")");
        Assertions.assertEquals(IndexDef.IndexType.NGRAM_BF, ngram.getIndexDef().getIndexType());

        CreateIndexOp multiColumn = parseCreateIndexOp("CREATE INDEX idx ON db.tbl (c1, c2) USING INVERTED");
        Assertions.assertEquals(Arrays.asList("c1", "c2"), multiColumn.getIndexDef().getCols());
    }

    @Test
    public void testAlterTableAddIndexGrammarUnchanged() {
        // The indexDef rule was deliberately not extended: Lance-only index types stay
        // parse errors in ALTER TABLE ADD INDEX.
        Plan plan = parser.parseSingle("ALTER TABLE db.tbl ADD INDEX idx (c) USING INVERTED");
        AlterTableCommand command = Assertions.assertInstanceOf(AlterTableCommand.class, plan);
        CreateIndexOp op = Assertions.assertInstanceOf(CreateIndexOp.class, command.getNereidsOps().get(0));
        Assertions.assertTrue(op.isAlter());

        Plan annPlan = parser.parseSingle("ALTER TABLE db.tbl ADD INDEX idx (v) USING ANN");
        AlterTableOp annOp = ((AlterTableCommand) annPlan).getNereidsOps().get(0);
        Assertions.assertTrue(((CreateIndexOp) annOp).isAlter());

        Assertions.assertThrows(ParseException.class,
                () -> parser.parseSingle("ALTER TABLE db.tbl ADD INDEX idx (c) USING BTREE"));
        Assertions.assertThrows(ParseException.class,
                () -> parser.parseSingle("ALTER TABLE db.tbl ADD INDEX idx (c) USING BITMAP"));
    }

    @Test
    public void testDropIndexParses() {
        DropIndexOp op = parseDropIndexOp("DROP INDEX idx ON db.tbl");
        Assertions.assertEquals("idx", op.getIndexName());
        Assertions.assertFalse(op.isAlter());
        Assertions.assertFalse(op.isSetIfExists());

        DropIndexOp ifExists = parseDropIndexOp("DROP INDEX IF EXISTS idx ON ctl.db.tbl");
        Assertions.assertTrue(ifExists.isSetIfExists());
        Assertions.assertFalse(ifExists.isAlter());
    }
}
