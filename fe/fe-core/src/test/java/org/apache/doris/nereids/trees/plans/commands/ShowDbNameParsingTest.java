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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests that SHOW commands taking `database=multipartIdentifier` resolve `[ctl.]db` the way the
 * siblings (e.g. visitShowFunctions) do: one part is the db, two parts are catalog + db (with the
 * internal-only commands rejecting a non-internal catalog), and more parts are rejected, instead
 * of silently truncating to the first part or concatenating the raw text.
 */
public class ShowDbNameParsingTest {
    private final NereidsParser parser = new NereidsParser();

    @Test
    public void testSinglePartDatabaseName() {
        Assertions.assertEquals("db1",
                ((ShowEncryptKeysCommand) parser.parseSingle("SHOW ENCRYPTKEYS FROM db1")).getDbName());
        Assertions.assertEquals("db1",
                ((ShowSmallFilesCommand) parser.parseSingle("SHOW FILE FROM db1")).getDbName());
        Assertions.assertEquals("db1",
                ((ShowDynamicPartitionCommand) parser.parseSingle("SHOW DYNAMIC PARTITION TABLES FROM db1"))
                        .getDbName());
        Assertions.assertEquals("db1",
                ((ShowDeleteCommand) parser.parseSingle("SHOW DELETE FROM db1")).getDbName());
    }

    @Test
    public void testCatalogDotDatabaseName() {
        // [catalog.]db must resolve to the db, not be truncated to the catalog part.
        Assertions.assertEquals("db1",
                ((ShowEncryptKeysCommand) parser.parseSingle("SHOW ENCRYPTKEYS FROM internal.db1")).getDbName());
        Assertions.assertEquals("db1",
                ((ShowSmallFilesCommand) parser.parseSingle("SHOW FILE FROM internal.db1")).getDbName());
        Assertions.assertEquals("db1",
                ((ShowDynamicPartitionCommand) parser.parseSingle("SHOW DYNAMIC PARTITION TABLES FROM internal.db1"))
                        .getDbName());
        Assertions.assertEquals("db1",
                ((ShowDeleteCommand) parser.parseSingle("SHOW DELETE FROM internal.db1")).getDbName());
        Assertions.assertEquals("db1",
                ((ShowDeleteCommand) parser.parseSingle("SHOW DELETE FROM INTERNAL.db1")).getDbName());
        Assertions.assertEquals("db1",
                ((ShowColumnsCommand) parser.parseSingle("SHOW COLUMNS FROM t1 FROM internal.db1")).getDbName());
    }

    @Test
    public void testNoDatabaseClause() {
        Assertions.assertNull(((ShowDeleteCommand) parser.parseSingle("SHOW DELETE")).getDbName());
        Assertions.assertNull(((ShowEncryptKeysCommand) parser.parseSingle("SHOW ENCRYPTKEYS")).getDbName());
    }

    @Test
    public void testNonInternalCatalogRejected() {
        // These commands resolve only against the internal catalog; a non-internal catalog
        // qualifier must be an explicit error, not a silent wrong-database read.
        String[] sqls = {
            "SHOW ENCRYPTKEYS FROM ext_catalog.db1",
            "SHOW FILE FROM ext_catalog.db1",
            "SHOW DYNAMIC PARTITION TABLES FROM ext_catalog.db1",
            "SHOW DELETE FROM ext_catalog.db1",
        };
        for (String sql : sqls) {
            Exception e = Assertions.assertThrows(Exception.class, () -> parser.parseSingle(sql), sql);
            Assertions.assertTrue(e.getMessage().contains("restricted to the internal catalog"),
                    sql + " -> " + e.getMessage());
        }
    }

    @Test
    public void testTooManyNamePartsRejected() {
        String[] sqls = {
            "SHOW ENCRYPTKEYS FROM a.b.c",
            "SHOW FILE FROM a.b.c",
            "SHOW DYNAMIC PARTITION TABLES FROM a.b.c",
            "SHOW DELETE FROM a.b.c",
            "SHOW COLUMNS FROM t1 FROM a.b.c",
        };
        for (String sql : sqls) {
            Exception e = Assertions.assertThrows(AnalysisException.class, () -> parser.parseSingle(sql), sql);
            Assertions.assertTrue(e.getMessage().contains("should be [ctl.]db"),
                    sql + " -> " + e.getMessage());
        }
    }
}
