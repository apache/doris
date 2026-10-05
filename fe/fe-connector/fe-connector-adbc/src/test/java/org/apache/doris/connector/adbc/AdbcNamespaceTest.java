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

package org.apache.doris.connector.adbc;

import org.apache.doris.connector.spi.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The three-level to two-level projection. Each case below is a real source shape: PostgreSQL populates
 * both levels, MySQL only the schema, SQLite only the catalog.
 */
class AdbcNamespaceTest {

    @Test
    void sourceWithOnlyASchemaLevelUsesTheSchema() {
        Assertions.assertEquals("testdb", new AdbcNamespace("", "testdb").dorisDatabaseName());
        Assertions.assertEquals("testdb", new AdbcNamespace(null, "testdb").dorisDatabaseName());
    }

    @Test
    void sourceWithOnlyACatalogLevelUsesTheCatalog() {
        Assertions.assertEquals("main", new AdbcNamespace("main", "").dorisDatabaseName());
        Assertions.assertEquals("main", new AdbcNamespace("main", null).dorisDatabaseName());
    }

    @Test
    void whenBothLevelsExistTheSchemaWins() {
        // uri pins the remote catalog, so it is identical for every namespace in this Doris catalog:
        // including it in the name would add nothing and cost a delimiter that cannot be parsed back.
        Assertions.assertEquals("public", new AdbcNamespace("mydb", "public").dorisDatabaseName());
    }

    @Test
    void nullAndEmptyAreTheSameAbsentLevel() {
        // Drivers disagree: SQLite reports the missing schema level as "", others as null. If the two were
        // treated differently, the same source would map to a different database name depending on which
        // driver build served it.
        Assertions.assertEquals(new AdbcNamespace("main", null), new AdbcNamespace("main", ""));
        Assertions.assertEquals(new AdbcNamespace("main", null).hashCode(),
                new AdbcNamespace("main", "").hashCode());
    }

    @Test
    void namespaceWithNeitherLevelFailsLoud() {
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> new AdbcNamespace("", "").dorisDatabaseName());
        Assertions.assertTrue(e.getMessage().contains("Doris database"), e.getMessage());
    }

    @Test
    void theRemotePartsSurviveACatalogNameContainingADot() {
        // The display name is never parsed back, which is what lets a remote catalog contain a dot. If any
        // code path re-derived the remote parts from the Doris name, "MY.DB"/"PUBLIC" would split wrong and
        // the pushed-down SQL would address a table that does not exist.
        AdbcNamespace namespace = new AdbcNamespace("MY.DB", "PUBLIC");

        Assertions.assertEquals("PUBLIC", namespace.dorisDatabaseName());
        Assertions.assertEquals("MY.DB", namespace.getRemoteCatalog());
        Assertions.assertEquals("PUBLIC", namespace.getRemoteDbSchema());
    }
}
