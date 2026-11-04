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

package org.apache.doris.nereids.stats;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Unit test of the table name a scan token records. Joining the qualifiers and the name with dots makes
 * two different tables serialize identically (the table `a.b` of the database `db` against the table `b`
 * of the database `db.a`), and a resolver which takes the last part as the table name then judges the
 * sibling of the recorded table.
 */
public class HboScanDescriptorNameTest {

    private static TableIf tableIn(String catalogName, String dbName, String tableName) {
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(catalog.getName()).thenReturn(catalogName);
        DatabaseIf<?> database = Mockito.mock(DatabaseIf.class);
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(database.getFullName()).thenReturn(dbName);
        TableIf table = Mockito.mock(TableIf.class);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(table.getName()).thenReturn(tableName);
        return table;
    }

    @Test
    public void testADotInsideATableNameIsEscaped() {
        Assertions.assertEquals("internal.db.a%2Eb",
                HboScanDescriptor.qualifiedNameOf(tableIn("internal", "db", "a.b")));
        // the dotted database and the dotted table name no longer serialize identically
        Assertions.assertEquals("internal.db.a.b",
                HboScanDescriptor.qualifiedNameOf(tableIn("internal", "db.a", "b")));
        Assertions.assertNotEquals(HboScanDescriptor.qualifiedNameOf(tableIn("internal", "db", "a.b")),
                HboScanDescriptor.qualifiedNameOf(tableIn("internal", "db.a", "b")));
        // an ordinary table keeps the name it had, so no key of one moves
        Assertions.assertEquals("internal.db.t", HboScanDescriptor.qualifiedNameOf(tableIn("internal", "db", "t")));
    }

    @Test
    public void testTheNameIsReadBack() {
        Assertions.assertEquals("a.b", HboScanDescriptor.unescapeNamePart("a%2Eb"));
        Assertions.assertEquals("t", HboScanDescriptor.unescapeNamePart("t"));
        // a name which contains the escape character itself survives the round trip
        Assertions.assertEquals("a%2Eb",
                HboScanDescriptor.unescapeNamePart(HboScanDescriptor.escapeNamePart("a%2Eb")));
        Assertions.assertEquals("a.b.c", HboScanDescriptor.unescapeNamePart(
                HboScanDescriptor.escapeNamePart("a.b.c")));
    }
}
