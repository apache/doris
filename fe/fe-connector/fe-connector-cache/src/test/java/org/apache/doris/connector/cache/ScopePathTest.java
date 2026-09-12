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

package org.apache.doris.connector.cache;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ScopePathTest {
    @Test
    public void containsExactlyMatchesHierarchyPrefixes() {
        ScopePath catalog = ScopePath.catalog();
        ScopePath database = ScopePath.database("db");
        ScopePath table = ScopePath.table("db", "tbl");
        ScopePath partition = ScopePath.partition("db", "tbl", "p=1");

        Assertions.assertTrue(catalog.contains(catalog));
        Assertions.assertTrue(catalog.contains(database));
        Assertions.assertTrue(catalog.contains(table));
        Assertions.assertTrue(catalog.contains(partition));
        Assertions.assertTrue(database.contains(database));
        Assertions.assertTrue(database.contains(table));
        Assertions.assertTrue(database.contains(partition));
        Assertions.assertTrue(table.contains(table));
        Assertions.assertTrue(table.contains(partition));
        Assertions.assertTrue(partition.contains(partition));

        Assertions.assertFalse(database.contains(catalog));
        Assertions.assertFalse(table.contains(database));
        Assertions.assertFalse(partition.contains(table));
        Assertions.assertFalse(database.contains(ScopePath.table("other", "tbl")));
        Assertions.assertFalse(table.contains(ScopePath.partition("db", "other", "p=1")));
        Assertions.assertFalse(partition.contains(ScopePath.partition("db", "tbl", "p=2")));
    }

    @Test
    public void valueSemanticsIncludeEveryScopeComponent() {
        Assertions.assertEquals(ScopePath.catalog(), ScopePath.catalog());
        Assertions.assertEquals(ScopePath.database("db"), ScopePath.database("db"));
        Assertions.assertEquals(ScopePath.table("db", "tbl"), ScopePath.table("db", "tbl"));
        Assertions.assertEquals(
                ScopePath.partition("db", "tbl", "p=1"),
                ScopePath.partition("db", "tbl", "p=1"));
        Assertions.assertEquals(
                ScopePath.partition("db", "tbl", "p=1").hashCode(),
                ScopePath.partition("db", "tbl", "p=1").hashCode());
        Assertions.assertNotEquals(ScopePath.database("db"), ScopePath.database("other"));
        Assertions.assertNotEquals(ScopePath.table("db", "tbl"), ScopePath.table("db", "other"));
        Assertions.assertNotEquals(
                ScopePath.partition("db", "tbl", "p=1"),
                ScopePath.partition("db", "tbl", "p=2"));
    }

    @Test
    public void rejectsIncompletePaths() {
        Assertions.assertThrows(NullPointerException.class, () -> ScopePath.database(null));
        Assertions.assertThrows(NullPointerException.class, () -> ScopePath.table(null, "tbl"));
        Assertions.assertThrows(NullPointerException.class, () -> ScopePath.table("db", null));
        Assertions.assertThrows(
                NullPointerException.class, () -> ScopePath.partition("db", "tbl", null));
        Assertions.assertThrows(NullPointerException.class, () -> ScopePath.catalog().contains(null));
    }
}
