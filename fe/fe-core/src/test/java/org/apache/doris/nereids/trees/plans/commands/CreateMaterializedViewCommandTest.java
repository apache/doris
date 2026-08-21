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

import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.exceptions.AnalysisException;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CreateMaterializedViewCommand}, focusing on the
 * catalog and database consistency validation added for DORIS-19133.
 */
public class CreateMaterializedViewCommandTest {

    private CreateMaterializedViewCommand newCmd(String db, String tbl) {
        TableNameInfo name = new TableNameInfo(db, tbl);
        return new CreateMaterializedViewCommand(name, null, ImmutableMap.of());
    }

    // ---------------------------------------------------------------------------
    // checkCatalogConsistency tests (DORIS-19133)
    // ---------------------------------------------------------------------------

    @Test
    public void testInternalCatalogDoesNotThrow() {
        CreateMaterializedViewCommand cmd = newCmd("db1", "mv1");
        Assertions.assertDoesNotThrow(
                () -> cmd.checkCatalogConsistency(InternalCatalog.INTERNAL_CATALOG_NAME));
    }

    @Test
    public void testExternalCatalogThrows() {
        CreateMaterializedViewCommand cmd = newCmd("db1", "mv1");
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> cmd.checkCatalogConsistency("hive_catalog"));
        Assertions.assertTrue(ex.getMessage().contains("internal catalog"),
                "Exception message should mention the internal catalog requirement");
    }

    @Test
    public void testNullCatalogDoesNotThrow() {
        CreateMaterializedViewCommand cmd = newCmd("db1", "mv1");
        Assertions.assertDoesNotThrow(() -> cmd.checkCatalogConsistency(null));
    }

    @Test
    public void testEmptyCatalogDoesNotThrow() {
        CreateMaterializedViewCommand cmd = newCmd("db1", "mv1");
        Assertions.assertDoesNotThrow(() -> cmd.checkCatalogConsistency(""));
    }

    // ---------------------------------------------------------------------------
    // checkDatabaseConsistency tests (DORIS-19133)
    // ---------------------------------------------------------------------------

    @Test
    public void testSameDatabaseDoesNotThrow() {
        CreateMaterializedViewCommand cmd = newCmd("db1", "mv1");
        Assertions.assertDoesNotThrow(() -> cmd.checkDatabaseConsistency("db1", "db1"));
    }

    @Test
    public void testDifferentDatabaseThrows() {
        CreateMaterializedViewCommand cmd = newCmd("db1", "mv1");
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> cmd.checkDatabaseConsistency("db1", "db2"));
        Assertions.assertTrue(ex.getMessage().contains("must be the same as"),
                "Exception message should describe the mismatch");
    }

    @Test
    public void testNullMvDbDoesNotThrow() {
        CreateMaterializedViewCommand cmd = newCmd("db1", "mv1");
        Assertions.assertDoesNotThrow(() -> cmd.checkDatabaseConsistency(null, "db2"));
    }

    @Test
    public void testEmptyMvDbDoesNotThrow() {
        CreateMaterializedViewCommand cmd = newCmd("db1", "mv1");
        Assertions.assertDoesNotThrow(() -> cmd.checkDatabaseConsistency("", "db2"));
    }
}
