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

package org.apache.doris.tablefunction;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;

/**
 * Tests for the {@code partitions()} TVF analyze gates added by FIX-PART-GATES for
 * {@link PluginDrivenExternalCatalog} tables (DDL-C1 / CACHE-C1).
 *
 * <p><b>Why:</b> after the MaxCompute SPI cutover the catalog is a PluginDrivenExternalCatalog
 * and its tables are PLUGIN_EXTERNAL_TABLE; the TVF's catalog allow-list and table-type allow-list
 * previously rejected both at analyze time, making the (already-wired) BE handler dead code. These
 * tests drive the private {@code analyze()} to lock that a partitioned PluginDriven table passes
 * both gates, while a non-partitioned one is rejected with the legacy message.</p>
 *
 * <p>The Batch-D red line (the {@code MaxComputeExternalCatalog} branch must remain) is not deleted
 * by this change; the PluginDriven branch is added alongside it.</p>
 */
public class PartitionsTableValuedFunctionPluginDrivenTest {

    @Test
    public void testAnalyzePassesForPartitionedPluginDrivenTable() throws Exception {
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getType()).thenReturn(TableType.PLUGIN_EXTERNAL_TABLE);
        Mockito.when(table.isPartitionedTable()).thenReturn(true);

        // No exception means the PluginDriven catalog passed the catalog allow-list (SEAM 1) and the
        // PLUGIN_EXTERNAL_TABLE passed the REAL table-type allow-list (SEAM 2 -- see invokeAnalyze,
        // which runs the genuine DatabaseIf.getTableOrMetaException membership check).
        invokeAnalyze(table);

        // WHY (non-vacuous, Rule 9): verifying isPartitionedTable() was actually called proves the
        // table was resolved (not null) AND the PluginDriven partition guard (SEAM 3) was reached.
        // If table resolution short-circuited (e.g. PLUGIN_EXTERNAL_TABLE removed from the SEAM-2
        // allow-list -> MetaNotFound) or the SEAM-3 branch were deleted, this verify fails.
        Mockito.verify(table).isPartitionedTable();
    }

    @Test
    public void testAnalyzeThrowsForNonPartitionedPluginDrivenTable() {
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getType()).thenReturn(TableType.PLUGIN_EXTERNAL_TABLE);
        Mockito.when(table.isPartitionedTable()).thenReturn(false);

        // WHY: a PluginDriven table with no partition columns must be rejected with the legacy
        // "not a partitioned table" message (mirroring the MaxCompute guard). A mutation that drops
        // the SEAM 3 guard makes this assertion red.
        InvocationTargetException ex = Assertions.assertThrows(InvocationTargetException.class,
                () -> invokeAnalyze(table));
        Assertions.assertTrue(ex.getCause() instanceof AnalysisException);
        Assertions.assertTrue(ex.getCause().getMessage().contains("is not a partitioned table"),
                "expected 'is not a partitioned table', got: " + ex.getCause().getMessage());
    }

    /**
     * Drives the private {@code analyze("ctl","db","t")} on a ctor-bypassed instance (analyze uses
     * no instance state), with Env/CatalogMgr/AccessManager mocked to resolve a PluginDriven
     * catalog + db.
     *
     * <p>The db mock uses {@code CALLS_REAL_METHODS} so the REAL
     * {@code DatabaseIf.getTableOrMetaException(name, types...)} default-method allow-list runs
     * (SEAM 2): only the single-arg resolver is stubbed to return the table, and {@code
     * table.getType()} decides membership. Thus removing {@code PLUGIN_EXTERNAL_TABLE} from the
     * production allow-list throws MetaNotFound -> AnalysisException and turns the tests red.</p>
     */
    private void invokeAnalyze(PluginDrivenExternalTable table) throws Exception {
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
            Mockito.when(env.getAccessManager()).thenReturn(accessManager);
            Mockito.when(accessManager.checkTblPriv(Mockito.nullable(ConnectContext.class), Mockito.eq("ctl"),
                    Mockito.eq("db"), Mockito.eq("t"), Mockito.any(PrivPredicate.class))).thenReturn(true);

            PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
            Mockito.when(catalogMgr.getCatalog("ctl")).thenReturn(catalog);
            Mockito.when(catalog.isInternalCatalog()).thenReturn(false);

            // CALLS_REAL_METHODS: run the genuine type allow-list (SEAM 2); stub only the single-arg
            // resolver so the real membership check at DatabaseIf.getTableOrMetaException(name,List)
            // executes against table.getType().
            DatabaseIf<?> db = Mockito.mock(DatabaseIf.class, Mockito.CALLS_REAL_METHODS);
            Mockito.doReturn(table).when(db).getTableOrMetaException("t");
            Mockito.doReturn(Optional.of(db)).when(catalog).getDb("db");

            PartitionsTableValuedFunction tvf =
                    Mockito.mock(PartitionsTableValuedFunction.class, Mockito.CALLS_REAL_METHODS);
            Method analyze = PartitionsTableValuedFunction.class
                    .getDeclaredMethod("analyze", String.class, String.class, String.class);
            analyze.setAccessible(true);
            try {
                analyze.invoke(tvf, "ctl", "db", "t");
            } catch (InvocationTargetException e) {
                throw e; // surface the wrapped AnalysisException to the caller
            }
        }
    }
}
