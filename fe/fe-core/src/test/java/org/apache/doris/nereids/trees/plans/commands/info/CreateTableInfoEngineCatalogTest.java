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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.types.VarBinaryType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Tests how {@link CreateTableInfo} settles the {@code ENGINE=} clause now that the engine holds no table of
 * which engine name belongs to which data source.
 *
 * <p><b>What changed and why these tests matter.</b> Analysis used to run four engine-name gates: it padded a
 * missing {@code ENGINE=} from a hardcoded catalog-type switch, checked the name against a nine-name
 * whitelist, checked it against the same switch again, and gated {@code PARTITION BY} / {@code DISTRIBUTED BY}
 * on per-engine allow-lists. All four are gone. What remains is one question asked of the resolved target
 * catalog ({@link CatalogIf#validateCreateTableEngine}) and one boolean derived from it — is the target the
 * internal catalog. A missing {@code ENGINE=} is padded only for the internal catalog, which still dispatches
 * on it; for every other target the statement now carries no engine name at all, because nothing past analysis
 * reads one.</p>
 *
 * <p>The gate re-fetches the catalog <em>by name</em> through
 * {@code Env.getCurrentEnv().getCatalogMgr().getCatalog(ctlName)}, so a test catalog must be registered into a
 * mocked {@link CatalogMgr} — a directly-constructed one would be ignored. The gate is private, so it is
 * invoked reflectively.</p>
 */
public class CreateTableInfoEngineCatalogTest {

    // Mirror of CreateTableInfo.ENGINE_OLAP, the one name the engine still resolves for itself.
    private static final String ENGINE_OLAP = "olap";

    private MockedStatic<Env> mockedEnv;
    private CatalogMgr catalogMgr;

    @BeforeEach
    public void setUp() {
        Env mockEnv = Mockito.mock(Env.class);
        catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.when(mockEnv.getCatalogMgr()).thenReturn(catalogMgr);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.when(accessManager.checkTblPriv(Mockito.nullable(ConnectContext.class),
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any()))
                .thenReturn(true);
        Mockito.when(mockEnv.getAccessManager()).thenReturn(accessManager);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(mockEnv);
    }

    @AfterEach
    public void tearDown() {
        if (mockedEnv != null) {
            mockedEnv.close();
        }
    }

    /**
     * Registers an external (plugin-driven) catalog that accepts exactly {@code acceptedEngine}, standing in
     * for whatever its connector's provider declares.
     */
    private PluginDrivenExternalCatalog registerExternalCatalog(String ctlName, String acceptedEngine) {
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.doReturn(ctlName).when(catalog).getName();
        Mockito.doReturn(false).when(catalog).isInternalCatalog();
        try {
            Mockito.doAnswer(invocation -> {
                String written = invocation.getArgument(0);
                if (!written.equals(acceptedEngine)) {
                    throw new org.apache.doris.common.AnalysisException(
                            CatalogIf.engineMismatchError(written, ctlName));
                }
                return null;
            }).when(catalog).validateCreateTableEngine(Mockito.anyString());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        Mockito.when(catalogMgr.getCatalog(ctlName)).thenReturn(catalog);
        return catalog;
    }

    private void registerInternalCatalog(String ctlName) {
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn(ctlName).when(catalog).getName();
        Mockito.when(catalogMgr.getCatalog(ctlName)).thenReturn(catalog);
    }

    private static CreateTableInfo newInfo(String ctlName, String engineName) {
        return newInfo(ctlName, engineName, false, false);
    }

    private static CreateTableInfo newInfo(String ctlName, String engineName, boolean isExternal, boolean isTemp) {
        return new CreateTableInfo(false, isExternal, isTemp, ctlName, "db", "tbl",
                new ArrayList<>(), new ArrayList<>(), engineName, null,
                new ArrayList<>(), null, null, null,
                new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
    }

    private static void resolve(CreateTableInfo info) throws Throwable {
        Method m = CreateTableInfo.class.getDeclaredMethod("resolveTargetCatalog");
        m.setAccessible(true);
        try {
            m.invoke(info);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    @Test
    public void noEngineOnExternalCatalogLeavesNoEngineName() throws Throwable {
        registerExternalCatalog("ice_ctl", "iceberg");
        CreateTableInfo info = newInfo("ice_ctl", null);

        resolve(info);

        // The engine used to invent a name here from a catalog-type switch. Nothing past analysis reads one
        // for an external target -- the connector request carries columns, partitioning, bucketing and
        // properties -- so inventing one only created a table fe-core had to keep in sync with the connectors.
        Assertions.assertNull(info.getEngineName(),
                "a no-ENGINE CREATE TABLE on an external catalog must not have an engine name invented for it");
    }

    @Test
    public void externalCatalogAllowsVarbinaryColumns() {
        registerExternalCatalog("paimon_ctl", "paimon");
        CreateTableInfo info = new CreateTableInfo(false, false, false,
                "paimon_ctl", "db", "tbl",
                Lists.newArrayList(new ColumnDefinition("payload", VarBinaryType.INSTANCE, true)),
                new ArrayList<>(), "paimon", null,
                new ArrayList<>(), null, PartitionTableInfo.EMPTY, null,
                new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());

        ConnectContext previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setStatementContext(new StatementContext(context, new OriginStatement("", 0)));
        context.setThreadLocalInfo();
        try {
            Assertions.assertDoesNotThrow(() -> info.validate(context),
                    "VARBINARY is unsupported only by internal Doris tables; external catalogs own their types");
        } finally {
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void noEngineOnTheInternalCatalogStillPadsOlap() throws Throwable {
        registerInternalCatalog("internal");
        CreateTableInfo info = newInfo("internal", null);

        resolve(info);

        // The internal catalog is the one target that still consumes the name: InternalCatalog.createTable
        // dispatches on it.
        Assertions.assertEquals(ENGINE_OLAP, info.getEngineName(),
                "a no-ENGINE CREATE TABLE on the internal catalog must still be padded to olap");
    }

    @Test
    public void explicitEngineIsJudgedByTheTargetCatalogAndItsWordingReachesTheUser() {
        registerExternalCatalog("ice_ctl", "iceberg");
        CreateTableInfo info = newInfo("ice_ctl", "jdbc");

        AnalysisException ex = Assertions.assertThrows(AnalysisException.class, () -> resolve(info),
                "an engine name the target catalog does not answer to must be rejected during analysis");
        // The catalog owns the wording; analysis only adapts the exception type. If it wrapped or reworded,
        // every catalog would need fe-core's permission to phrase its own rejection.
        Assertions.assertEquals(CatalogIf.engineMismatchError("jdbc", "ice_ctl"), ex.getMessage(),
                "the target catalog's wording must reach the user verbatim");
    }

    @Test
    public void acceptedExplicitEngineIsKept() throws Throwable {
        registerExternalCatalog("ice_ctl", "iceberg");
        CreateTableInfo info = newInfo("ice_ctl", "iceberg");

        resolve(info);

        Assertions.assertEquals("iceberg", info.getEngineName(),
                "an engine name the catalog accepts must survive analysis unchanged");
    }

    @Test
    public void theInternalCatalogRejectsAnExternalEngineName() {
        registerInternalCatalog("internal");
        CreateTableInfo info = newInfo("internal", "hive");

        // This used to survive the whole of analysis and fail only at execution, because the nine-name
        // whitelist accepted hive regardless of where the statement was aimed.
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class, () -> resolve(info));
        Assertions.assertEquals(CatalogIf.engineMismatchError("hive", "internal"), ex.getMessage());
    }

    @Test
    public void theInternalCatalogKeepsAnsweringForItsOwnRetiredEngines() {
        registerInternalCatalog("internal");
        for (String retired : new String[] {"odbc", "mysql", "broker"}) {
            CreateTableInfo info = newInfo("internal", retired);
            AnalysisException ex = Assertions.assertThrows(AnalysisException.class, () -> resolve(info),
                    retired + " is retired and must still be rejected");
            // Those three were the internal catalog's own table types, so it still owes the user the specific
            // "use X instead" message rather than the generic mismatch.
            Assertions.assertTrue(ex.getMessage().contains("no longer supported"),
                    "a retired internal engine must keep its own message, got: " + ex.getMessage());
        }
    }

    @Test
    public void ctasResolvesTheTargetCatalogTheSameWay() {
        registerInternalCatalog("internal");
        CreateTableInfo info = newInfo("internal", null);

        // CTAS has its own prologue but must settle the engine through the same path; the heavy validate(ctx)
        // that follows is not exercised here.
        try {
            info.validateCreateTableAsSelect(Lists.newArrayList("internal"), new ArrayList<>(),
                    Mockito.mock(ConnectContext.class));
        } catch (Exception ignored) {
            // Only the engine-resolution side effect is under test here.
        }

        Assertions.assertEquals(ENGINE_OLAP, info.getEngineName(),
                "CTAS into the internal catalog must resolve the engine the same way a plain CREATE does");
    }

    @Test
    public void theExternalKeywordIsRejectedAgainstTheInternalCatalog() {
        registerInternalCatalog("internal");
        CreateTableInfo info = newInfo("internal", null, true, false);

        // EXTERNAL used to be forced on by the engine-name whitelist and rejected only in the olap arm. It is
        // now derived from the target, so the contradiction has to be caught explicitly or it would be
        // silently overwritten -- and EXTERNAL is not cosmetic: it relaxes partition validation.
        Assertions.assertThrows(AnalysisException.class, () -> resolve(info),
                "CREATE EXTERNAL TABLE aimed at the internal catalog must still be rejected");
    }

    @Test
    public void externalTargetMakesTheStatementExternal() throws Throwable {
        registerExternalCatalog("ice_ctl", "iceberg");
        CreateTableInfo info = newInfo("ice_ctl", null);

        resolve(info);

        // isExternal reaches PartitionTableInfo.convertToPartitionDesc, where it turns on auto-partitioning.
        // Deriving it from the target rather than from the engine name is what keeps transform partitioning
        // working for a statement that never wrote ENGINE=.
        Assertions.assertTrue(info.isExternal(),
                "a statement aimed at an external catalog must be external even without the EXTERNAL keyword");
    }

    @Test
    public void temporaryTableIsRejectedOnAnExternalCatalog() {
        registerExternalCatalog("ice_ctl", "iceberg");
        CreateTableInfo info = newInfo("ice_ctl", null, false, true);

        Assertions.assertThrows(AnalysisException.class, () -> resolve(info),
                "temporary tables exist only in the internal catalog");
    }

    @Test
    public void unknownCatalogIsReportedBeforeAnythingElse() {
        Mockito.when(catalogMgr.getCatalog("nope")).thenReturn(null);
        CreateTableInfo info = newInfo("nope", "iceberg");

        AnalysisException ex = Assertions.assertThrows(AnalysisException.class, () -> resolve(info));
        Assertions.assertTrue(ex.getMessage().contains("Unknown catalog"),
                "an unknown catalog must be named as such, not answered with an engine complaint");
    }
}
