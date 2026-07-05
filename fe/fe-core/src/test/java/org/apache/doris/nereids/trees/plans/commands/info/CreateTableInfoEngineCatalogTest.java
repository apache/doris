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
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.qe.ConnectContext;

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
import java.util.List;
import java.util.Map;

/**
 * Tests engine-padding / catalog-engine-consistency in {@link CreateTableInfo} for a
 * {@link PluginDrivenExternalCatalog}, the form a {@code max_compute} catalog takes after the
 * SPI cutover (T06b). FIX-DDL-ENGINE (P4-T06d).
 *
 * <p><b>Why these tests matter:</b> {@code paddingEngineName} and {@code checkEngineWithCatalog}
 * key on {@code instanceof MaxComputeExternalCatalog}; after cutover the catalog is a
 * {@code PluginDrivenExternalCatalog} (type {@code "max_compute"}), so a no-ENGINE CREATE TABLE
 * (the most common MC form) threw "Current catalog does not support create table" at analysis
 * time and never reached the working {@code createTable} override. These tests lock in that the
 * engine is padded to {@code maxcompute} (plain CREATE and CTAS), that the catalog-engine
 * consistency check still rejects a wrong explicit ENGINE, and that the non-CREATE-TABLE SPI
 * types (jdbc/es/trino) keep their legacy behavior.</p>
 *
 * <p>Both gate methods re-fetch the catalog <em>by name</em> via
 * {@code Env.getCurrentEnv().getCatalogMgr().getCatalog(ctlName)}, so the test catalog must be
 * registered into a mocked {@link CatalogMgr} — a directly-constructed catalog would be ignored.
 * The gate methods are private, so they are invoked reflectively.</p>
 */
public class CreateTableInfoEngineCatalogTest {

    // Mirror of CreateTableInfo.ENGINE_MAXCOMPUTE (private constant).
    private static final String ENGINE_MAXCOMPUTE = "maxcompute";
    // Iceberg catalog-level format-version property keys (literal values of iceberg SDK
    // CatalogProperties.TABLE_DEFAULT_PREFIX/TABLE_OVERRIDE_PREFIX + TableProperties.FORMAT_VERSION;
    // spelled out to avoid importing org.apache.iceberg into this nereids test package).
    private static final String TABLE_DEFAULT_FORMAT_VERSION = "table-default.format-version";
    private static final String TABLE_OVERRIDE_FORMAT_VERSION = "table-override.format-version";

    private MockedStatic<Env> mockedEnv;
    private CatalogMgr catalogMgr;

    @BeforeEach
    public void setUp() {
        Env mockEnv = Mockito.mock(Env.class);
        catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.when(mockEnv.getCatalogMgr()).thenReturn(catalogMgr);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(mockEnv);
    }

    @AfterEach
    public void tearDown() {
        if (mockedEnv != null) {
            mockedEnv.close();
        }
    }

    /** Registers a PluginDriven catalog of the given connector type under the given name. */
    private void registerPluginCatalog(String ctlName, String type) {
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.doReturn(type).when(catalog).getType();
        Mockito.when(catalogMgr.getCatalog(ctlName)).thenReturn(catalog);
    }

    private static CreateTableInfo newInfo(String ctlName, String engineName) {
        return new CreateTableInfo(false, false, false, ctlName, "db", "tbl",
                new ArrayList<>(), new ArrayList<>(), engineName, null,
                new ArrayList<>(), null, null, null,
                new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
    }

    private static void invokePadding(CreateTableInfo info, String ctlName) throws Throwable {
        Method m = CreateTableInfo.class.getDeclaredMethod("paddingEngineName", String.class, ConnectContext.class);
        m.setAccessible(true);
        try {
            m.invoke(info, ctlName, null);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private static void invokeCheck(CreateTableInfo info) throws Throwable {
        Method m = CreateTableInfo.class.getDeclaredMethod("checkEngineWithCatalog");
        m.setAccessible(true);
        try {
            m.invoke(info);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    @Test
    public void noEnginePaddedToMaxcomputeForPluginDriven() throws Throwable {
        registerPluginCatalog("mc_ctl", "max_compute");
        CreateTableInfo info = newInfo("mc_ctl", null);

        invokePadding(info, "mc_ctl");

        // Why: a no-ENGINE CREATE TABLE under a cutover max_compute catalog must auto-pad the
        // legacy engine name, exactly as legacy MaxComputeExternalCatalog did, instead of throwing
        // "Current catalog does not support create table".
        Assertions.assertEquals(ENGINE_MAXCOMPUTE, info.getEngineName(),
                "no-ENGINE CREATE TABLE on a PluginDriven max_compute catalog must pad engine=maxcompute");
    }

    @Test
    public void ctasNoEnginePaddedToMaxcompute() {
        registerPluginCatalog("mc_ctl", "max_compute");
        CreateTableInfo info = newInfo("mc_ctl", null);

        // CTAS routes through validateCreateTableAsSelect, whose first action is paddingEngineName.
        // The downstream validate(ctx) is heavy and not exercised here; we assert only the padding
        // side effect (set before validate runs). Pre-fix, paddingEngineName throws "does not support
        // create table" before setting engineName, so getEngineName() would not be maxcompute.
        try {
            info.validateCreateTableAsSelect(Lists.newArrayList("mc_ctl"), new ArrayList<>(),
                    Mockito.mock(ConnectContext.class));
        } catch (Exception ignored) {
            // Only the engine-padding side effect is under test here.
        }

        Assertions.assertEquals(ENGINE_MAXCOMPUTE, info.getEngineName(),
                "CTAS into a PluginDriven max_compute catalog must pad engine=maxcompute via "
                        + "validateCreateTableAsSelect");
    }

    @Test
    public void wrongExplicitEngineRejectedForPluginDriven() {
        registerPluginCatalog("mc_ctl", "max_compute");
        CreateTableInfo info = newInfo("mc_ctl", "hive");

        // Why: the catalog-engine consistency check must still reject a mismatched explicit ENGINE
        // under PluginDriven (legacy MaxComputeExternalCatalog rejected ENGINE != maxcompute). This
        // fails with no exception if the checkEngineWithCatalog PluginDriven branch is absent.
        Assertions.assertThrows(AnalysisException.class, () -> invokeCheck(info),
                "explicit ENGINE=hive on a PluginDriven max_compute catalog must be rejected");
    }

    @Test
    public void correctExplicitEnginePassesForPluginDriven() {
        registerPluginCatalog("mc_ctl", "max_compute");
        CreateTableInfo info = newInfo("mc_ctl", ENGINE_MAXCOMPUTE);

        Assertions.assertDoesNotThrow(() -> invokeCheck(info),
                "explicit ENGINE=maxcompute on a PluginDriven max_compute catalog must pass the check");
    }

    @Test
    public void jdbcPluginDrivenStillUnsupported() {
        registerPluginCatalog("jdbc_ctl", "jdbc");

        // paddingEngineName: jdbc (helper returns null) falls through to the existing else-throw,
        // byte-identical to legacy behavior for an SPI type that does not support CREATE TABLE.
        CreateTableInfo padInfo = newInfo("jdbc_ctl", null);
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> invokePadding(padInfo, "jdbc_ctl"),
                "no-ENGINE CREATE TABLE on a jdbc PluginDriven catalog must still be unsupported");
        Assertions.assertTrue(ex.getMessage() != null && ex.getMessage().contains("does not support create table"),
                "jdbc PluginDriven catalog must reuse the existing 'does not support create table' message");

        // checkEngineWithCatalog: jdbc (helper returns null) must NOT throw — legacy lets jdbc/es/trino
        // pass the consistency check unconditionally (they are not in the legacy instanceof chain).
        CreateTableInfo checkInfo = newInfo("jdbc_ctl", "jdbc");
        Assertions.assertDoesNotThrow(() -> invokeCheck(checkInfo),
                "jdbc PluginDriven catalog must pass checkEngineWithCatalog (legacy pass-through parity)");
    }

    // ---------------------------------------------------------------------------------------------
    // ENG-1 / F1: getEffectiveIcebergFormatVersion must consult catalog-level
    // table-default/override.format-version for a flipped (PluginDrivenExternalCatalog) iceberg
    // catalog, so the v3 row-lineage reserved-column check is not silently no-op'd to v2 while the
    // connector creates a v3 table. Mirrors the paddingEngineName plugin-iceberg arm.
    // ---------------------------------------------------------------------------------------------

    /** Registers a PluginDriven catalog of the given connector type, exposing the given catalog properties. */
    private void registerPluginCatalogWithProps(String ctlName, String type, Map<String, String> props) {
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.doReturn(type).when(catalog).getType();
        Mockito.doReturn(props).when(catalog).getProperties();
        Mockito.when(catalogMgr.getCatalog(ctlName)).thenReturn(catalog);
    }

    private static CreateTableInfo newInfoWithColumns(String ctlName, List<ColumnDefinition> columns) {
        return new CreateTableInfo(false, false, false, ctlName, "db", "tbl",
                columns, new ArrayList<>(), null, null,
                new ArrayList<>(), null, null, null,
                new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
    }

    private static int invokeEffectiveVersion(CreateTableInfo info) throws Throwable {
        Method m = CreateTableInfo.class.getDeclaredMethod("getEffectiveIcebergFormatVersion");
        m.setAccessible(true);
        try {
            return (int) m.invoke(info);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private static void invokeValidateRowLineage(CreateTableInfo info) throws Throwable {
        Method m = CreateTableInfo.class.getDeclaredMethod("validateIcebergRowLineageColumns");
        m.setAccessible(true);
        try {
            m.invoke(info);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private static Map<String, String> propMap(String key, String value) {
        Map<String, String> m = new HashMap<>();
        m.put(key, value);
        return m;
    }

    @Test
    public void catalogLevelTableDefaultFormatVersionResolvedForPluginIceberg() throws Throwable {
        registerPluginCatalogWithProps("ice_ctl", "iceberg", propMap(TABLE_DEFAULT_FORMAT_VERSION, "3"));
        CreateTableInfo info = newInfoWithColumns("ice_ctl", new ArrayList<>());

        // Why: a flipped iceberg catalog is a PluginDrivenExternalCatalog, so the legacy
        // instanceof IcebergExternalCatalog gate is false; without the parallel plugin-iceberg arm the
        // catalog-level table-default.format-version=3 is ignored and the version resolves to 2.
        Assertions.assertEquals(3, invokeEffectiveVersion(info),
                "catalog-level table-default.format-version=3 must be honored for a PluginDriven iceberg catalog");
    }

    @Test
    public void catalogLevelTableOverrideFormatVersionResolvedForPluginIceberg() throws Throwable {
        registerPluginCatalogWithProps("ice_ctl", "iceberg", propMap(TABLE_OVERRIDE_FORMAT_VERSION, "3"));
        CreateTableInfo info = newInfoWithColumns("ice_ctl", new ArrayList<>());

        Assertions.assertEquals(3, invokeEffectiveVersion(info),
                "catalog-level table-override.format-version=3 must be honored for a PluginDriven iceberg catalog");
    }

    @Test
    public void catalogLevelV3RejectsReservedRowLineageColumnForPluginIceberg() {
        registerPluginCatalogWithProps("ice_ctl", "iceberg", propMap(TABLE_DEFAULT_FORMAT_VERSION, "3"));
        List<ColumnDefinition> columns =
                Lists.newArrayList(new ColumnDefinition("_row_id", BigIntType.INSTANCE, true));
        CreateTableInfo info = newInfoWithColumns("ice_ctl", columns);

        // End-to-end user-facing behavior: master rejected this at analysis; post-flip it must still
        // reject (else a v3 table with a colliding reserved column is silently created).
        Assertions.assertThrows(AnalysisException.class, () -> invokeValidateRowLineage(info),
                "CREATE TABLE(_row_id) under a PluginDriven iceberg catalog with catalog-level "
                        + "format-version=3 must be rejected");
    }

    @Test
    public void noCatalogLevelFormatVersionResolvesToV2ForPluginIceberg() throws Throwable {
        registerPluginCatalogWithProps("ice_ctl", "iceberg", new HashMap<>());
        List<ColumnDefinition> columns =
                Lists.newArrayList(new ColumnDefinition("_row_id", BigIntType.INSTANCE, true));
        CreateTableInfo info = newInfoWithColumns("ice_ctl", columns);

        // Not over-broadened: with no catalog-level format-version and no table-level one, the version
        // resolves to 2, so _row_id is allowed (v3 check does not fire).
        Assertions.assertEquals(2, invokeEffectiveVersion(info),
                "absent any format-version, a PluginDriven iceberg catalog must resolve to v2");
        Assertions.assertDoesNotThrow(() -> invokeValidateRowLineage(info),
                "_row_id must be allowed when the resolved format-version is 2");
    }

    @Test
    public void catalogLevelFormatVersionIgnoredForNonIcebergPluginCatalog() throws Throwable {
        // A max_compute PluginDriven catalog that happens to carry a table-default.format-version must NOT
        // have it read (the plugin-iceberg arm is gated on pluginCatalogTypeToEngine == iceberg): resolves
        // to 2 via the emptyMap branch.
        registerPluginCatalogWithProps("mc_ctl", "max_compute", propMap(TABLE_DEFAULT_FORMAT_VERSION, "3"));
        CreateTableInfo info = newInfoWithColumns("mc_ctl", new ArrayList<>());

        Assertions.assertEquals(2, invokeEffectiveVersion(info),
                "a non-iceberg PluginDriven catalog must not consult catalog-level format-version");
    }
}
