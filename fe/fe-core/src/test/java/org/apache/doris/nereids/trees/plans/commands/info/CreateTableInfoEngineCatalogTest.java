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
}
