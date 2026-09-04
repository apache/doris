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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class AccessControllerManagerTest {

    private boolean originalSkipCatalogPrivCheck;

    @Before
    public void setUp() {
        originalSkipCatalogPrivCheck = Config.skip_catalog_priv_check;
    }

    @After
    public void tearDown() {
        Config.skip_catalog_priv_check = originalSkipCatalogPrivCheck;
    }

    @Test
    public void testCheckCtlPrivSkipCatalogPrivCheckWithCustomAccessControllerForSelect() {
        CatalogAccessController defaultAccessController = Mockito.mock(CatalogAccessController.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            AccessControllerManager accessControllerManager = createAccessControllerManager(defaultAccessController);
            UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");
            Config.skip_catalog_priv_check = true;

            Mockito.when(defaultAccessController.checkGlobalPriv(Mockito.any(), Mockito.any())).thenReturn(false);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(catalogMgr.getCatalog("custom_catalog")).thenReturn(catalog);
            Mockito.when(catalog.isInternalCatalog()).thenReturn(false);
            Mockito.when(catalog.getProperties()).thenReturn(
                    ImmutableMap.of(CatalogMgr.ACCESS_CONTROLLER_CLASS_PROP, "mock.access.controller"));

            Assert.assertTrue(accessControllerManager.checkCtlPriv(
                    userIdentity, "custom_catalog", PrivPredicate.SELECT));
        }
    }

    @Test
    public void testCheckCtlPrivSkipCatalogPrivCheckWithCustomAccessControllerForShow() {
        CatalogAccessController defaultAccessController = Mockito.mock(CatalogAccessController.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            AccessControllerManager accessControllerManager = createAccessControllerManager(defaultAccessController);
            UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");
            Config.skip_catalog_priv_check = true;

            Mockito.when(defaultAccessController.checkGlobalPriv(Mockito.any(), Mockito.any())).thenReturn(false);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(catalogMgr.getCatalog("custom_catalog")).thenReturn(catalog);
            Mockito.when(catalog.isInternalCatalog()).thenReturn(false);
            Mockito.when(catalog.getProperties()).thenReturn(
                    ImmutableMap.of(CatalogMgr.ACCESS_CONTROLLER_CLASS_PROP, "mock.access.controller"));

            Assert.assertTrue(accessControllerManager.checkCtlPriv(
                    userIdentity, "custom_catalog", PrivPredicate.SHOW));
        }
    }

    @Test
    public void testCheckCtlPrivSkipCatalogPrivCheckWithoutCustomAccessController() {
        CatalogAccessController defaultAccessController = Mockito.mock(CatalogAccessController.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            AccessControllerManager accessControllerManager = createAccessControllerManager(defaultAccessController);
            UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");
            Config.skip_catalog_priv_check = true;

            Mockito.when(defaultAccessController.checkGlobalPriv(Mockito.any(), Mockito.any())).thenReturn(false);
            Mockito.when(defaultAccessController.checkCtlPriv(
                    Mockito.anyBoolean(), Mockito.any(), Mockito.anyString(), Mockito.any())).thenReturn(false);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(catalogMgr.getCatalog("custom_catalog")).thenReturn(catalog);
            Mockito.when(catalog.isInternalCatalog()).thenReturn(false);
            Mockito.when(catalog.getProperties()).thenReturn(ImmutableMap.of("type", "test"));

            Assert.assertFalse(accessControllerManager.checkCtlPriv(
                    userIdentity, "custom_catalog", PrivPredicate.SELECT));
        }
    }

    @Test
    public void testCheckCtlPrivCreateMustCheckDefaultAccessController() {
        CatalogAccessController defaultAccessController = Mockito.mock(CatalogAccessController.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            AccessControllerManager accessControllerManager = createAccessControllerManager(defaultAccessController);
            UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");
            Config.skip_catalog_priv_check = true;

            Mockito.when(defaultAccessController.checkGlobalPriv(Mockito.any(), Mockito.any())).thenReturn(false);
            Mockito.when(defaultAccessController.checkCtlPriv(
                    Mockito.anyBoolean(), Mockito.any(), Mockito.anyString(), Mockito.any())).thenReturn(true);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(catalogMgr.getCatalog("not_exist_catalog")).thenReturn(null);

            Assert.assertTrue(accessControllerManager.checkCtlPriv(
                    userIdentity, "not_exist_catalog", PrivPredicate.CREATE));
        }
    }

    @Test
    public void testCheckCtlPrivLoadMustCheckDefaultAccessController() {
        CatalogAccessController defaultAccessController = Mockito.mock(CatalogAccessController.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            AccessControllerManager accessControllerManager = createAccessControllerManager(defaultAccessController);
            UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");
            Config.skip_catalog_priv_check = true;

            Mockito.when(defaultAccessController.checkGlobalPriv(Mockito.any(), Mockito.any())).thenReturn(false);
            Mockito.when(defaultAccessController.checkCtlPriv(
                    Mockito.anyBoolean(), Mockito.any(), Mockito.anyString(), Mockito.any())).thenReturn(false);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(catalogMgr.getCatalog("custom_catalog")).thenReturn(catalog);
            Mockito.when(catalog.isInternalCatalog()).thenReturn(false);
            Mockito.when(catalog.getProperties()).thenReturn(
                    ImmutableMap.of(CatalogMgr.ACCESS_CONTROLLER_CLASS_PROP, "mock.access.controller"));

            Assert.assertFalse(accessControllerManager.checkCtlPriv(
                    userIdentity, "custom_catalog", PrivPredicate.LOAD));
        }
    }

    @Test
    public void testCheckCtlPrivSkipCatalogPrivCheckWhenCatalogNotExist() {
        CatalogAccessController defaultAccessController = Mockito.mock(CatalogAccessController.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            AccessControllerManager accessControllerManager = createAccessControllerManager(defaultAccessController);
            UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");
            Config.skip_catalog_priv_check = true;

            Mockito.when(defaultAccessController.checkGlobalPriv(Mockito.any(), Mockito.any())).thenReturn(false);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(catalogMgr.getCatalog("not_exist_catalog")).thenReturn(null);

            Assert.assertFalse(accessControllerManager.checkCtlPriv(
                    userIdentity, "not_exist_catalog", PrivPredicate.SELECT));
        }
    }

    @Test
    public void testDryRunClosesTemporaryAccessController() {
        CatalogAccessController defaultAccessController = Mockito.mock(CatalogAccessController.class);
        CatalogAccessController temporaryAccessController = Mockito.mock(CatalogAccessController.class);
        AccessControllerFactory factory = Mockito.mock(AccessControllerFactory.class);
        AccessControllerManager accessControllerManager = createAccessControllerManager(defaultAccessController);
        ConcurrentHashMap<String, AccessControllerFactory> factories =
                Deencapsulation.getField(accessControllerManager, "accessControllerFactoriesCache");
        factories.put("test-controller", factory);
        Mockito.when(factory.createAccessController(ImmutableMap.of())).thenReturn(temporaryAccessController);
        ExternalCatalog catalog = mockCatalog("test_catalog", 1L);

        accessControllerManager.createAccessController(
                catalog, "test-controller", ImmutableMap.of(), true);

        Mockito.verify(temporaryAccessController).close();
        Assert.assertFalse(accessControllerManager.checkIfAccessControllerExist("test_catalog"));
    }

    @Test
    public void testRemoveClosesRegisteredAccessController() {
        CatalogAccessController defaultAccessController = Mockito.mock(CatalogAccessController.class);
        CatalogAccessController registeredAccessController = Mockito.mock(CatalogAccessController.class);
        AccessControllerFactory factory = Mockito.mock(AccessControllerFactory.class);
        AccessControllerManager accessControllerManager = createAccessControllerManager(defaultAccessController);
        ConcurrentHashMap<String, AccessControllerFactory> factories =
                Deencapsulation.getField(accessControllerManager, "accessControllerFactoriesCache");
        factories.put("test-controller", factory);
        Mockito.when(factory.createAccessController(ImmutableMap.of())).thenReturn(registeredAccessController);
        ExternalCatalog catalog = mockCatalog("test_catalog", 1L);

        withCurrentCatalog(catalog, () -> accessControllerManager.createAccessController(
                catalog, "test-controller", ImmutableMap.of(), false));
        accessControllerManager.removeAccessController("test_catalog", catalog.getId());

        Mockito.verify(registeredAccessController).close();
        Assert.assertFalse(accessControllerManager.checkIfAccessControllerExist("test_catalog"));
    }

    @Test
    public void testRemoveContinuesWhenAccessControllerCloseFails() {
        CatalogAccessController defaultAccessController = Mockito.mock(CatalogAccessController.class);
        CatalogAccessController registeredAccessController = Mockito.mock(CatalogAccessController.class);
        AccessControllerFactory factory = Mockito.mock(AccessControllerFactory.class);
        AccessControllerManager accessControllerManager = createAccessControllerManager(defaultAccessController);
        ConcurrentHashMap<String, AccessControllerFactory> factories =
                Deencapsulation.getField(accessControllerManager, "accessControllerFactoriesCache");
        factories.put("test-controller", factory);
        Mockito.when(factory.createAccessController(ImmutableMap.of())).thenReturn(registeredAccessController);
        Mockito.doThrow(new RuntimeException("plugin cleanup failure")).when(registeredAccessController).close();
        ExternalCatalog catalog = mockCatalog("test_catalog", 1L);

        withCurrentCatalog(catalog, () -> accessControllerManager.createAccessController(
                catalog, "test-controller", ImmutableMap.of(), false));
        accessControllerManager.removeAccessController("test_catalog", catalog.getId());

        Mockito.verify(registeredAccessController).close();
        Assert.assertFalse(accessControllerManager.checkIfAccessControllerExist("test_catalog"));
    }

    @Test
    public void testOldGenerationCannotRemoveReplacementController() {
        CatalogAccessController defaultAccessController = Mockito.mock(CatalogAccessController.class);
        CatalogAccessController oldController = Mockito.mock(CatalogAccessController.class);
        CatalogAccessController newController = Mockito.mock(CatalogAccessController.class);
        AccessControllerFactory factory = Mockito.mock(AccessControllerFactory.class);
        AccessControllerManager manager = createAccessControllerManager(defaultAccessController);
        ConcurrentHashMap<String, AccessControllerFactory> factories =
                Deencapsulation.getField(manager, "accessControllerFactoriesCache");
        factories.put("test-controller", factory);
        Mockito.when(factory.createAccessController(ImmutableMap.of())).thenReturn(oldController, newController);
        ExternalCatalog oldCatalog = mockCatalog("same_name", 10L);
        ExternalCatalog newCatalog = mockCatalog("same_name", 11L);

        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        AtomicReference<CatalogIf> currentCatalog = new AtomicReference<>(oldCatalog);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(catalogMgr.getCatalog("same_name")).thenAnswer(invocation -> currentCatalog.get());

            manager.createAccessController(oldCatalog, "test-controller", ImmutableMap.of(), false);
            currentCatalog.set(newCatalog);
            manager.createAccessController(newCatalog, "test-controller", ImmutableMap.of(), false);
            manager.removeAccessController("same_name", oldCatalog.getId());

            Assert.assertSame(newController, manager.getAccessControllerOrDefault("same_name"));
        }

        Mockito.verify(oldController).close();
        Mockito.verify(newController, Mockito.never()).close();
    }

    @Test
    public void testControllerPublishedAfterDropIsClosedInsteadOfCached() {
        CatalogAccessController defaultAccessController = Mockito.mock(CatalogAccessController.class);
        CatalogAccessController orphanController = Mockito.mock(CatalogAccessController.class);
        AccessControllerFactory factory = Mockito.mock(AccessControllerFactory.class);
        AccessControllerManager manager = createAccessControllerManager(defaultAccessController);
        ConcurrentHashMap<String, AccessControllerFactory> factories =
                Deencapsulation.getField(manager, "accessControllerFactoriesCache");
        factories.put("test-controller", factory);
        Mockito.when(factory.createAccessController(ImmutableMap.of())).thenReturn(orphanController);
        ExternalCatalog droppedCatalog = mockCatalog("dropped", 20L);

        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(catalogMgr.getCatalog("dropped")).thenReturn(null);

            manager.createAccessController(droppedCatalog, "test-controller", ImmutableMap.of(), false);
        }

        Mockito.verify(orphanController).close();
        Assert.assertFalse(manager.checkIfAccessControllerExist("dropped"));
    }

    @Test
    public void testRemovingFallbackAliasDoesNotCloseSharedDefaultController() {
        CatalogAccessController defaultAccessController = Mockito.mock(CatalogAccessController.class);
        AccessControllerManager manager = createAccessControllerManager(defaultAccessController);
        ExternalCatalog catalog = mockCatalog("fallback", 30L);

        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(catalogMgr.getCatalog("fallback")).thenReturn(catalog);

            Assert.assertSame(defaultAccessController, manager.getAccessControllerOrDefault("fallback"));
            manager.removeAccessController("fallback", catalog.getId());
        }

        Mockito.verify(defaultAccessController, Mockito.never()).close();
    }

    private ExternalCatalog mockCatalog(String name, long id) {
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn(name);
        Mockito.when(catalog.getId()).thenReturn(id);
        return catalog;
    }

    private void withCurrentCatalog(ExternalCatalog catalog, Runnable action) {
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(catalogMgr.getCatalog(catalog.getName())).thenReturn(catalog);
            action.run();
        }
    }

    private AccessControllerManager createAccessControllerManager(CatalogAccessController defaultAccessController) {
        AccessControllerManager accessControllerManager = new AccessControllerManager(new Auth());
        Deencapsulation.setField(accessControllerManager, "defaultAccessController", defaultAccessController);
        return accessControllerManager;
    }
}
