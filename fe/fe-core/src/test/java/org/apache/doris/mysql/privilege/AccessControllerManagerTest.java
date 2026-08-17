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
import org.apache.doris.authorization.AccessContext;
import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.AuthorizedSubject;
import org.apache.doris.authorization.spi.AuthorizationPlugin;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.qe.ConnectContext;

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

            Mockito.when(defaultAccessController.checkCtlPriv(
                    Mockito.any(), Mockito.anyString(), Mockito.any())).thenReturn(false);
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

            Mockito.when(defaultAccessController.checkCtlPriv(
                    Mockito.any(), Mockito.anyString(), Mockito.any())).thenReturn(true);
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

            Mockito.when(defaultAccessController.checkCtlPriv(
                    Mockito.any(), Mockito.anyString(), Mockito.any())).thenReturn(false);
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

            Assert.assertSame(newController, controllerOf(manager, "same_name"));
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

            Assert.assertSame(defaultAccessController, controllerOf(manager, "fallback"));
            manager.removeAccessController("fallback", catalog.getId());
        }

        Mockito.verify(defaultAccessController, Mockito.never()).close();
    }

    /**
     * The circumstances of a check come from the connection the caller handed over, not from the thread.
     *
     * <p>A check can run before its connection is installed on the thread - {@code BaseController.checkCookie}
     * authorizes before it builds one - and a thread out of a pool carries whatever the request before it
     * left behind, so reading the thread there yields not "no circumstances" but another client's.
     */
    @Test
    public void testACheckSeesTheConnectionItWasGivenNotTheOneOnTheThread() {
        AtomicReference<String> seenClientIp = new AtomicReference<>();
        AccessControllerManager manager = new AccessControllerManager(new Auth());
        Deencapsulation.setField(manager, "defaultAccessController", recordsClientIp(seenClientIp));

        ConnectContext leftOnTheThread = new ConnectContext();
        leftOnTheThread.setRemoteIP("10.0.0.1");
        leftOnTheThread.setCurrentUserIdentity(UserIdentity.ROOT);
        leftOnTheThread.setThreadLocalInfo();
        try {
            ConnectContext handedOver = new ConnectContext();
            handedOver.setRemoteIP("10.0.0.2");
            handedOver.setCurrentUserIdentity(UserIdentity.ROOT);

            manager.checkGlobalPriv(handedOver, PrivPredicate.ADMIN);
            Assert.assertEquals("10.0.0.2", seenClientIp.get());

            // And the thread is still the fallback for the callers that have no connection to hand over.
            seenClientIp.set(null);
            manager.checkGlobalPriv(UserIdentity.ROOT, PrivPredicate.ADMIN);
            Assert.assertEquals("10.0.0.1", seenClientIp.get());
        } finally {
            ConnectContext.remove();
        }
    }

    /** A source that answers yes and remembers the client address it was told about. */
    private static AuthorizationPlugin recordsClientIp(AtomicReference<String> seenClientIp) {
        return new AuthorizationPlugin() {
            @Override
            public String name() {
                return "records-client-ip";
            }

            @Override
            public void checkPrivilege(AuthorizedSubject subject, AuthorizedResource resource,
                    AccessRequirement requirement, AccessContext context) {
                seenClientIp.set(context.getClientIp().orElse(null));
            }
        };
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
        Deencapsulation.setField(accessControllerManager, "defaultAccessController",
                new LegacyAccessControllerPlugin("mock", defaultAccessController,
                        (subject, requirement) -> false));
        return accessControllerManager;
    }

    /** The controller behind an installed source; what the manager holds is the source, not the controller. */
    private CatalogAccessController controllerOf(AccessControllerManager manager, String ctl) {
        return ((LegacyAccessControllerPlugin) manager.getAccessControllerOrDefault(ctl)).getController();
    }
}
