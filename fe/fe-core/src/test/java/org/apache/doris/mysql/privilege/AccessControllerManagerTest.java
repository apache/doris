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

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

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

    private AccessControllerManager createAccessControllerManager(CatalogAccessController defaultAccessController) {
        AccessControllerManager accessControllerManager = new AccessControllerManager(new Auth());
        Deencapsulation.setField(accessControllerManager, "defaultAccessController", defaultAccessController);
        return accessControllerManager;
    }
}
