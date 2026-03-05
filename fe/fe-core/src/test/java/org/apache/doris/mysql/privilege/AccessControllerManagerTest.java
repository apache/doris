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
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
    public void testCheckCtlPrivSkipCatalogPrivCheckWithCustomAccessController(
            @Injectable CatalogAccessController defaultAccessController,
            @Mocked Env env,
            @Mocked CatalogMgr catalogMgr,
            @Mocked CatalogIf catalog) {
        AccessControllerManager accessControllerManager = createAccessControllerManager(defaultAccessController);
        UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");
        Config.skip_catalog_priv_check = true;

        new Expectations() {
            {
                defaultAccessController.checkGlobalPriv((UserIdentity) any, (PrivPredicate) any);
                minTimes = 0;
                result = false;

                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getCatalogMgr();
                minTimes = 0;
                result = catalogMgr;

                catalogMgr.getCatalog("custom_catalog");
                minTimes = 0;
                result = catalog;

                catalog.isInternalCatalog();
                minTimes = 0;
                result = false;

                catalog.getProperties();
                minTimes = 0;
                result = ImmutableMap.of(CatalogMgr.ACCESS_CONTROLLER_CLASS_PROP, "mock.access.controller");
            }
        };

        Assert.assertTrue(accessControllerManager.checkCtlPriv(userIdentity, "custom_catalog", PrivPredicate.SELECT));
    }

    @Test
    public void testCheckCtlPrivSkipCatalogPrivCheckWithoutCustomAccessController(
            @Injectable CatalogAccessController defaultAccessController,
            @Mocked Env env,
            @Mocked CatalogMgr catalogMgr,
            @Mocked CatalogIf catalog) {
        AccessControllerManager accessControllerManager = createAccessControllerManager(defaultAccessController);
        UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");
        Config.skip_catalog_priv_check = true;

        new Expectations() {
            {
                defaultAccessController.checkGlobalPriv((UserIdentity) any, (PrivPredicate) any);
                minTimes = 0;
                result = false;

                defaultAccessController.checkCtlPriv(anyBoolean, (UserIdentity) any, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = false;

                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getCatalogMgr();
                minTimes = 0;
                result = catalogMgr;

                catalogMgr.getCatalog("custom_catalog");
                minTimes = 0;
                result = catalog;

                catalog.isInternalCatalog();
                minTimes = 0;
                result = false;

                catalog.getProperties();
                minTimes = 0;
                result = ImmutableMap.of("type", "test");
            }
        };

        Assert.assertFalse(accessControllerManager.checkCtlPriv(userIdentity, "custom_catalog", PrivPredicate.SELECT));
    }

    @Test
    public void testCheckCtlPrivSkipCatalogPrivCheckWhenCatalogNotExist(
            @Injectable CatalogAccessController defaultAccessController,
            @Mocked Env env,
            @Mocked CatalogMgr catalogMgr) {
        AccessControllerManager accessControllerManager = createAccessControllerManager(defaultAccessController);
        UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");
        Config.skip_catalog_priv_check = true;

        new Expectations() {
            {
                defaultAccessController.checkGlobalPriv((UserIdentity) any, (PrivPredicate) any);
                minTimes = 0;
                result = false;

                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getCatalogMgr();
                minTimes = 0;
                result = catalogMgr;

                catalogMgr.getCatalog("not_exist_catalog");
                minTimes = 0;
                result = null;
            }
        };

        Assert.assertFalse(accessControllerManager.checkCtlPriv(userIdentity,
                "not_exist_catalog", PrivPredicate.SELECT));
    }

    private AccessControllerManager createAccessControllerManager(CatalogAccessController defaultAccessController) {
        AccessControllerManager accessControllerManager = new AccessControllerManager(new Auth());
        Deencapsulation.setField(accessControllerManager, "defaultAccessController", defaultAccessController);
        return accessControllerManager;
    }
}
