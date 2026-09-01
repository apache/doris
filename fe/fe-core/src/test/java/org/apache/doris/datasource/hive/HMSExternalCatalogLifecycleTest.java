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

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.hudi.HudiExternalMetaCache;
import org.apache.doris.mysql.privilege.AccessControllerManager;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.function.Supplier;

public class HMSExternalCatalogLifecycleTest {

    @Test
    public void testScanRuntimeAndResetUseLifecycleFenceBeforeCatalogMonitor() {
        TestCatalog catalog = new TestCatalog();
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getAccessManager()).thenReturn(Mockito.mock(AccessControllerManager.class));
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        HudiExternalMetaCache hudiCache = Mockito.mock(HudiExternalMetaCache.class);
        HudiExternalMetaCache.FsViewGeneration fsViewGeneration =
                Mockito.mock(HudiExternalMetaCache.FsViewGeneration.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        Mockito.when(cacheMgr.hudi(catalog.getId())).thenAnswer(invocation -> {
            Assert.assertTrue("Hudi cache lookup must be inside the lifecycle fence",
                    catalog.lifecycleFenceHeld);
            return hudiCache;
        });
        Mockito.when(hudiCache.captureFsViewGeneration(catalog.getId()))
                .thenReturn(fsViewGeneration);
        Mockito.when(cacheMgr.withCatalogLifecycleLock(
                Mockito.eq(catalog.getId()), Mockito.<Supplier<Object>>any()))
                .thenAnswer(invocation -> {
                    Assert.assertFalse("catalog monitor must not be held before lifecycle fence acquisition",
                            Thread.holdsLock(catalog));
                    @SuppressWarnings("unchecked")
                    Supplier<Object> action = invocation.getArgument(1);
                    catalog.lifecycleFenceHeld = true;
                    try {
                        return action.get();
                    } finally {
                        catalog.lifecycleFenceHeld = false;
                    }
                });
        Mockito.doAnswer(invocation -> {
            Assert.assertTrue("cache retirement must be inside the lifecycle fence",
                    catalog.lifecycleFenceHeld);
            Assert.assertTrue("cache retirement must run while the catalog runtime is frozen",
                    Thread.holdsLock(catalog));
            return null;
        }).when(cacheMgr).removeCatalogByEngine(Mockito.eq(catalog.getId()), Mockito.anyString());

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            HMSExternalCatalog.HudiScanRuntimeContext runtime = catalog.getHudiScanRuntimeContext();
            Assert.assertSame(catalog.authenticator, runtime.getAuthenticator());
            Assert.assertSame(fsViewGeneration, runtime.getFsViewGeneration());
            catalog.resetToUninitialized(false);
        }
    }

    private static final class TestCatalog extends HMSExternalCatalog {
        private final ExecutionAuthenticator authenticator = new ExecutionAuthenticator() { };
        private boolean lifecycleFenceHeld;

        private TestCatalog() {
            super(7L, "hms-lifecycle", null, java.util.Collections.emptyMap(), "");
            initialized = true;
            executionAuthenticator = authenticator;
        }

        @Override
        protected void initLocalObjectsImpl() {
        }
    }
}
