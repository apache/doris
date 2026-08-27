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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.RefreshManager;
import org.apache.doris.catalog.constraint.ConstraintManager;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.event.ConnectorEventSource;
import org.apache.doris.connector.spi.event.EventPollRequest;
import org.apache.doris.connector.spi.event.EventPollResult;
import org.apache.doris.connector.spi.event.MetastoreChangeDescriptor;
import org.apache.doris.connector.spi.event.MetastoreChangeDescriptor.Op;
import org.apache.doris.datasource.log.CatalogLog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.persist.EditLog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

public class MetastoreEventSyncDriverClassLoaderTest {

    @Test
    public void eventSourceAcquisitionUsesConnectorClassLoaderAndRestoresCaller() throws Exception {
        ClassLoader apiLoader = Connector.class.getClassLoader();
        AtomicReference<ClassLoader> acquisitionClassLoader = new AtomicReference<>();

        try (URLClassLoader connectorLoader = new URLClassLoader(new URL[0], apiLoader);
                URLClassLoader callerLoader = new URLClassLoader(new URL[0], null)) {
            Connector connector = (Connector) Proxy.newProxyInstance(
                    connectorLoader, new Class<?>[] {Connector.class}, (proxy, method, args) -> {
                        if ("getEventSource".equals(method.getName())) {
                            acquisitionClassLoader.set(Thread.currentThread().getContextClassLoader());
                            return null;
                        }
                        if ("getMetadata".equals(method.getName()) || "close".equals(method.getName())) {
                            return null;
                        }
                        return handleObjectMethod(proxy, method, args);
                    });

            PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
            Mockito.when(catalog.isInitialized()).thenReturn(true);
            Mockito.when(catalog.getConnector()).thenReturn(connector);
            CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
            Mockito.when(catalogMgr.getCatalogIds()).thenReturn(Collections.singletonList(1L));
            Mockito.doReturn(catalog).when(catalogMgr).getCatalog(1L);
            Env env = Mockito.mock(Env.class);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);

            Thread thread = Thread.currentThread();
            ClassLoader original = thread.getContextClassLoader();
            try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
                envStatic.when(Env::getCurrentEnv).thenReturn(env);
                thread.setContextClassLoader(callerLoader);
                Deencapsulation.invoke(new MetastoreEventSyncDriver(), "realRun");

                Assertions.assertSame(connectorLoader, acquisitionClassLoader.get());
                Assertions.assertSame(callerLoader, thread.getContextClassLoader());
            } finally {
                thread.setContextClassLoader(original);
            }
        }
    }

    @Test
    public void masterFullRefreshUsesConnectorClassLoaderAndRestoresCaller() throws Exception {
        ClassLoader apiLoader = Connector.class.getClassLoader();
        AtomicReference<ClassLoader> pollClassLoader = new AtomicReference<>();
        AtomicReference<ClassLoader> refreshClassLoader = new AtomicReference<>();
        AtomicReference<CatalogLog> refreshLog = new AtomicReference<>();

        try (URLClassLoader eventSourceLoader = new URLClassLoader(new URL[0], apiLoader);
                URLClassLoader connectorLoader = new URLClassLoader(new URL[0], apiLoader);
                URLClassLoader callerLoader = new URLClassLoader(new URL[0], null)) {
            ConnectorEventSource eventSource = (ConnectorEventSource) Proxy.newProxyInstance(
                    eventSourceLoader, new Class<?>[] {ConnectorEventSource.class}, (proxy, method, args) -> {
                        if ("pollOnce".equals(method.getName())) {
                            pollClassLoader.set(Thread.currentThread().getContextClassLoader());
                            return EventPollResult.ofFullRefresh(2L);
                        }
                        if ("getCurrentEventId".equals(method.getName())) {
                            return 2L;
                        }
                        return handleObjectMethod(proxy, method, args);
                    });
            Connector connector = (Connector) Proxy.newProxyInstance(
                    connectorLoader, new Class<?>[] {Connector.class}, (proxy, method, args) -> {
                        if ("getMetadata".equals(method.getName()) || "close".equals(method.getName())) {
                            return null;
                        }
                        return handleObjectMethod(proxy, method, args);
                    });

            PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
            Mockito.when(catalog.getId()).thenReturn(1L);
            Mockito.when(catalog.getName()).thenReturn("test_catalog");
            RefreshManager refreshManager = Mockito.mock(RefreshManager.class);
            Mockito.doAnswer(invocation -> {
                refreshClassLoader.set(Thread.currentThread().getContextClassLoader());
                refreshLog.set(invocation.getArgument(0));
                return null;
            }).when(refreshManager).replayRefreshCatalog(Mockito.any(CatalogLog.class));
            ExternalMetaIdMgr externalMetaIdMgr = Mockito.mock(ExternalMetaIdMgr.class);
            EditLog editLog = Mockito.mock(EditLog.class);
            ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
            Env env = Mockito.mock(Env.class);
            Mockito.when(env.isMaster()).thenReturn(true);
            Mockito.when(env.getRefreshManager()).thenReturn(refreshManager);
            Mockito.when(env.getExternalMetaIdMgr()).thenReturn(externalMetaIdMgr);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
            Mockito.when(constraintManager.applyMetastoreConstraintMutation(Mockito.any()))
                    .thenReturn(Collections.emptyList());

            Thread thread = Thread.currentThread();
            ClassLoader original = thread.getContextClassLoader();
            try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
                envStatic.when(Env::getCurrentEnv).thenReturn(env);
                thread.setContextClassLoader(callerLoader);
                Deencapsulation.invoke(new MetastoreEventSyncDriver(), "syncCatalog",
                        catalog, connector, eventSource);

                Assertions.assertSame(eventSourceLoader, pollClassLoader.get());
                Assertions.assertSame(connectorLoader, refreshClassLoader.get());
                Assertions.assertEquals(1L, refreshLog.get().getCatalogId());
                Assertions.assertTrue(refreshLog.get().isInvalidCache());
                Assertions.assertSame(callerLoader, thread.getContextClassLoader());
                Mockito.verify(externalMetaIdMgr).replayMetaIdMappingsLog(Mockito.any());
                Mockito.verify(editLog).logMetaIdMappingsLog(Mockito.any());
            } finally {
                thread.setContextClassLoader(original);
            }
        }
    }

    @Test
    public void pollAndDescriptorApplicationUseTheirPluginClassLoaders() throws Exception {
        assertDescriptorApplicationClassLoader(false);
    }

    @Test
    public void descriptorFailureRestoresCallerClassLoaderBeforePropagating() throws Exception {
        assertDescriptorApplicationClassLoader(true);
    }

    @Test
    public void pollingErrorDoesNotLeaveDriverPermanentlyRunning() {
        ConnectorEventSource eventSource = Mockito.mock(ConnectorEventSource.class);
        Mockito.when(eventSource.pollOnce(Mockito.any())).thenThrow(new AssertionError("injected linkage error"));
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getEventSource()).thenReturn(eventSource);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.isInitialized()).thenReturn(true);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.when(catalogMgr.getCatalogIds()).thenReturn(Collections.singletonList(1L));
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog(1L);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);

        MetastoreEventSyncDriver driver = new MetastoreEventSyncDriver();
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertThrows(AssertionError.class, driver::runAfterCatalogReady);
            Assertions.assertThrows(AssertionError.class, driver::runAfterCatalogReady);
        }

        Mockito.verify(eventSource, Mockito.times(2)).pollOnce(Mockito.any());
    }

    private void assertDescriptorApplicationClassLoader(boolean failInvalidation) throws Exception {
        ClassLoader apiLoader = Connector.class.getClassLoader();
        AtomicReference<ClassLoader> pollClassLoader = new AtomicReference<>();
        AtomicReference<ClassLoader> invalidationClassLoader = new AtomicReference<>();
        AtomicReference<ClassLoader> mutationClassLoader = new AtomicReference<>();
        MetastoreChangeDescriptor descriptor = MetastoreChangeDescriptor.forDatabase(
                Op.REGISTER_DATABASE, "db1", null, 1L, 2L);

        try (URLClassLoader eventSourceLoader = new URLClassLoader(new URL[0], apiLoader);
                URLClassLoader connectorLoader = new URLClassLoader(new URL[0], apiLoader);
                URLClassLoader callerLoader = new URLClassLoader(new URL[0], null)) {
            ConnectorEventSource eventSource = (ConnectorEventSource) Proxy.newProxyInstance(
                    eventSourceLoader, new Class<?>[] {ConnectorEventSource.class}, (proxy, method, args) -> {
                        if ("pollOnce".equals(method.getName())) {
                            EventPollRequest request = (EventPollRequest) args[0];
                            Assertions.assertFalse(request.isMaster());
                            Assertions.assertEquals(7L, request.getMasterUpperBound());
                            pollClassLoader.set(Thread.currentThread().getContextClassLoader());
                            return EventPollResult.ofChanges(1L, Collections.singletonList(descriptor));
                        }
                        if ("getCurrentEventId".equals(method.getName())) {
                            return 1L;
                        }
                        return handleObjectMethod(proxy, method, args);
                    });
            Connector connector = (Connector) Proxy.newProxyInstance(
                    connectorLoader, new Class<?>[] {Connector.class}, (proxy, method, args) -> {
                        if ("invalidateDb".equals(method.getName())) {
                            invalidationClassLoader.set(Thread.currentThread().getContextClassLoader());
                            if (failInvalidation) {
                                throw new IllegalStateException("expected invalidation failure");
                            }
                            return null;
                        }
                        if ("getMetadata".equals(method.getName()) || "close".equals(method.getName())) {
                            return null;
                        }
                        return handleObjectMethod(proxy, method, args);
                    });

            PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
            Mockito.when(catalog.getId()).thenReturn(1L);
            Mockito.when(catalog.getName()).thenReturn("test_catalog");
            Mockito.when(catalog.getLastSyncedMetastoreEventId()).thenReturn(7L);
            Mockito.when(catalog.canonicalLocalDatabaseNameFromRemote("db1")).thenReturn("db1");
            CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
            Mockito.doAnswer(invocation -> {
                mutationClassLoader.set(Thread.currentThread().getContextClassLoader());
                return null;
            }).when(catalogMgr).registerExternalDatabaseFromEvent("db1", "db1", "test_catalog");
            Env env = Mockito.mock(Env.class);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(env.isMaster()).thenReturn(false);

            Thread thread = Thread.currentThread();
            ClassLoader original = thread.getContextClassLoader();
            try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
                envStatic.when(Env::getCurrentEnv).thenReturn(env);
                thread.setContextClassLoader(callerLoader);
                if (failInvalidation) {
                    Assertions.assertThrows(RuntimeException.class, () -> Deencapsulation.invoke(
                            new MetastoreEventSyncDriver(), "syncCatalog",
                            catalog, connector, eventSource));
                    Mockito.verifyNoInteractions(catalogMgr);
                } else {
                    Deencapsulation.invoke(new MetastoreEventSyncDriver(), "syncCatalog",
                            catalog, connector, eventSource);
                    Mockito.verify(catalogMgr)
                            .registerExternalDatabaseFromEvent("db1", "db1", "test_catalog");
                }

                Assertions.assertSame(eventSourceLoader, pollClassLoader.get());
                Assertions.assertSame(connectorLoader, invalidationClassLoader.get());
                if (!failInvalidation) {
                    Assertions.assertSame(connectorLoader, mutationClassLoader.get());
                }
                Assertions.assertSame(callerLoader, thread.getContextClassLoader());
            } finally {
                thread.setContextClassLoader(original);
            }
        }
    }

    private static Object handleObjectMethod(Object proxy, Method method, Object[] args) {
        if ("toString".equals(method.getName())) {
            return proxy.getClass().getName();
        }
        if ("hashCode".equals(method.getName())) {
            return System.identityHashCode(proxy);
        }
        if ("equals".equals(method.getName())) {
            return proxy == args[0];
        }
        throw new UnsupportedOperationException("Unexpected proxy method: " + method);
    }
}
