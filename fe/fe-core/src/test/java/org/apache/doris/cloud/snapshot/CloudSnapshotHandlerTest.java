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

package org.apache.doris.cloud.snapshot;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Enumeration;

public class CloudSnapshotHandlerTest {

    private String originalHandlerClass;
    private ClassLoader originalContextClassLoader;

    @Before
    public void setUp() {
        originalHandlerClass = Config.cloud_snapshot_handler_class;
        originalContextClassLoader = Thread.currentThread().getContextClassLoader();
        Config.cloud_snapshot_handler_class = CloudSnapshotHandler.class.getName();
        CloudSnapshotHandler.setSnapshotEnv(null);
    }

    @After
    public void tearDown() {
        Config.cloud_snapshot_handler_class = originalHandlerClass;
        Thread.currentThread().setContextClassLoader(originalContextClassLoader);
        CloudSnapshotHandler.setSnapshotEnv(null);
    }

    @Test
    public void testDefaultHandlerWithoutProvider() {
        Thread.currentThread().setContextClassLoader(new SnapshotHandlerClassLoader(null));
        CloudSnapshotHandler handler = CloudSnapshotHandler.getInstance();

        Assert.assertEquals(CloudSnapshotHandler.class, handler.getClass());
    }

    @Test
    public void testLoadHandlerFromServiceProvider() {
        Thread.currentThread().setContextClassLoader(
                new SnapshotHandlerClassLoader(ServiceLoadedSnapshotHandler.class));
        CloudSnapshotHandler handler = CloudSnapshotHandler.getInstance();

        Assert.assertTrue(handler instanceof ServiceLoadedSnapshotHandler);
    }

    @Test
    public void testConfiguredHandlerTakesPrecedenceOverServiceProvider() {
        Config.cloud_snapshot_handler_class = ConfiguredSnapshotHandler.class.getName();
        Thread.currentThread().setContextClassLoader(
                new SnapshotHandlerClassLoader(ServiceLoadedSnapshotHandler.class));

        CloudSnapshotHandler handler = CloudSnapshotHandler.getInstance();

        Assert.assertTrue(handler instanceof ConfiguredSnapshotHandler);
    }

    @Test
    public void testSnapshotEnvOverridesCurrentEnv() {
        Env snapshotEnv = Mockito.mock(Env.class);

        CloudSnapshotHandler.setSnapshotEnv(snapshotEnv);

        Assert.assertSame(snapshotEnv, CloudSnapshotHandler.getSnapshotEnv());
        Assert.assertSame(snapshotEnv, Env.getCurrentEnv());
    }

    public static class ServiceLoadedSnapshotHandler extends CloudSnapshotHandler {
    }

    public static class ConfiguredSnapshotHandler extends CloudSnapshotHandler {
    }

    private static class SnapshotHandlerClassLoader extends ClassLoader {

        private static final String SERVICE_FILE =
                "META-INF/services/" + CloudSnapshotHandler.class.getName();

        private final Class<? extends CloudSnapshotHandler> providerClass;

        SnapshotHandlerClassLoader(Class<? extends CloudSnapshotHandler> providerClass) {
            super(CloudSnapshotHandlerTest.class.getClassLoader());
            this.providerClass = providerClass;
        }

        @Override
        public Enumeration<URL> getResources(String name) {
            if (!SERVICE_FILE.equals(name) || providerClass == null) {
                return Collections.emptyEnumeration();
            }
            return Collections.enumeration(Collections.singletonList(serviceFileUrl()));
        }

        private URL serviceFileUrl() {
            byte[] content = providerClass.getName().getBytes(StandardCharsets.UTF_8);
            try {
                return new URL("synthetic", "", -1, SERVICE_FILE, new URLStreamHandler() {
                    @Override
                    protected URLConnection openConnection(URL url) {
                        return new URLConnection(url) {
                            @Override
                            public void connect() {
                            }

                            @Override
                            public InputStream getInputStream() {
                                return new ByteArrayInputStream(content);
                            }
                        };
                    }
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
