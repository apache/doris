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

package org.apache.doris.connector.maxcompute;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * R-004 part 1 — defensive test that the ODPS SDK loads and constructs an Odps client when the
 * MaxCompute connector is loaded under an isolated, child-first class loader (no credentials, no
 * network, CI-runnable).
 *
 * <p>In production the connector runs inside {@code ConnectorPluginManager}'s plugin isolation,
 * where {@code org.apache.doris.connector.} / {@code org.apache.doris.filesystem.} are parent-first
 * (the shared SPI) while the connector impl and its third-party deps — including the ODPS SDK
 * ({@code com.aliyun.odps.*}) — load child-first, getting an isolated copy per plugin. Risk R-004 is
 * that loading the ODPS SDK in such isolation breaks (NoClassDefFoundError / ClassCastException) or
 * that a per-plugin SDK copy poisons a process-wide singleton.</p>
 *
 * <p>This test reproduces the risk with a deliberately stricter loader: everything outside the JDK
 * is child-first, so the connector class and the whole ODPS SDK are defined by the isolated loader.
 * That is a superset of production isolation for the SDK, so passing here covers the production
 * policy. It asserts: (1) two isolated loaders define distinct connector classes (no shared static
 * state across plugins); (2) {@code createClient} builds an {@code Odps} under isolation with no
 * linkage error; (3) the SDK class is defined by the isolated loader, not leaked from the app loader;
 * (4) the SDK class differs across loaders (isolated, not a shared singleton).</p>
 */
public class OdpsClassloaderIsolationTest {

    private static final String FACTORY =
            "org.apache.doris.connector.maxcompute.MCConnectorClientFactory";

    @Test
    public void odpsClientConstructsUnderIsolatedChildFirstLoaderWithoutLeak() throws Exception {
        URL[] classpath = classpathUrls();
        // AK/SK auth builds the client fully offline (new AliyunAccount + new Odps; no network).
        Map<String, String> props = new HashMap<>();
        props.put(MCConnectorProperties.ACCESS_KEY, "test-ak");
        props.put(MCConnectorProperties.SECRET_KEY, "test-sk");

        try (IsolatedChildFirstClassLoader loaderA = new IsolatedChildFirstClassLoader(classpath);
                IsolatedChildFirstClassLoader loaderB = new IsolatedChildFirstClassLoader(classpath)) {

            Object odpsA = createIsolatedClient(loaderA, props);
            Object odpsB = createIsolatedClient(loaderB, props);

            Class<?> factoryA = loaderA.loadClass(FACTORY);
            Assertions.assertNotSame(MCConnectorClientFactory.class, factoryA,
                    "the isolated loader must define its own connector class, not reuse the app one");
            Assertions.assertNotSame(factoryA, loaderB.loadClass(FACTORY),
                    "two isolated plugin loaders must not share connector class identity");

            Assertions.assertEquals("com.aliyun.odps.Odps", odpsA.getClass().getName(),
                    "createClient must build an ODPS client even under classloader isolation");
            Assertions.assertSame(loaderA, odpsA.getClass().getClassLoader(),
                    "the ODPS SDK class must be defined by the isolated loader, not leaked from the app loader");
            Assertions.assertNotSame(odpsA.getClass(), odpsB.getClass(),
                    "the ODPS SDK must be isolated per plugin — no shared singleton class across loaders");
        }
    }

    /** Loads {@code MCConnectorClientFactory} through {@code loader} and builds an Odps reflectively. */
    private static Object createIsolatedClient(ClassLoader loader, Map<String, String> props)
            throws Exception {
        Class<?> factory = loader.loadClass(FACTORY);
        Assertions.assertSame(loader, factory.getClassLoader(),
                "sanity: the connector factory must be defined by the isolated loader");
        Method createClient = factory.getMethod("createClient", Map.class);
        Object odps = createClient.invoke(null, props);
        Assertions.assertNotNull(odps, "createClient must return a non-null ODPS client");
        return odps;
    }

    private static URL[] classpathUrls() throws Exception {
        String classpath = System.getProperty("java.class.path");
        String[] entries = classpath.split(File.pathSeparator);
        List<URL> urls = new ArrayList<>(entries.length);
        for (String entry : entries) {
            if (!entry.isEmpty()) {
                urls.add(new File(entry).toURI().toURL());
            }
        }
        return urls.toArray(new URL[0]);
    }

    /**
     * Child-first loader: defines every non-JDK class from its own URLs (delegating only JDK
     * packages to the parent), mirroring — and exceeding — the plugin isolation the connector runs
     * under in production.
     */
    private static final class IsolatedChildFirstClassLoader extends URLClassLoader {

        IsolatedChildFirstClassLoader(URL[] urls) {
            // Parent is the JDK-only loader, so connector + SDK classes fall through to this loader.
            super(urls, ClassLoader.getSystemClassLoader().getParent());
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
                Class<?> loaded = findLoadedClass(name);
                if (loaded == null) {
                    if (isJdkClass(name)) {
                        loaded = super.loadClass(name, false);
                    } else {
                        try {
                            loaded = findClass(name);
                        } catch (ClassNotFoundException notLocal) {
                            loaded = super.loadClass(name, false);
                        }
                    }
                }
                if (resolve) {
                    resolveClass(loaded);
                }
                return loaded;
            }
        }

        private static boolean isJdkClass(String name) {
            return name.startsWith("java.") || name.startsWith("javax.")
                    || name.startsWith("jdk.") || name.startsWith("sun.")
                    || name.startsWith("com.sun.") || name.startsWith("org.w3c.")
                    || name.startsWith("org.xml.");
        }
    }
}
