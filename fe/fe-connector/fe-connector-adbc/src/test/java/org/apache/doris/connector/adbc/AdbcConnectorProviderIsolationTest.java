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

package org.apache.doris.connector.adbc;

import org.apache.doris.connector.spi.ConnectorProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Pins the class-loading invariant stated on {@link AdbcConnectorProvider}: discovering and instantiating
 * the provider must not load any {@code org.apache.arrow.adbc.*} class.
 *
 * <p>WHY this matters, and why a plain "it constructs fine" test would not catch it: the plugin loader
 * ({@code DirectoryPluginRuntimeManager#loadAll}) builds the plugin classloader and instantiates every
 * discovered factory, and only afterwards rejects one whose type name is already claimed. So when the same
 * connector exists both as a classpath built-in and as a directory plugin — the normal shape in tests and
 * embedded setups — the directory copy IS constructed once, in its own classloader, before being discarded.
 * If that construction reached an ADBC class, it would run a second {@code System.load} of the JNI shim from
 * a second classloader, which the JVM answers with {@code UnsatisfiedLinkError}, and FE would lose the
 * connector it had already loaded correctly. Deferring every ADBC reference into method bodies keeps the
 * discarded instance inert.
 *
 * <p>The test reproduces that shape: it re-defines the whole connector package in a loader that refuses
 * {@code org.apache.arrow.adbc.*}, then constructs the provider and asks it for its type. Verified by
 * mutation: adding {@code private static final Object M = AdbcDriver.PARAM_URI;} to the provider makes this
 * fail, naming {@code org.apache.arrow.adbc.core.AdbcDriver}.
 *
 * <p>One shape is invisible here and that is correct, not a gap: referencing a {@code static final String}
 * of an ADBC class (e.g. {@code AdbcDriver.PARAM_URL}) is inlined by javac into a constant, leaving no
 * symbolic reference — so it loads no class and cannot trigger the failure this guard exists to prevent.
 */
class AdbcConnectorProviderIsolationTest {

    @Test
    void constructingTheProviderLoadsNoAdbcClass() throws Exception {
        AdbcBlockingClassLoader loader = new AdbcBlockingClassLoader(getClass().getClassLoader());

        Class<?> providerClass = loader.loadClass(AdbcConnectorProvider.class.getName());
        Assertions.assertNotSame(AdbcConnectorProvider.class, providerClass,
                "The test must exercise a provider defined by the blocking loader, not the one already"
                        + " loaded by the test classloader");

        Object provider;
        try {
            provider = providerClass.getDeclaredConstructor().newInstance();
        } catch (Throwable t) {
            throw new AssertionError("Instantiating AdbcConnectorProvider reached ADBC classes "
                    + loader.blockedNames() + "; keep the fields, static initializer and constructor free of"
                    + " org.apache.arrow.adbc types and let them first appear inside create()", t);
        }

        Assertions.assertEquals("adbc", ((ConnectorProvider) provider).getType(),
                "getType() must be answerable without any ADBC class");
        Assertions.assertTrue(((ConnectorProvider) provider).isStandaloneCatalogType(),
                "adbc is a catalog type a user writes in CREATE CATALOG");

        Assertions.assertEquals(List.of(), loader.blockedNames(),
                "Provider discovery must not load any org.apache.arrow.adbc class");
    }

    /**
     * Child-first for the connector's own package, and a hard stop for {@code org.apache.arrow.adbc.*}.
     * Re-defining the whole package (not just the provider) is deliberate: it makes the check transitive,
     * so a provider that stayed clean itself but constructed a connector class whose static initializer
     * touched ADBC would still fail here.
     */
    private static final class AdbcBlockingClassLoader extends ClassLoader {

        private static final String BLOCKED_PREFIX = "org.apache.arrow.adbc.";
        private static final String OWNED_PREFIX = "org.apache.doris.connector.adbc.";

        private final List<String> blocked = new ArrayList<>();

        AdbcBlockingClassLoader(ClassLoader parent) {
            super(parent);
        }

        List<String> blockedNames() {
            return blocked;
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.startsWith(BLOCKED_PREFIX)) {
                blocked.add(name);
                throw new ClassNotFoundException("blocked by " + AdbcBlockingClassLoader.class.getSimpleName()
                        + ": " + name);
            }
            if (!name.startsWith(OWNED_PREFIX)) {
                return super.loadClass(name, resolve);
            }
            synchronized (getClassLoadingLock(name)) {
                Class<?> loaded = findLoadedClass(name);
                if (loaded == null) {
                    byte[] bytes = readClassBytes(name);
                    loaded = defineClass(name, bytes, 0, bytes.length);
                }
                if (resolve) {
                    resolveClass(loaded);
                }
                return loaded;
            }
        }

        private byte[] readClassBytes(String name) throws ClassNotFoundException {
            String resource = name.replace('.', '/') + ".class";
            try (InputStream in = getParent().getResourceAsStream(resource)) {
                if (in == null) {
                    throw new ClassNotFoundException(name);
                }
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                byte[] buffer = new byte[8192];
                int read;
                while ((read = in.read(buffer)) > 0) {
                    out.write(buffer, 0, read);
                }
                return out.toByteArray();
            } catch (IOException e) {
                throw new ClassNotFoundException(name, e);
            }
        }
    }
}
