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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.ConnectorProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Pins the class-loading invariant stated on {@link LanceConnectorProvider}: discovering the provider,
 * validating properties with it and asking it for its security surface must not load any {@code org.lance.*}
 * or {@code io.questdb.*} class.
 *
 * <p>The plugin loader instantiates every discovered factory before it rejects one whose type name is
 * already claimed, and the engine asks a provider for its security surface by type without building a
 * connector. Reaching lance-core on either path loads the JNI library from another classloader: lance-core
 * extracts it to a fresh temporary file every time, so nothing fails, and the FE quietly maps a second copy
 * of a library of about 180 MB. A plain "it constructs fine" test cannot see that.
 *
 * <p>The test re-defines the whole connector package in a loader that refuses those packages, then uses the
 * provider the way the engine does before any catalog exists.
 */
class LanceConnectorProviderIsolationTest {

    @Test
    void usingTheProviderBeforeAnyCatalogLoadsNoLanceClass() throws Exception {
        LanceBlockingClassLoader loader = new LanceBlockingClassLoader(getClass().getClassLoader());

        Class<?> providerClass = loader.loadClass(LanceConnectorProvider.class.getName());
        Assertions.assertNotSame(LanceConnectorProvider.class, providerClass,
                "The test must exercise a provider defined by the blocking loader, not the one already"
                        + " loaded by the test classloader");

        ConnectorProvider provider;
        try {
            provider = (ConnectorProvider) providerClass.getDeclaredConstructor().newInstance();
        } catch (Throwable t) {
            throw new AssertionError("Instantiating LanceConnectorProvider reached Lance classes "
                    + loader.blockedNames() + "; keep the fields, static initializer and constructor free of"
                    + " org.lance types and let them first appear when a catalog reads metadata", t);
        }

        Assertions.assertEquals("lance", provider.getType());
        Assertions.assertTrue(provider.isStandaloneCatalogType(),
                "lance is a catalog type a user writes in CREATE CATALOG");
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put("warehouse", "/tmp/lance");
        provider.validateProperties(properties);
        provider.validatePropertiesForUpdate(properties, Collections.singletonMap("warehouse", "/tmp/other"));
        Assertions.assertFalse(provider.securitySurface().sensitiveKeys().isEmpty());

        Assertions.assertEquals(List.of(), loader.blockedNames(),
                "Using the provider before a catalog exists must not load any org.lance or io.questdb class");
    }

    /**
     * Child-first for the connector's own package, and a hard stop for lance-core and its JNI loader.
     * Re-defining the whole package (not just the provider) is deliberate: it makes the check transitive.
     */
    private static final class LanceBlockingClassLoader extends ClassLoader {

        private static final String[] BLOCKED_PREFIXES = {"org.lance.", "io.questdb."};
        private static final String OWNED_PREFIX = "org.apache.doris.connector.lance.";

        private final List<String> blocked = new ArrayList<>();

        LanceBlockingClassLoader(ClassLoader parent) {
            super(parent);
        }

        List<String> blockedNames() {
            return blocked;
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            for (String prefix : BLOCKED_PREFIXES) {
                if (name.startsWith(prefix)) {
                    blocked.add(name);
                    throw new ClassNotFoundException("blocked by "
                            + LanceBlockingClassLoader.class.getSimpleName() + ": " + name);
                }
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
