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

package org.apache.doris.connector.metastore.spi;

import org.apache.doris.connector.metastore.MetaStoreProperties;
import org.apache.doris.connector.metastore.spi.dlf.DlfMetaStorePropertiesImpl;
import org.apache.doris.connector.metastore.spi.fs.FileSystemMetaStorePropertiesImpl;
import org.apache.doris.connector.metastore.spi.fs.FileSystemMetaStoreProvider;
import org.apache.doris.connector.metastore.spi.hms.HmsMetaStorePropertiesImpl;
import org.apache.doris.connector.metastore.spi.hms.HmsMetaStoreProvider;
import org.apache.doris.connector.metastore.spi.jdbc.JdbcMetaStorePropertiesImpl;
import org.apache.doris.connector.metastore.spi.rest.RestMetaStorePropertiesImpl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Verifies the ServiceLoader-based discovery: all 5 built-in providers register, and the first-hit
 * dispatcher selects the right backend by {@code paimon.catalog.type} (no central switch, no enum).
 */
public class MetaStoreProvidersDispatchTest {

    private static Map<String, String> typed(String flavor) {
        Map<String, String> m = new HashMap<>();
        if (flavor != null) {
            m.put("paimon.catalog.type", flavor);
        }
        m.put("warehouse", "wh");
        return m;
    }

    private static String providerOf(String flavor) {
        return MetaStoreProviders.bind(typed(flavor), Collections.emptyMap()).providerName();
    }

    @Test
    public void dispatchesEachFlavorToItsBackend() {
        Assertions.assertEquals("HMS", providerOf("hms"));
        Assertions.assertEquals("DLF", providerOf("dlf"));
        Assertions.assertEquals("REST", providerOf("rest"));
        Assertions.assertEquals("JDBC", providerOf("jdbc"));
        Assertions.assertEquals("FILESYSTEM", providerOf("filesystem"));
        // case-insensitive flavor
        Assertions.assertEquals("HMS", providerOf("HMS"));
    }

    @Test
    public void absentTypeDefaultsToFilesystem() {
        Assertions.assertEquals("FILESYSTEM", providerOf(null));
    }

    @Test
    public void unknownTypeHasNoSupportingProvider() {
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> MetaStoreProviders.bind(typed("nessie"), Collections.emptyMap()));
        Assertions.assertTrue(ex.getMessage().startsWith("No MetaStoreProvider supports the given properties"),
                ex.getMessage());
    }

    @Test
    public void allFiveProvidersAreRegistered() {
        Assertions.assertTrue(MetaStoreProviders.registeredNames()
                .containsAll(java.util.Arrays.asList("HMS", "DLF", "REST", "JDBC", "FILESYSTEM")),
                "registered=" + MetaStoreProviders.registeredNames());
    }

    @Test
    public void boundPropertiesExposeRawAndProvider() {
        MetaStoreProperties ms = MetaStoreProviders.bind(typed("hms"), Collections.emptyMap());
        Assertions.assertEquals("wh", ms.rawProperties().get("warehouse"));
    }

    @Test
    public void dispatchReturnsTheWiredConcreteImpl() {
        // providerName() is a hardcoded literal; assert the actual bound type to catch a mis-wired bind().
        Assertions.assertTrue(MetaStoreProviders.bind(typed("hms"), Collections.emptyMap())
                instanceof HmsMetaStorePropertiesImpl);
        Assertions.assertTrue(MetaStoreProviders.bind(typed("dlf"), Collections.emptyMap())
                instanceof DlfMetaStorePropertiesImpl);
        Assertions.assertTrue(MetaStoreProviders.bind(typed("rest"), Collections.emptyMap())
                instanceof RestMetaStorePropertiesImpl);
        Assertions.assertTrue(MetaStoreProviders.bind(typed("jdbc"), Collections.emptyMap())
                instanceof JdbcMetaStorePropertiesImpl);
        Assertions.assertTrue(MetaStoreProviders.bind(typed(null), Collections.emptyMap())
                instanceof FileSystemMetaStorePropertiesImpl);
    }

    @Test
    public void providersExposeTheirSensitiveKeys() {
        // The HMS provider surfaces its sensitive=true keytab keys (for masking when wired in P2-T03);
        // FileSystem has no sensitive fields -> empty (pins the default).
        Assertions.assertTrue(new HmsMetaStoreProvider().sensitivePropertyKeys()
                .contains("hive.metastore.client.keytab"));
        Assertions.assertTrue(new FileSystemMetaStoreProvider().sensitivePropertyKeys().isEmpty());
    }
}
