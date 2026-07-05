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

package org.apache.doris.connector.metastore.paimon;

import org.apache.doris.connector.metastore.MetaStoreProperties;
import org.apache.doris.connector.metastore.paimon.dlf.PaimonDlfMetaStoreProperties;
import org.apache.doris.connector.metastore.paimon.fs.PaimonFileSystemMetaStoreProperties;
import org.apache.doris.connector.metastore.paimon.fs.PaimonFileSystemMetaStoreProvider;
import org.apache.doris.connector.metastore.paimon.hms.PaimonHmsMetaStoreProperties;
import org.apache.doris.connector.metastore.paimon.hms.PaimonHmsMetaStoreProvider;
import org.apache.doris.connector.metastore.paimon.jdbc.PaimonJdbcMetaStoreProperties;
import org.apache.doris.connector.metastore.paimon.rest.PaimonRestMetaStoreProperties;
import org.apache.doris.connector.metastore.spi.MetaStoreProviders;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Verifies the ServiceLoader-based discovery on the paimon classpath: all 5 paimon providers register, and
 * the first-hit dispatcher selects the right backend by {@code paimon.catalog.type} (no central switch, no
 * enum). After the metastore module split, the providers live in fe-connector-metastore-paimon, so this
 * dispatch test lives here too (the -spi test classpath has no providers).
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
                instanceof PaimonHmsMetaStoreProperties);
        Assertions.assertTrue(MetaStoreProviders.bind(typed("dlf"), Collections.emptyMap())
                instanceof PaimonDlfMetaStoreProperties);
        Assertions.assertTrue(MetaStoreProviders.bind(typed("rest"), Collections.emptyMap())
                instanceof PaimonRestMetaStoreProperties);
        Assertions.assertTrue(MetaStoreProviders.bind(typed("jdbc"), Collections.emptyMap())
                instanceof PaimonJdbcMetaStoreProperties);
        Assertions.assertTrue(MetaStoreProviders.bind(typed(null), Collections.emptyMap())
                instanceof PaimonFileSystemMetaStoreProperties);
    }

    @Test
    public void bindForTypeSelectsByExplicitFlavorNotByPaimonKey() {
        // WHY: iceberg carries "iceberg.catalog.type", NOT "paimon.catalog.type". The metastore-spi
        // dispatch cannot sniff the hardcoded paimon key for an iceberg catalog; the caller (which has
        // already resolved its flavor) passes it explicitly via bindForType. These props deliberately
        // OMIT paimon.catalog.type to prove selection is driven by the flavor ARG, not the key.
        // MUTATION: if bindForType read props.get("paimon.catalog.type") instead of the flavor arg, no
        // provider would match -> IllegalArgumentException -> red.
        Map<String, String> icebergProps = new HashMap<>();
        icebergProps.put("iceberg.catalog.type", "hms");
        icebergProps.put("hive.metastore.uris", "thrift://h:9083");
        MetaStoreProperties ms = MetaStoreProviders.bindForType("hms", icebergProps, Collections.emptyMap());
        Assertions.assertTrue(ms instanceof PaimonHmsMetaStoreProperties,
                "bindForType(\"hms\", ...) must select the HMS backend by the explicit flavor");
        // the real (iceberg) props reach bind(), not the flavor token:
        Assertions.assertEquals("thrift://h:9083", ms.rawProperties().get("hive.metastore.uris"));
    }

    @Test
    public void bindForTypeIsCaseInsensitive() {
        // WHY: a user who writes "HMS"/"Hms" in iceberg.catalog.type must still route to the HMS backend,
        // mirroring supports(Map)'s equalsIgnoreCase. MUTATION: a case-sensitive supportsType -> "HMS"
        // matches nothing -> throws -> red.
        Assertions.assertTrue(MetaStoreProviders.bindForType("HMS", typed("hms"), Collections.emptyMap())
                instanceof PaimonHmsMetaStoreProperties);
    }

    @Test
    public void bindForTypeUnknownFlavorThrows() {
        // WHY: an unrecognized flavor must fail loudly (same contract as bind(Map)), not silently fall
        // through to the filesystem default. MUTATION: routing an unknown flavor to FILESYSTEM -> no
        // throw -> red.
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> MetaStoreProviders.bindForType("nessie", new HashMap<>(), Collections.emptyMap()));
        Assertions.assertTrue(ex.getMessage().startsWith("No MetaStoreProvider supports"), ex.getMessage());
    }

    @Test
    public void providersExposeTheirSensitiveKeys() {
        // The HMS provider surfaces its sensitive=true keytab keys (for masking when wired in P2-T03);
        // FileSystem has no sensitive fields -> empty (pins the default).
        Assertions.assertTrue(new PaimonHmsMetaStoreProvider().sensitivePropertyKeys()
                .contains("hive.metastore.client.keytab"));
        Assertions.assertTrue(new PaimonFileSystemMetaStoreProvider().sensitivePropertyKeys().isEmpty());
    }
}
