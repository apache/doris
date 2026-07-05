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

package org.apache.doris.connector.metastore.iceberg;

import org.apache.doris.connector.metastore.MetaStoreProperties;
import org.apache.doris.connector.metastore.iceberg.dlf.IcebergDlfMetaStoreProperties;
import org.apache.doris.connector.metastore.iceberg.glue.IcebergGlueMetaStoreProperties;
import org.apache.doris.connector.metastore.iceberg.hms.IcebergHmsMetaStoreProperties;
import org.apache.doris.connector.metastore.iceberg.jdbc.IcebergJdbcMetaStoreProperties;
import org.apache.doris.connector.metastore.iceberg.noop.IcebergNoOpMetaStoreProperties;
import org.apache.doris.connector.metastore.iceberg.rest.IcebergRestMetaStoreProperties;
import org.apache.doris.connector.metastore.spi.MetaStoreProviders;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Verifies ServiceLoader discovery on the iceberg classpath: all 7 iceberg providers register and
 * {@link MetaStoreProviders#bindForType} routes each {@code iceberg.catalog.type} flavor to its iceberg
 * backend (the caller resolves the flavor from {@code iceberg.catalog.type} and passes it explicitly, so
 * the metastore-spi never learns iceberg's key). Unknown / null flavors fail loudly — no iceberg provider
 * claims null (unlike the paimon FileSystem default), which lives on a different classpath.
 */
public class IcebergMetaStoreProvidersDispatchTest {

    private static MetaStoreProperties bind(String flavor) {
        return MetaStoreProviders.bindForType(flavor, new HashMap<>(), Collections.emptyMap());
    }

    @Test
    public void bindForTypeRoutesEachFlavorToItsIcebergBackend() {
        Assertions.assertTrue(bind("hms") instanceof IcebergHmsMetaStoreProperties);
        Assertions.assertTrue(bind("dlf") instanceof IcebergDlfMetaStoreProperties);
        Assertions.assertTrue(bind("rest") instanceof IcebergRestMetaStoreProperties);
        Assertions.assertTrue(bind("jdbc") instanceof IcebergJdbcMetaStoreProperties);
        Assertions.assertTrue(bind("glue") instanceof IcebergGlueMetaStoreProperties);
        Assertions.assertTrue(bind("hadoop") instanceof IcebergNoOpMetaStoreProperties);
        Assertions.assertTrue(bind("s3tables") instanceof IcebergNoOpMetaStoreProperties);
    }

    @Test
    public void bindForTypeIsCaseInsensitive() {
        // resolveFlavor lowercases, but the providers also accept mixed case (equalsIgnoreCase), matching
        // the paimon providers. MUTATION: a case-sensitive supportsType would make "HMS" route to nothing.
        Assertions.assertTrue(bind("HMS") instanceof IcebergHmsMetaStoreProperties);
        Assertions.assertTrue(bind("Rest") instanceof IcebergRestMetaStoreProperties);
    }

    @Test
    public void hadoopValidatesWarehouseAndS3TablesIsNoOp() {
        // s3tables has NO metastore-side CREATE-CATALOG rule, so its validate() is a genuine no-op. hadoop, in
        // contrast, restores the legacy IcebergHadoopExternalCatalog check (commit 935e4fb9d80): a HadoopCatalog
        // cannot initialize without a warehouse root, so validate() throws when the warehouse is absent and passes
        // once it is supplied. MUTATION: dropping the hadoop warehouse gate lets the missing-warehouse case pass
        // -> red; a bogus s3tables rule makes the no-op case throw -> red.
        bind("s3tables").validate();

        IllegalArgumentException missing = Assertions.assertThrows(IllegalArgumentException.class,
                () -> bind("hadoop").validate());
        Assertions.assertEquals(
                "Cannot initialize Iceberg HadoopCatalog because 'warehouse' must not be null or empty",
                missing.getMessage());

        Map<String, String> withWarehouse = new HashMap<>();
        withWarehouse.put("warehouse", "hdfs://ns/wh");
        MetaStoreProviders.bindForType("hadoop", withWarehouse, Collections.emptyMap()).validate();

        Assertions.assertEquals("HADOOP", bind("hadoop").providerName());
        Assertions.assertEquals("S3TABLES", bind("s3tables").providerName());
    }

    @Test
    public void unknownFlavorThrows() {
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> bind("nessie"));
        Assertions.assertTrue(ex.getMessage().startsWith("No MetaStoreProvider supports"), ex.getMessage());
    }

    @Test
    public void nullFlavorThrows() {
        // WHY: a missing iceberg.catalog.type resolves to null; no iceberg provider claims null (the
        // paimon FileSystem default-on-null lives on a different classpath), so bindForType fails loudly —
        // parity with the connector's "Missing 'iceberg.catalog.type'". MUTATION: an iceberg provider that
        // claimed null would silently default instead of rejecting.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> MetaStoreProviders.bindForType(null, new HashMap<>(), Collections.emptyMap()));
    }

    @Test
    public void allSevenProvidersRegistered() {
        // Per-engine scope: assert the iceberg flavor names are all present (HMS/DLF/REST/JDBC/GLUE share
        // the type token with paimon, but on the iceberg classpath only iceberg providers are loaded).
        Assertions.assertTrue(MetaStoreProviders.registeredNames().containsAll(
                java.util.Arrays.asList("HMS", "DLF", "REST", "JDBC", "GLUE", "HADOOP", "S3TABLES")),
                "registered=" + MetaStoreProviders.registeredNames());
    }

    @Test
    public void boundFlavorValidatesThroughDispatch() {
        // End-to-end: a glue catalog missing credentials, routed by bindForType, surfaces the §4 message.
        Map<String, String> props = new HashMap<>();
        props.put("glue.endpoint", "https://glue.us-east-1.amazonaws.com");
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> MetaStoreProviders.bindForType("glue", props, Collections.emptyMap()).validate());
        Assertions.assertEquals("At least one of glue.access_key or glue.role_arn must be set", ex.getMessage());
    }
}
