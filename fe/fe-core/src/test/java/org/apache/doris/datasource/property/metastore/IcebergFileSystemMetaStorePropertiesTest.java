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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.property.storage.HdfsProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.kerberos.HadoopExecutionAuthenticator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergFileSystemMetaStorePropertiesTest {

    @Test
    public void testKerberosCatalog() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(HdfsProperties.FS_HDFS_SUPPORT, "true");
        props.put("fs.defaultFS", "hdfs://mycluster_test");
        props.put("hadoop.security.authentication", "kerberos");
        props.put("hadoop.kerberos.principal", "myprincipal");
        props.put("hadoop.kerberos.keytab", "mykeytab");
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "hadoop");
        props.put("warehouse", "hdfs://mycluster_test/ice");
        IcebergFileSystemMetaStoreProperties icebergProps = (IcebergFileSystemMetaStoreProperties) MetastoreProperties.create(props);
        List<StorageProperties> storagePropertiesList = Collections.singletonList(StorageProperties.createPrimary(props));
        //We expect a Kerberos-related exception, but because the messages vary by environment, we’re only doing a simple check.
        Assertions.assertThrows(RuntimeException.class, () -> icebergProps.initializeCatalog("iceberg", storagePropertiesList));
    }

    @Test
    public void testNonKerberosCatalog() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(HdfsProperties.FS_HDFS_SUPPORT, "true");
        props.put("fs.defaultFS", "file:///tmp");
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "hadoop");
        props.put("warehouse", "file:///tmp");
        IcebergFileSystemMetaStoreProperties icebergProps = (IcebergFileSystemMetaStoreProperties) MetastoreProperties.create(props);
        Assertions.assertEquals("hadoop", icebergProps.getIcebergCatalogType());
        List<StorageProperties> storagePropertiesList = Collections.singletonList(StorageProperties.createPrimary(props));
        Assertions.assertDoesNotThrow(() -> icebergProps.initializeCatalog("iceberg", storagePropertiesList));
        props.put("fs.defaultFS", "hdfs://mycluster" + System.currentTimeMillis());
        props.put("warehouse", "hdfs://mycluster" + System.currentTimeMillis());
        IcebergFileSystemMetaStoreProperties icebergPropsFailed = (IcebergFileSystemMetaStoreProperties) MetastoreProperties.create(props);
        RuntimeException e = Assertions.assertThrows(RuntimeException.class, () -> icebergPropsFailed.initializeCatalog("iceberg", storagePropertiesList));
        Assertions.assertTrue(e.getMessage().contains("UnknownHostException:"));
    }

    @Test
    public void testInitExecutionAuthenticatorWiresHdfsAuthenticatorWithoutInitializeCatalog() throws Exception {
        // H-3: post-flip the connector builds its own catalog, so the legacy initCatalog path (which set the
        // Kerberos authenticator) is dead. PluginDrivenExternalCatalog.initPreExecutionAuthenticator instead
        // invokes initExecutionAuthenticator and reads getExecutionAuthenticator(); without the override the
        // authenticator stays the base no-op and doAs is silently lost over Kerberized HDFS.
        Map<String, String> props = new HashMap<>();
        props.put(HdfsProperties.FS_HDFS_SUPPORT, "true");
        props.put("fs.defaultFS", "hdfs://mycluster_test");
        props.put("hadoop.security.authentication", "kerberos");
        props.put("hadoop.kerberos.principal", "myprincipal");
        props.put("hadoop.kerberos.keytab", "mykeytab");
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "hadoop");
        props.put("warehouse", "hdfs://mycluster_test/ice");
        IcebergFileSystemMetaStoreProperties icebergProps =
                (IcebergFileSystemMetaStoreProperties) MetastoreProperties.create(props);
        // Before wiring: base no-op (the Kerberos authenticator was only set inside the now-dead initCatalog).
        Assertions.assertNotEquals(HadoopExecutionAuthenticator.class,
                icebergProps.getExecutionAuthenticator().getClass());
        // The fix wires it from the storage props, WITHOUT the legacy initializeCatalog path (no login here —
        // only the authenticator object is built; doAs is lazy). MUTATION: drop the override -> stays no-op -> red.
        icebergProps.initExecutionAuthenticator(
                Collections.singletonList(StorageProperties.createPrimary(props)));
        Assertions.assertEquals(HadoopExecutionAuthenticator.class,
                icebergProps.getExecutionAuthenticator().getClass());
    }

    @Test
    public void testInitExecutionAuthenticatorNoOpForNonKerberosHdfs() throws Exception {
        // Legacy iceberg parity: only Kerberized HDFS gets a real authenticator; plain HDFS keeps the base
        // no-op (simple auth needs no doAs). Reverse-mutation guard for the isKerberos() check.
        Map<String, String> props = new HashMap<>();
        props.put(HdfsProperties.FS_HDFS_SUPPORT, "true");
        props.put("fs.defaultFS", "file:///tmp");
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "hadoop");
        props.put("warehouse", "file:///tmp");
        IcebergFileSystemMetaStoreProperties icebergProps =
                (IcebergFileSystemMetaStoreProperties) MetastoreProperties.create(props);
        icebergProps.initExecutionAuthenticator(
                Collections.singletonList(StorageProperties.createPrimary(props)));
        Assertions.assertNotEquals(HadoopExecutionAuthenticator.class,
                icebergProps.getExecutionAuthenticator().getClass());
    }

    // ---- M-1: warehouse=hdfs://<ns> -> fs.defaultFS=hdfs://<ns> derivation (axis 2) ----

    private static IcebergFileSystemMetaStoreProperties hadoopProps(String warehouse) throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "hadoop");
        if (warehouse != null) {
            props.put("warehouse", warehouse);
        }
        return (IcebergFileSystemMetaStoreProperties) MetastoreProperties.create(props);
    }

    @Test
    public void testDerivedFsDefaultFsFromHdfsNameserviceWarehouse() throws Exception {
        // HA-nameservice warehouse with no inline fs.defaultFS: the nameservice is bridged to fs.defaultFS so
        // HdfsProperties.guessIsMe (which never reads warehouse) binds HDFS. Mirrors the legacy constructor.
        // MUTATION: flip the override to return emptyMap -> red.
        Assertions.assertEquals(Collections.singletonMap("fs.defaultFS", "hdfs://myns"),
                hadoopProps("hdfs://myns/warehouse").getDerivedStorageProperties());
    }

    @Test
    public void testDerivedFsDefaultFsFromSingleNnAuthorityWarehouse() throws Exception {
        // Single-NN authority (host:port) parity: substringBetween keeps the whole authority.
        Assertions.assertEquals(Collections.singletonMap("fs.defaultFS", "hdfs://nn-host:8020"),
                hadoopProps("hdfs://nn-host:8020/warehouse").getDerivedStorageProperties());
    }

    @Test
    public void testNoDerivationForNonHdfsWarehouse() throws Exception {
        // file:// and s3:// warehouses derive nothing: the bridge is hdfs-only (legacy startsWith "hdfs:").
        Assertions.assertTrue(hadoopProps("file:///tmp/wh").getDerivedStorageProperties().isEmpty());
        Assertions.assertTrue(hadoopProps("s3://bucket/wh").getDerivedStorageProperties().isEmpty());
    }

    @Test
    public void testNoDerivationForBlankWarehouse() throws Exception {
        // No warehouse configured: fe-core derivation yields nothing (the connector CREATE gate is what
        // rejects a hadoop catalog without a warehouse).
        Assertions.assertTrue(hadoopProps(null).getDerivedStorageProperties().isEmpty());
    }

    @Test
    public void testMalformedHdfsWarehouseEmptyNameserviceThrows() throws Exception {
        // hdfs:///wh -> empty nameservice -> verbatim legacy fail-loud message. MUTATION: remove the throw -> red.
        IcebergFileSystemMetaStoreProperties props = hadoopProps("hdfs:///warehouse");
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                props::getDerivedStorageProperties);
        Assertions.assertEquals("Unrecognized 'warehouse' location format because name service is required.",
                e.getMessage());
    }

    @Test
    public void testWarehouseHdfsBridgedToBackendDefaultFsThroughCatalogProperty() {
        // End-to-end: a hadoop catalog with ONLY warehouse (no inline fs.defaultFS) binds HDFS storage with
        // the warehouse nameservice shipped to BE -> proves the CatalogProperty merge wiring. MUTATION: skip
        // the merge in CatalogProperty.initStorageProperties -> fs.defaultFS absent -> red.
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "hadoop");
        props.put(HdfsProperties.FS_HDFS_SUPPORT, "true");
        props.put("warehouse", "hdfs://nsbridge/wh");
        Map<String, String> backend = new CatalogProperty(null, props).getBackendStorageProperties();
        Assertions.assertEquals("hdfs://nsbridge", backend.get("fs.defaultFS"));
    }

    @Test
    public void testExplicitDefaultFsWinsOverDerivedThroughCatalogProperty() {
        // An explicit user fs.defaultFS is a hard key: the derived default must NOT overwrite it (putIfAbsent).
        // MUTATION: change putIfAbsent -> put -> derived "hdfs://nsbridge" overwrites -> red.
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "hadoop");
        props.put(HdfsProperties.FS_HDFS_SUPPORT, "true");
        props.put("warehouse", "hdfs://nsbridge/wh");
        props.put("fs.defaultFS", "hdfs://explicit-ns");
        Map<String, String> backend = new CatalogProperty(null, props).getBackendStorageProperties();
        Assertions.assertEquals("hdfs://explicit-ns", backend.get("fs.defaultFS"));
    }

}
