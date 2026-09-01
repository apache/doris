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

import org.apache.doris.datasource.iceberg.fileio.GcsReadOnlyFileSystem;
import org.apache.doris.datasource.property.storage.GCSProperties;
import org.apache.doris.datasource.property.storage.HdfsProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergFileSystemMetaStorePropertiesTest {

    @Test
    public void testGcsBucketWithUnderscoreUsesAuthorityCompatibleFileSystem() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(GCSProperties.FS_GCS_SUPPORT, "true");
        props.put("gs.endpoint", "https://storage.googleapis.com");
        props.put("gs.access_key", "test-access-key");
        props.put("gs.secret_key", "test-secret-key");
        props.put("warehouse", "s3://bucket_with_underscore/iceberg");

        StorageProperties storageProperties = StorageProperties.createPrimary(props);
        Configuration configuration = new Configuration(false);
        configuration.addResource(storageProperties.getHadoopStorageConfig());
        Map<String, String> catalogProps = new HashMap<>(props);

        Assertions.assertTrue(IcebergFileSystemMetaStoreProperties.configureGcsWarehouseWithUnderscore(
                Collections.singletonList(storageProperties), catalogProps, configuration));
        Assertions.assertEquals(GcsReadOnlyFileSystem.class.getName(), configuration.get("fs.s3.impl"));
        Assertions.assertTrue(configuration.getBoolean("fs.s3.impl.disable.cache", false));
        Assertions.assertTrue(configuration.getBoolean("fs.s3a.path.style.access", false));
        Assertions.assertEquals("true", catalogProps.get(S3FileIOProperties.PATH_STYLE_ACCESS));
        Assertions.assertEquals(S3FileIO.class.getName(), catalogProps.get(CatalogProperties.FILE_IO_IMPL));

        try (FileSystem fileSystem = FileSystem.get(new URI(props.get("warehouse")), configuration)) {
            Assertions.assertInstanceOf(GcsReadOnlyFileSystem.class, fileSystem);
            Assertions.assertEquals("bucket_with_underscore", fileSystem.getUri().getAuthority());
            Assertions.assertThrows(UnsupportedOperationException.class,
                    () -> fileSystem.delete(new org.apache.hadoop.fs.Path(props.get("warehouse")), true));
        }
    }

    @Test
    public void testValidGcsBucketKeepsS3AFileSystem() {
        Map<String, String> props = new HashMap<>();
        props.put(GCSProperties.FS_GCS_SUPPORT, "true");
        props.put("gs.endpoint", "https://storage.googleapis.com");
        props.put("warehouse", "s3://valid-gcs-bucket/iceberg");

        StorageProperties storageProperties = StorageProperties.createPrimary(props);
        Configuration configuration = new Configuration(false);
        configuration.addResource(storageProperties.getHadoopStorageConfig());

        Assertions.assertFalse(IcebergFileSystemMetaStoreProperties.configureGcsWarehouseWithUnderscore(
                Collections.singletonList(storageProperties), new HashMap<>(props), configuration));
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", configuration.get("fs.s3.impl"));
    }

    @Test
    public void testNonGcsStorageWithUnderscoreKeepsOriginalFileSystem() {
        Map<String, String> props = new HashMap<>();
        props.put(StorageProperties.FS_HDFS_SUPPORT, "true");
        props.put("fs.defaultFS", "hdfs://namenode:8020");
        props.put("warehouse", "s3://bucket_with_underscore/iceberg");

        StorageProperties storageProperties = StorageProperties.createPrimary(props);
        Configuration configuration = new Configuration(false);
        configuration.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

        Assertions.assertFalse(IcebergFileSystemMetaStoreProperties.configureGcsWarehouseWithUnderscore(
                Collections.singletonList(storageProperties), new HashMap<>(props), configuration));
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", configuration.get("fs.s3.impl"));
    }

    @Test
    public void testNonEffectiveGcsStorageDoesNotInstallAdapter() {
        Map<String, String> ossProps = new HashMap<>();
        ossProps.put(StorageProperties.FS_OSS_SUPPORT, "true");
        ossProps.put("oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        StorageProperties ossStorage = StorageProperties.createPrimary(ossProps);

        Map<String, String> gcsProps = new HashMap<>();
        gcsProps.put(StorageProperties.FS_GCS_SUPPORT, "true");
        gcsProps.put("gs.endpoint", "https://storage.googleapis.com");
        StorageProperties gcsStorage = StorageProperties.createPrimary(gcsProps);

        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("warehouse", "s3://bucket_with_underscore/iceberg");
        Configuration configuration = new Configuration(false);
        configuration.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

        Assertions.assertFalse(IcebergFileSystemMetaStoreProperties.configureGcsWarehouseWithUnderscore(
                Arrays.asList(ossStorage, gcsStorage), catalogProps, configuration));
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", configuration.get("fs.s3.impl"));
    }

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

}
