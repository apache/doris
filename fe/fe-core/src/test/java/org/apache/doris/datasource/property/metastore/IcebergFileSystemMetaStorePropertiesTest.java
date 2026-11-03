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

import org.apache.doris.datasource.iceberg.fileio.GcpS3FileIOAwsClientFactory;
import org.apache.doris.datasource.property.storage.GCSProperties;
import org.apache.doris.datasource.property.storage.HdfsProperties;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergFileSystemMetaStorePropertiesTest {

    @Test
    public void testGcpNativeCredentialUsesS3FileIOClientFactory() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "hadoop");
        props.put("warehouse", "gs://catalog-bucket/warehouse");
        props.put("provider", "GCP");
        props.put("gs.endpoint", "https://storage.googleapis.com");
        props.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE, "COMPUTE_ENGINE");
        props.put(GCSProperties.IMPERSONATION_SERVICE_ACCOUNT,
                "target@my-project.iam.gserviceaccount.com");
        props.put("unrelated.catalog.secret", "must-not-reach-file-io");

        IcebergFileSystemMetaStoreProperties icebergProperties =
                (IcebergFileSystemMetaStoreProperties) MetastoreProperties.create(props);
        List<StorageProperties> storagePropertiesList = StorageProperties.createAll(props);
        Assertions.assertTrue(storagePropertiesList.stream().anyMatch(GCSProperties.class::isInstance));

        Map<String, String> fileIOProperties = new HashMap<>();
        icebergProperties.toFileIOProperties(storagePropertiesList, fileIOProperties, new Configuration());

        // S3FileIO provides Iceberg's seekable streams and create/overwrite implementation. The
        // custom factory only replaces client authentication with the native GCP bearer flow.
        Assertions.assertEquals(S3FileIO.class.getName(),
                fileIOProperties.get(CatalogProperties.FILE_IO_IMPL));
        Assertions.assertEquals(GcpS3FileIOAwsClientFactory.class.getName(),
                fileIOProperties.get(S3FileIOProperties.CLIENT_FACTORY));
        Assertions.assertEquals("GCP", fileIOProperties.get("provider"));
        Assertions.assertEquals("https://storage.googleapis.com", fileIOProperties.get(S3FileIOProperties.ENDPOINT));
        Assertions.assertEquals("us-east1", fileIOProperties.get(AwsClientProperties.CLIENT_REGION));
        Assertions.assertEquals("COMPUTE_ENGINE",
                fileIOProperties.get(GCSProperties.CREDENTIAL_PROVIDER_TYPE));
        Assertions.assertEquals("target@my-project.iam.gserviceaccount.com",
                fileIOProperties.get(GCSProperties.IMPERSONATION_SERVICE_ACCOUNT));
        Assertions.assertFalse(fileIOProperties.containsKey("unrelated.catalog.secret"));

        GcpS3FileIOAwsClientFactory clientFactory = new GcpS3FileIOAwsClientFactory();
        clientFactory.initialize(fileIOProperties);
        try (S3Client client = clientFactory.s3(); S3AsyncClient asyncClient = clientFactory.s3Async()) {
            Assertions.assertNotNull(client);
            Assertions.assertNotNull(asyncClient);
        }

        FileIO fileIO = new S3FileIO();
        fileIO.initialize(fileIOProperties);
        try {
            Assertions.assertEquals("gs://catalog-bucket/warehouse/metadata/v1.metadata.json",
                    fileIO.newInputFile("gs://catalog-bucket/warehouse/metadata/v1.metadata.json").location());
            Assertions.assertEquals("gs://catalog-bucket/warehouse/metadata/v2.metadata.json",
                    fileIO.newOutputFile("gs://catalog-bucket/warehouse/metadata/v2.metadata.json").location());
        } finally {
            fileIO.close();
        }
    }

    @Test
    public void testGcsAnonymousCredentialsReachIcebergS3Client() throws Exception {
        for (String key : new String[] {GCSProperties.CREDENTIAL_PROVIDER_TYPE,
                S3Properties.CREDENTIALS_PROVIDER_TYPE, S3Properties.Env.CREDENTIALS_PROVIDER_TYPE}) {
            Map<String, String> props = new HashMap<>();
            props.put("type", "iceberg");
            props.put("iceberg.catalog.type", "hadoop");
            props.put("warehouse", "gs://public-bucket/warehouse");
            props.put(key, " anonymous ");
            props.put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName());
            IcebergFileSystemMetaStoreProperties icebergProperties =
                    (IcebergFileSystemMetaStoreProperties) MetastoreProperties.create(props);
            Map<String, String> options = new HashMap<>(props);
            icebergProperties.toFileIOProperties(StorageProperties.createAll(props), options, new Configuration());

            // Exercise Iceberg's actual credential binding, not just the emitted property string.
            S3ClientBuilder builder = Mockito.mock(S3ClientBuilder.class);
            new S3FileIOProperties(options).applyCredentialConfigurations(new AwsClientProperties(options), builder);
            ArgumentCaptor<AwsCredentialsProvider> credentials = ArgumentCaptor.forClass(AwsCredentialsProvider.class);
            Mockito.verify(builder).credentialsProvider(credentials.capture());
            Assertions.assertInstanceOf(AnonymousCredentialsProvider.class, credentials.getValue(), key);
            Assertions.assertEquals(S3FileIO.class.getName(), options.get(CatalogProperties.FILE_IO_IMPL));
            Assertions.assertFalse(options.containsKey(S3FileIOProperties.CLIENT_FACTORY));
        }
    }

    @Test
    public void testGcsHmacDoesNotOverrideFileIO() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "hadoop");
        props.put("warehouse", "gs://catalog-bucket/warehouse");
        props.put("provider", "GCP");
        props.put("gs.access_key", "access-key");
        props.put("gs.secret_key", "secret-key");

        IcebergFileSystemMetaStoreProperties icebergProperties =
                (IcebergFileSystemMetaStoreProperties) MetastoreProperties.create(props);
        Map<String, String> fileIOProperties = new HashMap<>();
        icebergProperties.toFileIOProperties(StorageProperties.createAll(props), fileIOProperties,
                new Configuration());

        Assertions.assertFalse(fileIOProperties.containsKey(CatalogProperties.FILE_IO_IMPL));
        Assertions.assertFalse(fileIOProperties.containsKey(S3FileIOProperties.CLIENT_FACTORY));
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
