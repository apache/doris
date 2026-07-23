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

package org.apache.doris.datasource.connectivity;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.datasource.property.storage.HdfsProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.foundation.property.ConnectorProperty;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Verifies that {@link CatalogSsrfChecker} discovers the right URIs from various property
 * shapes, driven by the {@code @ConnectorProperty(checkSsrf=true)} annotation plus the
 * HDFS dynamic-key special case, and hands each one to the validator in the expected form.
 */
public class CatalogSsrfCheckerTest {

    @Test
    public void testNullInputsDoNothing() throws Exception {
        RecordingValidator validator = new RecordingValidator();

        CatalogSsrfChecker.check("cat", null, null, validator);

        Assertions.assertTrue(validator.checkedUris.isEmpty());
        Assertions.assertTrue(validator.checkedJdbcUrls.isEmpty());
    }

    @Test
    public void testEmptyStorageMapDoesNothing() throws Exception {
        RecordingValidator validator = new RecordingValidator();

        CatalogSsrfChecker.check("cat", null, new HashMap<>(), validator);

        Assertions.assertTrue(validator.checkedUris.isEmpty());
        Assertions.assertTrue(validator.checkedJdbcUrls.isEmpty());
    }

    @Test
    public void testHmsThriftUriIsValidated() throws Exception {
        MetastoreProperties msProps = createHmsProperties("thrift://internal-host:9083");
        RecordingValidator validator = new RecordingValidator();

        CatalogSsrfChecker.check("cat", msProps, null, validator);

        Assertions.assertEquals(Arrays.asList("http://internal-host:9083"), validator.checkedUris);
    }

    @Test
    public void testLoopbackHostIsRejectedWithoutNetworkHook() throws Exception {
        MetastoreProperties msProps = createHmsProperties("thrift://127.0.0.1:9083");

        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> CatalogSsrfChecker.check("cat", msProps, null));

        Assertions.assertTrue(ex.getMessage().contains("127.0.0.1"),
                "message should name the rejected host, was: " + ex.getMessage());
    }

    @Test
    public void testCommaSeparatedHmsUrisValidatedIndependently() throws Exception {
        MetastoreProperties msProps = createHmsProperties("thrift://h1:9083,thrift://h2:9083");
        RecordingValidator validator = new RecordingValidator();

        CatalogSsrfChecker.check("cat", msProps, null, validator);

        Assertions.assertEquals(Arrays.asList("http://h1:9083", "http://h2:9083"), validator.checkedUris);
    }

    @Test
    public void testIcebergRestUriStripsSchemeAndPath() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "rest");
        props.put("iceberg.rest.uri", "https://internal-host:8181/v1/catalog");
        props.put("warehouse", "s3://w/path");
        MetastoreProperties msProps = MetastoreProperties.create(props);
        RecordingValidator validator = new RecordingValidator();

        CatalogSsrfChecker.check("cat", msProps, null, validator);

        Assertions.assertEquals(Arrays.asList("http://internal-host:8181"), validator.checkedUris);
    }

    @Test
    public void testIcebergRestOauth2ServerUriIsValidated() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "rest");
        props.put("iceberg.rest.uri", "https://iceberg-rest.example.com/v1/catalog");
        props.put("iceberg.rest.oauth2.credential", "client:secret");
        props.put("iceberg.rest.oauth2.server-uri", "https://oauth.example.com/token");
        props.put("warehouse", "s3://w/path");
        MetastoreProperties msProps = MetastoreProperties.create(props);
        RecordingValidator validator = new RecordingValidator();

        CatalogSsrfChecker.check("cat", msProps, null, validator);

        Assertions.assertEquals(Arrays.asList(
                "http://iceberg-rest.example.com", "http://oauth.example.com"), validator.checkedUris);
    }

    @Test
    public void testIcebergJdbcUriUsesJdbcChecker() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "jdbc");
        props.put("iceberg.jdbc.uri", "jdbc:mysql://jdbc-host:3306/iceberg");
        props.put("iceberg.jdbc.catalog_name", "iceberg");
        props.put("warehouse", "s3://w/path");
        MetastoreProperties msProps = MetastoreProperties.create(props);
        RecordingValidator validator = new RecordingValidator();

        CatalogSsrfChecker.check("cat", msProps, null, validator);

        Assertions.assertEquals(Arrays.asList("jdbc:mysql://jdbc-host:3306/iceberg"), validator.checkedJdbcUrls);
    }

    @Test
    public void testHdfsDefaultFsIsValidated() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("fs.defaultFS", "hdfs://nn-host:9000");
        HdfsProperties hdfs = new HdfsProperties(props);
        hdfs.initNormalizeAndCheckProps();

        Map<StorageProperties.Type, StorageProperties> storageMap = new HashMap<>();
        storageMap.put(StorageProperties.Type.HDFS, hdfs);
        RecordingValidator validator = new RecordingValidator();

        CatalogSsrfChecker.check("cat", null, storageMap, validator);

        Assertions.assertEquals(Arrays.asList("http://nn-host:9000"), validator.checkedUris);
    }

    @Test
    public void testHdfsHaNamenodeRpcAddressesAreValidated() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("fs.defaultFS", "hdfs://ns1");
        props.put("dfs.nameservices", "ns1");
        props.put("dfs.ha.namenodes.ns1", "nn1,nn2");
        props.put("dfs.namenode.rpc-address.ns1.nn1", "nn1-host:8020");
        props.put("dfs.namenode.rpc-address.ns1.nn2", "nn2-host:8020");
        props.put("dfs.client.failover.proxy.provider.ns1",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        HdfsProperties hdfs = new HdfsProperties(props);
        hdfs.initNormalizeAndCheckProps();

        Map<StorageProperties.Type, StorageProperties> storageMap = new HashMap<>();
        storageMap.put(StorageProperties.Type.HDFS, hdfs);
        RecordingValidator validator = new RecordingValidator();

        CatalogSsrfChecker.check("cat", null, storageMap, validator);

        Assertions.assertEquals(new HashSet<>(Arrays.asList("http://nn1-host:8020", "http://nn2-host:8020")),
                new HashSet<>(validator.checkedUris));
    }

    @Test
    public void testAzureOauthServerUriIsValidated() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "rest");
        props.put("provider", "azure");
        props.put("azure.auth_type", "OAuth2");
        props.put("azure.endpoint", "https://onelake.dfs.fabric.microsoft.com");
        props.put("azure.oauth2_client_id", "client-id");
        props.put("azure.oauth2_client_secret", "client-secret");
        props.put("azure.oauth2_server_uri", "https://login.microsoftonline.com/tenant/oauth2/token");
        props.put("azure.oauth2_account_host", "onelake.dfs.fabric.microsoft.com");
        StorageProperties azure = StorageProperties.createPrimary(props);

        Map<StorageProperties.Type, StorageProperties> storageMap = new HashMap<>();
        storageMap.put(StorageProperties.Type.AZURE, azure);
        RecordingValidator validator = new RecordingValidator();

        CatalogSsrfChecker.check("cat", null, storageMap, validator);

        Assertions.assertEquals(Arrays.asList(
                "http://onelake.dfs.fabric.microsoft.com", "http://login.microsoftonline.com"),
                validator.checkedUris);
    }

    @Test
    public void testImplicitHdfsStorageIsSkipped() throws Exception {
        // explicitlyConfigured=false means auto-created fallback; should be ignored to avoid
        // breaking catalogs whose user didn't actually configure HDFS.
        Map<String, String> props = new HashMap<>();
        props.put("fs.defaultFS", "hdfs://nn-host:9000");
        HdfsProperties hdfs = new HdfsProperties(props, false);
        hdfs.initNormalizeAndCheckProps();

        Map<StorageProperties.Type, StorageProperties> storageMap = new HashMap<>();
        storageMap.put(StorageProperties.Type.HDFS, hdfs);
        RecordingValidator validator = new RecordingValidator();

        CatalogSsrfChecker.check("cat", null, storageMap, validator);

        Assertions.assertTrue(validator.checkedUris.isEmpty());
    }

    @Test
    public void testSecurityCheckerExceptionPropagatesAsDdlException() throws Exception {
        MetastoreProperties msProps = createHmsProperties("thrift://forbidden-host:9083");
        RecordingValidator validator = new RecordingValidator();
        validator.uriFailureMessage = "URL points to private IP";

        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> CatalogSsrfChecker.check("cat", msProps, null, validator));

        Assertions.assertTrue(ex.getMessage().contains("SSRF check failed"),
                "message should explain SSRF failure, was: " + ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("forbidden-host"),
                "message should name the offending host, was: " + ex.getMessage());
    }

    @Test
    public void testEndpointLikeConnectorPropertiesOptIntoSsrfCheck() throws Exception {
        Set<String> allowedUncheckedFields = new HashSet<>(Arrays.asList(
                "org.apache.doris.datasource.property.storage.AzureProperties#accountHost",
                "org.apache.doris.datasource.property.storage.AzureProperties#forceParsingByStandardUrl",
                "org.apache.doris.datasource.property.metastore.IcebergJdbcMetaStoreProperties#driverUrl"
        ));
        for (Class<?> clazz : new Class<?>[] {
                org.apache.doris.datasource.property.metastore.AWSGlueMetaStoreBaseProperties.class,
                org.apache.doris.datasource.property.metastore.AliyunDLFBaseProperties.class,
                org.apache.doris.datasource.property.metastore.HMSBaseProperties.class,
                org.apache.doris.datasource.property.metastore.IcebergJdbcMetaStoreProperties.class,
                org.apache.doris.datasource.property.metastore.IcebergRestProperties.class,
                org.apache.doris.datasource.property.metastore.PaimonRestMetaStoreProperties.class,
                org.apache.doris.datasource.property.storage.AzureProperties.class,
                org.apache.doris.datasource.property.storage.COSProperties.class,
                org.apache.doris.datasource.property.storage.GCSProperties.class,
                org.apache.doris.datasource.property.storage.HdfsProperties.class,
                org.apache.doris.datasource.property.storage.MinioProperties.class,
                org.apache.doris.datasource.property.storage.OBSProperties.class,
                org.apache.doris.datasource.property.storage.OSSHdfsProperties.class,
                org.apache.doris.datasource.property.storage.OSSProperties.class,
                org.apache.doris.datasource.property.storage.OzoneProperties.class,
                org.apache.doris.datasource.property.storage.S3Properties.class
        }) {
            assertEndpointLikeFieldsAreAnnotated(clazz, allowedUncheckedFields);
        }
    }

    @Test
    public void testNonUriPropertiesAreNotValidated() throws Exception {
        // The validator should only see calls for the annotated URI fields. Region / credentials
        // / non-URL property fields must not be validated.
        MetastoreProperties msProps = createHmsProperties("thrift://h:9083");
        RecordingValidator validator = new RecordingValidator();

        CatalogSsrfChecker.check("cat", msProps, null, validator);

        // Exactly one URI from this catalog means exactly one validation call.
        Assertions.assertEquals(1, validator.checkedUris.size());
    }

    @Test
    public void testCheckUrisNullDoesNothing() throws Exception {
        RecordingValidator validator = new RecordingValidator();

        CatalogSsrfChecker.checkUris("cat", null, validator);

        Assertions.assertTrue(validator.checkedUris.isEmpty());
        Assertions.assertTrue(validator.checkedJdbcUrls.isEmpty());
    }

    @Test
    public void testCheckUrisValidatesRawEndpoints() throws Exception {
        // Raw endpoint values (used for MaxCompute / Doris catalog endpoints): a full URL
        // with a path, and a comma-separated list of bare host:port, are normalized and each
        // host handed to the validator independently.
        RecordingValidator validator = new RecordingValidator();

        CatalogSsrfChecker.checkUris("cat", Arrays.asList(
                "http://service.cn-beijing.maxcompute.aliyun-inc.com/api",
                "fe-host:8030,fe-host2:8030"), validator);

        Assertions.assertEquals(Arrays.asList(
                "http://service.cn-beijing.maxcompute.aliyun-inc.com",
                "http://fe-host:8030",
                "http://fe-host2:8030"), validator.checkedUris);
    }

    @Test
    public void testCheckUrisRejectsLoopbackEndpoint() {
        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> CatalogSsrfChecker.checkUris("cat", Arrays.asList("127.0.0.1:9020")));

        Assertions.assertTrue(ex.getMessage().contains("127.0.0.1"),
                "message should name the rejected host, was: " + ex.getMessage());
    }

    private MetastoreProperties createHmsProperties(String metastoreUri) throws UserException {
        Map<String, String> props = new HashMap<>();
        props.put("type", "hms");
        props.put("hive.metastore.uris", metastoreUri);
        return MetastoreProperties.create(props);
    }

    private void assertEndpointLikeFieldsAreAnnotated(Class<?> clazz, Set<String> allowedUncheckedFields) {
        Class<?> current = clazz;
        while (current != null && current != Object.class) {
            for (Field field : current.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                ConnectorProperty property = field.getAnnotation(ConnectorProperty.class);
                if (property == null) {
                    continue;
                }
                boolean endpointLike = isEndpointLikeName(field.getName())
                        || Arrays.stream(property.names()).anyMatch(this::isEndpointLikeName);
                if (endpointLike && !allowedUncheckedFields.contains(current.getName() + "#" + field.getName())) {
                    Assertions.assertTrue(property.checkSsrf(),
                            current.getName() + "#" + field.getName()
                                    + " looks like an outbound endpoint and must set checkSsrf=true");
                }
            }
            current = current.getSuperclass();
        }
    }

    private boolean isEndpointLikeName(String name) {
        String lowerName = name.toLowerCase();
        if (lowerName.equals("forceparsingbystandardurl")
                || lowerName.contains("force_parsing_by_standard_uri")) {
            return false;
        }
        return lowerName.equals("uri")
                || lowerName.endsWith("uri")
                || lowerName.contains(".uri")
                || lowerName.contains("_uri")
                || lowerName.contains("-uri")
                || lowerName.contains("endpoint");
    }

    private static class RecordingValidator implements CatalogSsrfChecker.UriValidator {
        private final List<String> checkedUris = new ArrayList<>();
        private final List<String> checkedJdbcUrls = new ArrayList<>();
        private String uriFailureMessage;

        @Override
        public void checkUri(String uri) throws Exception {
            if (uriFailureMessage != null) {
                throw new RuntimeException(uriFailureMessage);
            }
            checkedUris.add(uri);
        }

        @Override
        public void checkJdbcUrl(String jdbcUrl) {
            checkedJdbcUrls.add(jdbcUrl);
        }
    }
}
