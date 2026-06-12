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

import org.apache.doris.cloud.security.SecurityChecker;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.datasource.property.storage.HdfsProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

/**
 * Verifies that {@link CatalogSsrfChecker} discovers the right URIs from various property
 * shapes — driven by the {@code @ConnectorProperty(checkSsrf=true)} annotation plus the
 * HDFS dynamic-key special case — and hands each one to {@link SecurityChecker} in the
 * expected normalized form.
 *
 * <p>The real {@link SecurityChecker} is a singleton wrapping Aliyun's SecurityUtil and a
 * no-op {@code DummySecurityChecker} in dev builds. Tests cannot assert anything against
 * that no-op, so we replace the singleton with a Mockito mock per test and verify call
 * arguments / count.
 */
public class CatalogSsrfCheckerTest {

    private SecurityChecker mockChecker;
    private MockedStatic<SecurityChecker> mockedStatic;

    @BeforeEach
    public void setUp() {
        mockChecker = Mockito.mock(SecurityChecker.class);
        mockedStatic = Mockito.mockStatic(SecurityChecker.class);
        mockedStatic.when(SecurityChecker::getInstance).thenReturn(mockChecker);
    }

    @AfterEach
    public void tearDown() {
        if (mockedStatic != null) {
            mockedStatic.close();
        }
    }

    @Test
    public void testNullInputsDoNothing() throws Exception {
        CatalogSsrfChecker.check("cat", null, null);
        Mockito.verifyNoInteractions(mockChecker);
    }

    @Test
    public void testEmptyStorageMapDoesNothing() throws Exception {
        CatalogSsrfChecker.check("cat", null, new HashMap<>());
        Mockito.verifyNoInteractions(mockChecker);
    }

    @Test
    public void testHmsThriftUriIsValidated() throws Exception {
        MetastoreProperties msProps = createHmsProperties("thrift://internal-host:9083");

        CatalogSsrfChecker.check("cat", msProps, null);

        Mockito.verify(mockChecker).startSSRFChecking("http://internal-host:9083");
        Mockito.verify(mockChecker, Mockito.atLeastOnce()).stopSSRFChecking();
    }

    @Test
    public void testCommaSeparatedHmsUrisValidatedIndependently() throws Exception {
        MetastoreProperties msProps = createHmsProperties("thrift://h1:9083,thrift://h2:9083");

        CatalogSsrfChecker.check("cat", msProps, null);

        Mockito.verify(mockChecker).startSSRFChecking("http://h1:9083");
        Mockito.verify(mockChecker).startSSRFChecking("http://h2:9083");
    }

    @Test
    public void testIcebergRestUriStripsSchemeAndPath() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        props.put("iceberg.catalog.type", "rest");
        props.put("iceberg.rest.uri", "https://internal-host:8181/v1/catalog");
        props.put("warehouse", "s3://w/path");
        MetastoreProperties msProps = MetastoreProperties.create(props);

        CatalogSsrfChecker.check("cat", msProps, null);

        Mockito.verify(mockChecker).startSSRFChecking("http://internal-host:8181");
    }

    @Test
    public void testHdfsDefaultFsIsValidated() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("fs.defaultFS", "hdfs://nn-host:9000");
        HdfsProperties hdfs = new HdfsProperties(props);
        hdfs.initNormalizeAndCheckProps();

        Map<StorageProperties.Type, StorageProperties> storageMap = new HashMap<>();
        storageMap.put(StorageProperties.Type.HDFS, hdfs);
        CatalogSsrfChecker.check("cat", null, storageMap);

        Mockito.verify(mockChecker).startSSRFChecking("http://nn-host:9000");
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
        CatalogSsrfChecker.check("cat", null, storageMap);

        Mockito.verify(mockChecker).startSSRFChecking("http://nn1-host:8020");
        Mockito.verify(mockChecker).startSSRFChecking("http://nn2-host:8020");
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
        CatalogSsrfChecker.check("cat", null, storageMap);

        Mockito.verify(mockChecker).startSSRFChecking("http://login.microsoftonline.com");
    }

    @Test
    public void testImplicitHdfsStorageIsSkipped() throws Exception {
        // explicitlyConfigured=false → auto-created fallback; should be ignored to avoid
        // breaking catalogs whose user didn't actually configure HDFS.
        Map<String, String> props = new HashMap<>();
        props.put("fs.defaultFS", "hdfs://nn-host:9000");
        HdfsProperties hdfs = new HdfsProperties(props, false);
        hdfs.initNormalizeAndCheckProps();

        Map<StorageProperties.Type, StorageProperties> storageMap = new HashMap<>();
        storageMap.put(StorageProperties.Type.HDFS, hdfs);
        CatalogSsrfChecker.check("cat", null, storageMap);

        Mockito.verify(mockChecker, Mockito.never()).startSSRFChecking(Mockito.anyString());
    }

    @Test
    public void testSecurityCheckerExceptionPropagatesAsDdlException() throws Exception {
        MetastoreProperties msProps = createHmsProperties("thrift://forbidden-host:9083");
        Mockito.doThrow(new RuntimeException("URL points to private IP"))
                .when(mockChecker).startSSRFChecking(Mockito.anyString());

        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> CatalogSsrfChecker.check("cat", msProps, null));

        Assertions.assertTrue(ex.getMessage().contains("SSRF check failed"),
                "message should explain SSRF failure, was: " + ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("forbidden-host"),
                "message should name the offending host, was: " + ex.getMessage());
        // stopSSRFChecking must still run after the failure to clean up thread state.
        Mockito.verify(mockChecker).stopSSRFChecking();
    }

    @Test
    public void testNonUriPropertiesAreNotValidated() throws Exception {
        // The mock should only see calls for the annotated URI fields. Region / credentials
        // / non-URL property fields must NOT be passed to SecurityChecker.
        MetastoreProperties msProps = createHmsProperties("thrift://h:9083");

        CatalogSsrfChecker.check("cat", msProps, null);

        // Exactly one URI from this catalog → exactly one startSSRFChecking call.
        Mockito.verify(mockChecker, Mockito.times(1)).startSSRFChecking(Mockito.anyString());
    }

    private MetastoreProperties createHmsProperties(String metastoreUri) throws UserException {
        Map<String, String> props = new HashMap<>();
        props.put("type", "hms");
        props.put("hive.metastore.uris", metastoreUri);
        return MetastoreProperties.create(props);
    }
}
