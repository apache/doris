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

package org.apache.doris.datasource.scan;

import org.apache.doris.filesystem.properties.AzureBackendPropertyKeys;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TAzureClientConfig;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class FileQueryScanNodeAzureConfigTest {

    @ParameterizedTest
    @ValueSource(strings = {"SharedKey", "SAS", "OAuth2"})
    void bindsAzureConfigForNativeSplitsAfterJniPropertiesAreSet(String authType) throws Exception {
        Map<String, String> properties = azureProperties(authType);
        FileQueryScanNode node = newNode(TFileFormatType.FORMAT_JNI);
        // System tables seed properties for JNI before a position-delete split selects native IO.
        node.params.setProperties(properties);

        bindObjectStorage(node, properties);

        Assertions.assertSame(properties, node.params.getProperties());
        Assertions.assertEquals(TFileFormatType.FORMAT_JNI, node.params.getFormatType());
        assertAzureConfig(node.params, authType);
        TAzureClientConfig config = node.params.getAzureClientConfig();

        // Further splits share the scan parameters and should retain the bound configuration.
        bindObjectStorage(node, properties);
        Assertions.assertSame(config, node.params.getAzureClientConfig());
    }

    @ParameterizedTest
    @ValueSource(strings = {"SharedKey", "SAS", "OAuth2"})
    void bindsAzureConfigForNativeScansWithoutProperties(String authType) throws Exception {
        Map<String, String> properties = azureProperties(authType);
        FileQueryScanNode node = newNode(TFileFormatType.FORMAT_PARQUET);

        bindObjectStorage(node, properties);

        Assertions.assertSame(properties, node.params.getProperties());
        assertAzureConfig(node.params, authType);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void preservesS3PropertiesWithoutBindingAzureConfig(boolean propertiesAlreadySet) throws Exception {
        Map<String, String> properties = Map.of(
                "AWS_ENDPOINT", "s3.us-east-1.amazonaws.com",
                "AWS_ACCESS_KEY", "access-key",
                "AWS_SECRET_KEY", "secret-key");
        FileQueryScanNode node = newNode(TFileFormatType.FORMAT_PARQUET);
        if (propertiesAlreadySet) {
            node.params.setProperties(properties);
        }

        bindObjectStorage(node, properties);

        Assertions.assertSame(properties, node.params.getProperties());
        Assertions.assertFalse(node.params.isSetAzureClientConfig());
        Assertions.assertEquals(TFileType.FILE_S3, node.params.getFileType());
    }

    private static FileQueryScanNode newNode(TFileFormatType format) {
        FileQueryScanNode node = Mockito.mock(FileQueryScanNode.class, Mockito.CALLS_REAL_METHODS);
        node.params = new TFileScanRangeParams();
        node.params.setFormatType(format);
        return node;
    }

    private static void bindObjectStorage(FileQueryScanNode node, Map<String, String> properties)
            throws Exception {
        Method method = FileQueryScanNode.class.getDeclaredMethod("setLocationPropertiesIfNecessary",
                Backend.class, TFileType.class, Map.class);
        method.setAccessible(true);
        method.invoke(node, null, TFileType.FILE_S3, properties);
    }

    private static Map<String, String> azureProperties(String authType) {
        Map<String, String> properties = new HashMap<>();
        properties.put(AzureBackendPropertyKeys.AUTH_TYPE, authType);
        properties.put(AzureBackendPropertyKeys.ENDPOINT, "https://acct.blob.core.windows.net");
        properties.put(AzureBackendPropertyKeys.ACCOUNT_NAME, "acct");
        properties.put(AzureBackendPropertyKeys.CONTAINER, "container");
        switch (authType) {
            case "SharedKey":
                properties.put(AzureBackendPropertyKeys.ACCOUNT_KEY, "account-key");
                break;
            case "SAS":
                properties.put(AzureBackendPropertyKeys.SAS_TOKEN, "sv=2024&sig=test");
                break;
            case "OAuth2":
                properties.put(AzureBackendPropertyKeys.TENANT_ID, "tenant");
                properties.put(AzureBackendPropertyKeys.CLIENT_ID, "client");
                properties.put(AzureBackendPropertyKeys.CLIENT_SECRET, "client-secret");
                break;
            default:
                throw new IllegalArgumentException(authType);
        }
        return properties;
    }

    private static void assertAzureConfig(TFileScanRangeParams params, String authType) {
        Assertions.assertTrue(params.isSetAzureClientConfig());
        Assertions.assertEquals(TFileType.FILE_S3, params.getFileType());
        TAzureClientConfig config = params.getAzureClientConfig();
        Assertions.assertEquals("https://acct.blob.core.windows.net", config.getEndpoint());
        Assertions.assertEquals("acct", config.getAccountName());
        Assertions.assertEquals("container", config.getContainer());
        switch (authType) {
            case "SharedKey":
                Assertions.assertEquals("account-key", config.getCredential().getSharedKey().getAccountKey());
                break;
            case "SAS":
                Assertions.assertEquals("sv=2024&sig=test", config.getCredential().getSas().getToken());
                break;
            case "OAuth2":
                Assertions.assertEquals("tenant", config.getCredential().getClientSecret().getTenantId());
                Assertions.assertEquals("client", config.getCredential().getClientSecret().getClientId());
                Assertions.assertEquals("client-secret", config.getCredential().getClientSecret().getClientSecret());
                break;
            default:
                throw new IllegalArgumentException(authType);
        }
    }
}
