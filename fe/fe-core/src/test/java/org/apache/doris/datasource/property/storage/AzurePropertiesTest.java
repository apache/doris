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

package org.apache.doris.datasource.property.storage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class AzurePropertiesTest {
    private Map<String, String> origProps;

    @BeforeEach
    public void setUp() {
        origProps = new HashMap<>();
    }

    @Test
    public void testMissingRequiredFields() {
        origProps.put("s3.endpoint", "https://mystorageaccount.blob.core.windows.net");
        origProps.put("provider", "azure");

        origProps.put("s3.access_key", "myAzureAccessKey");

        Assertions.assertThrows(IllegalArgumentException.class, () ->
                StorageProperties.create(origProps), "Property s3.secret_key is required.");

        origProps.put("s3.secret_key", "myAzureSecretKey");

        // no exception expected
        StorageProperties.create(origProps);
    }

    @Test
    public void testToNativeConfiguration() {
        origProps.put("s3.endpoint", "https://azure.blob.core.windows.net");
        origProps.put("s3.access_key", "myAzureAccessKey");
        origProps.put("s3.secret_key", "myAzureSecretKey");
        origProps.put("use_path_style", "true");
        origProps.put("provider", "azure");

        AzureProperties azureProperties = (AzureProperties) StorageProperties.createStorageProperties(origProps);
        Map<String, String> nativeProps = azureProperties.getBackendConfigProperties();

        Assertions.assertEquals("https://azure.blob.core.windows.net", nativeProps.get("AWS_ENDPOINT"));
        Assertions.assertEquals("dummy_region", nativeProps.get("AWS_REGION"));
        Assertions.assertEquals("myAzureAccessKey", nativeProps.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("myAzureSecretKey", nativeProps.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("true", nativeProps.get("AWS_NEED_OVERRIDE_ENDPOINT"));
        Assertions.assertEquals("azure", nativeProps.get("provider"));
        Assertions.assertEquals("true", nativeProps.get("use_path_style"));

        // update use_path_style
        origProps.put("use_path_style", "false");
        azureProperties = (AzureProperties) StorageProperties.createStorageProperties(origProps);
        nativeProps = azureProperties.getBackendConfigProperties();
        Assertions.assertEquals("false", nativeProps.get("use_path_style"));
    }

    @Test
    public void testConvertUrlToFilePath() throws Exception {
        origProps.put("s3.endpoint", "https://mystorageaccount.blob.core.windows.net");
        origProps.put("s3.access_key", "a");
        origProps.put("s3.secret_key", "b");
        origProps.put("provider", "azure");

        AzureProperties azureProperties = (AzureProperties) StorageProperties.createStorageProperties(origProps);
        Assertions.assertEquals("s3://mystorageaccount/mycontainer/blob.txt",
                azureProperties.convertUrlToFilePath("https://mystorageaccount.blob.core.windows.net/mycontainer/blob.txt"));
        origProps.put("use_path_style", "true");
        azureProperties = (AzureProperties) StorageProperties.createStorageProperties(origProps);
        Assertions.assertEquals("s3://mycontainer/blob.txt",
                azureProperties.convertUrlToFilePath("https://mystorageaccount.blob.core.windows.net/mycontainer/blob.txt"));
    }

    @Test
    public void testCheckLoadPropsAndReturnUri() throws Exception {
        origProps.put("s3.endpoint", "https://azure.blob.core.windows.net");
        origProps.put("s3.access_key", "a");
        origProps.put("s3.secret_key", "b");
        origProps.put("provider", "azure");
        origProps.put(StorageProperties.FS_AZURE_SUPPORT, "true");

        AzureProperties azureProperties = (AzureProperties) StorageProperties.createStorageProperties(origProps);

        Map<String, String> loadProps = new HashMap<>();
        loadProps.put("uri", "azure://mycontainer/blob.txt");

        Assertions.assertEquals("azure://mycontainer/blob.txt", azureProperties.checkLoadPropsAndReturnUri(loadProps));
    }

    @Test
    public void testGetStorageName() {
        AzureProperties azureProperties = new AzureProperties(new HashMap<>(), "a", "b");
        Assertions.assertEquals("S3", azureProperties.getStorageName());
    }

}
