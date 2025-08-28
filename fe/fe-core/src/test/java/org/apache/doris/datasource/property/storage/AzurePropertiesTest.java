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

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.exception.StoragePropertiesException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AzurePropertiesTest {

    private Map<String, String> origProps;

    // Setup method to initialize the properties map before each test
    @BeforeEach
    public void setup() {
        origProps = new HashMap<>();
    }

    // Test for valid Azure configuration
    @Test
    public void testValidAzureConfiguration() throws UserException {
        origProps.put("s3.endpoint", "https://mystorageaccount.blob.core.windows.net");
        origProps.put("s3.access_key", "myAzureAccessKey");
        origProps.put("s3.secret_key", "myAzureSecretKey");
        origProps.put("provider", "azure");

        AzureProperties azureProperties = (AzureProperties) StorageProperties.createPrimary(origProps);

        // Verify if the properties are correctly parsed
        Assertions.assertEquals("https://mystorageaccount.blob.core.windows.net", azureProperties.getEndpoint());
        Assertions.assertEquals("myAzureAccessKey", azureProperties.getAccessKey());
        Assertions.assertEquals("myAzureSecretKey", azureProperties.getSecretKey());
        Assertions.assertEquals("false", azureProperties.getUsePathStyle());
        Assertions.assertEquals("false", azureProperties.getForceParsingByStandardUrl());
        Assertions.assertEquals("Azure", azureProperties.getStorageName());
    }

    // Test for missing access_key configuration, should throw an exception
    @Test
    public void testMissingAccessKey() {
        origProps.put("s3.endpoint", "https://mystorageaccount.blob.core.windows.net");
        origProps.put("provider", "azure");
        origProps.put("s3.secret_key", "myAzureSecretKey");

        // Expect an exception due to missing access_key
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                StorageProperties.createAll(origProps), "Property s3.access_key is required.");
        origProps.put("s3.access_key", "myAzureAccessKey");
        Assertions.assertDoesNotThrow(() -> StorageProperties.createAll(origProps));
    }

    // Test for missing provider configuration, should throw an exception
    @Test
    public void testMissingProvider() throws UserException {
        origProps.put("s3.endpoint", "https://mystorageaccount.blob.core.windows.net");
        origProps.put("s3.access_key", "myAzureAccessKey");
        origProps.put("s3.secret_key", "myAzureSecretKey");
        List<StorageProperties> storagePropertiesList = StorageProperties.createAll(origProps);
        Assertions.assertEquals(2, storagePropertiesList.size());
        Assertions.assertEquals(HdfsProperties.class, storagePropertiesList.get(1).getClass());
        Assertions.assertEquals(AzureProperties.class, storagePropertiesList.get(0).getClass());
        origProps.put("s3.endpoint", "https://mystorageaccount.net");
        // Expect an exception due to missing provider
        origProps.put("provider", "azure");
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                StorageProperties.createPrimary(origProps), "Endpoint 'https://mystorageaccount.net' is not valid. It should end with '.blob.core.windows.net'.");
    }

    // Test for empty configuration, should throw an exception
    @Test
    public void testEmptyConfiguration() {
        // Expect an exception due to empty configuration
        Assertions.assertThrows(RuntimeException.class, () ->
                StorageProperties.createPrimary(new HashMap<>()), "Empty configuration is not allowed.");


    }

    // Test for path style when use_path_style is false
    @Test
    public void testPathStyleCombinations() throws Exception {
        origProps.put("s3.endpoint", "https://mystorageaccount.blob.core.windows.net");
        origProps.put("s3.access_key", "a");
        origProps.put("s3.secret_key", "b");
        origProps.put("provider", "azure");

        // By default, use_path_style is false
        AzureProperties azureProperties = (AzureProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("s3://mystorageaccount/mycontainer/blob.txt",
                azureProperties.validateAndNormalizeUri("https://mystorageaccount.blob.core.windows.net/mycontainer/blob.txt"));

        // Set use_path_style to true
        origProps.put("use_path_style", "true");
        azureProperties = (AzureProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("s3://mycontainer/blob.txt",
                azureProperties.validateAndNormalizeUri("https://mystorageaccount.blob.core.windows.net/mycontainer/blob.txt"));
    }

    @Test
    public void testParsingUri() throws Exception {
        origProps.put("s3.endpoint", "https://mystorageaccount.blob.core.windows.net");
        origProps.put("s3.access_key", "a");
        origProps.put("s3.secret_key", "b");
        origProps.put("provider", "azure");
        origProps.put("use_path_style", "true");

        AzureProperties azureProperties = (AzureProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("s3://mycontainer/blob.txt",
                azureProperties.validateAndNormalizeUri("https://mystorageaccount.blob.core.windows.net/mycontainer/blob.txt"));
        Assertions.assertThrowsExactly(StoragePropertiesException.class, () ->
                azureProperties.validateAndGetUri(origProps),
                "props must contain uri");
        origProps.put("uri", "https://mystorageaccount.blob.core.windows.net/mycontainer/blob.txt");
        Assertions.assertEquals("https://mystorageaccount.blob.core.windows.net/mycontainer/blob.txt",
                azureProperties.validateAndGetUri(origProps));
        azureProperties.setUsePathStyle("false");
        Assertions.assertEquals("s3://mystorageaccount/mycontainer/blob.txt",
                azureProperties.validateAndNormalizeUri("https://mystorageaccount.blob.core.windows.net/mycontainer/blob.txt"));


    }

    // Test for backend configuration properties in Azure
    @Test
    public void testBackendConfigProperties() throws UserException {
        origProps.put("s3.endpoint", "https://mystorageaccount.blob.core.windows.net");
        origProps.put("s3.access_key", "myAzureAccessKey");
        origProps.put("s3.secret_key", "myAzureSecretKey");
        origProps.put("provider", "azure");

        AzureProperties azureProperties = (AzureProperties) StorageProperties.createPrimary(origProps);
        Map<String, String> nativeProps = azureProperties.getBackendConfigProperties();

        // Verify if backend properties are set correctly
        Assertions.assertEquals("https://mystorageaccount.blob.core.windows.net", nativeProps.get("AWS_ENDPOINT"));
        Assertions.assertEquals("dummy_region", nativeProps.get("AWS_REGION"));
        Assertions.assertEquals("myAzureAccessKey", nativeProps.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("myAzureSecretKey", nativeProps.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("true", nativeProps.get("AWS_NEED_OVERRIDE_ENDPOINT"));
        Assertions.assertEquals("azure", nativeProps.get("provider"));
        Assertions.assertEquals("false", nativeProps.get("use_path_style"));
    }

    // Test for force_parsing_by_standard_uri being false
    @Test
    public void testForceParsingByStandardUriFalse() {
        origProps.put("s3.endpoint", "https://s3.amazonaws.com");
        origProps.put("s3.access_key", "myAWSAccessKey");
        origProps.put("s3.secret_key", "myAWSSecretKey");
        origProps.put("provider", "azure");
        origProps.put("force_parsing_by_standard_uri", "false");

        // Expect an exception since force_parsing_by_standard_uri cannot be false for S3
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                StorageProperties.createPrimary(origProps), "force_parsing_by_standard_uri cannot be false for S3.");
    }

    // Test for empty path, should throw an exception
    @Test
    public void testEmptyPath() throws UserException {
        origProps.put("s3.endpoint", "https://mystorageaccount.blob.core.windows.net");
        origProps.put("s3.access_key", "myAzureAccessKey");
        origProps.put("s3.secret_key", "myAzureSecretKey");
        origProps.put("provider", "azure");

        AzureProperties azureProperties = (AzureProperties) StorageProperties.createPrimary(origProps);
        // Expect an exception when the path is empty
        Assertions.assertThrows(StoragePropertiesException.class, () ->
                azureProperties.validateAndNormalizeUri(""), "Path cannot be empty.");
    }
}
