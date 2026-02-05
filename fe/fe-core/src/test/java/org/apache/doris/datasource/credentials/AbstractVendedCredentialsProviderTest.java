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

package org.apache.doris.datasource.credentials;

import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.datasource.property.storage.StorageProperties.Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class AbstractVendedCredentialsProviderTest {

    /**
     * Test implementation of AbstractVendedCredentialsProvider for testing purposes
     */
    private static class TestVendedCredentialsProvider extends AbstractVendedCredentialsProvider {
        private boolean isVendedCredentialsEnabledResult = true;
        private Map<String, String> rawVendedCredentialsResult = new HashMap<>();
        private String tableNameResult = "test_table";

        public void setVendedCredentialsEnabledResult(boolean result) {
            this.isVendedCredentialsEnabledResult = result;
        }

        public void setRawVendedCredentialsResult(Map<String, String> result) {
            this.rawVendedCredentialsResult = result;
        }

        public void setTableNameResult(String result) {
            this.tableNameResult = result;
        }

        @Override
        protected boolean isVendedCredentialsEnabled(MetastoreProperties metastoreProperties) {
            return isVendedCredentialsEnabledResult;
        }

        @Override
        protected <T> Map<String, String> extractRawVendedCredentials(T tableObject) {
            return rawVendedCredentialsResult;
        }

        @Override
        protected <T> String getTableName(T tableObject) {
            return tableNameResult;
        }
    }

    @Test
    public void testGetStoragePropertiesMapWithVendedCredentialsSuccess() {
        TestVendedCredentialsProvider provider = new TestVendedCredentialsProvider();

        // Setup test data
        Map<String, String> rawCredentials = new HashMap<>();
        rawCredentials.put("s3.access-key-id", "testAccessKey");
        rawCredentials.put("s3.secret-access-key", "testSecretKey");
        rawCredentials.put("s3.region", "us-west-2");

        provider.setVendedCredentialsEnabledResult(true);
        provider.setRawVendedCredentialsResult(rawCredentials);

        MetastoreProperties metastoreProperties = Mockito.mock(MetastoreProperties.class);
        Object tableObject = new Object();

        Map<Type, StorageProperties> result = provider.getStoragePropertiesMapWithVendedCredentials(
                metastoreProperties, tableObject);

        // Note: The actual result depends on StorageProperties.createAll() implementation
        // At minimum, it should not be null and should attempt to process the credentials
        Assertions.assertNotNull(result);
    }

    @Test
    public void testGetStoragePropertiesMapWithVendedCredentialsDisabled() {
        TestVendedCredentialsProvider provider = new TestVendedCredentialsProvider();
        provider.setVendedCredentialsEnabledResult(false);

        MetastoreProperties metastoreProperties = Mockito.mock(MetastoreProperties.class);
        Object tableObject = new Object();

        Map<Type, StorageProperties> result = provider.getStoragePropertiesMapWithVendedCredentials(
                metastoreProperties, tableObject);

        Assertions.assertNull(result);
    }

    @Test
    public void testGetStoragePropertiesMapWithNullTableObject() {
        TestVendedCredentialsProvider provider = new TestVendedCredentialsProvider();
        provider.setVendedCredentialsEnabledResult(true);

        MetastoreProperties metastoreProperties = Mockito.mock(MetastoreProperties.class);

        Map<Type, StorageProperties> result = provider.getStoragePropertiesMapWithVendedCredentials(
                metastoreProperties, null);

        Assertions.assertNull(result);
    }

    @Test
    public void testGetStoragePropertiesMapWithEmptyRawCredentials() {
        TestVendedCredentialsProvider provider = new TestVendedCredentialsProvider();
        provider.setVendedCredentialsEnabledResult(true);
        provider.setRawVendedCredentialsResult(new HashMap<>()); // Empty map

        MetastoreProperties metastoreProperties = Mockito.mock(MetastoreProperties.class);
        Object tableObject = new Object();

        Map<Type, StorageProperties> result = provider.getStoragePropertiesMapWithVendedCredentials(
                metastoreProperties, tableObject);

        Assertions.assertNull(result);
    }

    @Test
    public void testGetStoragePropertiesMapWithFilteredCredentials() {
        TestVendedCredentialsProvider provider = new TestVendedCredentialsProvider();

        // Setup credentials with mixed properties (some will be filtered out)
        Map<String, String> rawCredentials = new HashMap<>();
        rawCredentials.put("s3.access-key-id", "testAccessKey");
        rawCredentials.put("s3.secret-access-key", "testSecretKey");
        rawCredentials.put("table.name", "test_table"); // Should be filtered out
        rawCredentials.put("other.property", "other_value"); // Should be filtered out

        provider.setVendedCredentialsEnabledResult(true);
        provider.setRawVendedCredentialsResult(rawCredentials);

        MetastoreProperties metastoreProperties = Mockito.mock(MetastoreProperties.class);
        Object tableObject = new Object();

        Map<Type, StorageProperties> result = provider.getStoragePropertiesMapWithVendedCredentials(
                metastoreProperties, tableObject);

        // The filtering should happen internally via CredentialUtils.filterCloudStorageProperties()
        Assertions.assertNotNull(result);
    }

    @Test
    public void testGetStoragePropertiesMapWithOnlyNonCloudStorageProperties() {
        TestVendedCredentialsProvider provider = new TestVendedCredentialsProvider();

        // Setup credentials with only non-cloud storage properties
        Map<String, String> rawCredentials = new HashMap<>();
        rawCredentials.put("table.name", "test_table");
        rawCredentials.put("database.name", "test_db");
        rawCredentials.put("other.property", "other_value");

        provider.setVendedCredentialsEnabledResult(true);
        provider.setRawVendedCredentialsResult(rawCredentials);

        MetastoreProperties metastoreProperties = Mockito.mock(MetastoreProperties.class);
        Object tableObject = new Object();

        Map<Type, StorageProperties> result = provider.getStoragePropertiesMapWithVendedCredentials(
                metastoreProperties, tableObject);

        // Should return null since no cloud storage properties after filtering
        Assertions.assertNull(result);
    }

    @Test
    public void testGetStoragePropertiesMapWithNullRawCredentials() {
        TestVendedCredentialsProvider provider = new TestVendedCredentialsProvider();
        provider.setVendedCredentialsEnabledResult(true);
        provider.setRawVendedCredentialsResult(null);

        MetastoreProperties metastoreProperties = Mockito.mock(MetastoreProperties.class);
        Object tableObject = new Object();

        Map<Type, StorageProperties> result = provider.getStoragePropertiesMapWithVendedCredentials(
                metastoreProperties, tableObject);

        Assertions.assertNull(result);
    }

    @Test
    public void testGetStoragePropertiesMapWithExceptionHandling() {
        // Test the case where extractRawVendedCredentials returns null (simulating an internal failure)
        AbstractVendedCredentialsProvider provider = new AbstractVendedCredentialsProvider() {
            @Override
            protected boolean isVendedCredentialsEnabled(MetastoreProperties metastoreProperties) {
                return true;
            }

            @Override
            protected <T> Map<String, String> extractRawVendedCredentials(T tableObject) {
                // Return null to simulate extraction failure (like network timeout, invalid response, etc.)
                return null;
            }

            @Override
            protected <T> String getTableName(T tableObject) {
                return "test_table";
            }
        };

        MetastoreProperties metastoreProperties = Mockito.mock(MetastoreProperties.class);
        Object tableObject = new Object();

        // Should handle null credentials gracefully and return null
        Map<Type, StorageProperties> result = provider.getStoragePropertiesMapWithVendedCredentials(
                metastoreProperties, tableObject);

        Assertions.assertNull(result);
    }

    @Test
    public void testDefaultGetTableNameImplementation() {
        // Create a minimal provider that doesn't override getTableName() to test the default implementation
        AbstractVendedCredentialsProvider provider = new AbstractVendedCredentialsProvider() {
            @Override
            protected boolean isVendedCredentialsEnabled(MetastoreProperties metastoreProperties) {
                return true;
            }

            @Override
            protected <T> Map<String, String> extractRawVendedCredentials(T tableObject) {
                return new HashMap<>();
            }
        };

        // Test with null object
        String result1 = provider.getTableName(null);
        Assertions.assertEquals("null", result1);

        // Test with non-null object (should use the default implementation)
        Object tableObject = new Object();
        String result2 = provider.getTableName(tableObject);
        Assertions.assertEquals(tableObject.toString(), result2); // Default implementation returns toString()
    }

    @Test
    public void testAbstractMethodsAreImplemented() {
        // Verify that our test implementation correctly implements all abstract methods
        TestVendedCredentialsProvider provider = new TestVendedCredentialsProvider();

        MetastoreProperties metastoreProperties = Mockito.mock(MetastoreProperties.class);
        Object tableObject = new Object();

        // These should not throw AbstractMethodError
        Assertions.assertDoesNotThrow(() -> {
            provider.isVendedCredentialsEnabled(metastoreProperties);
            provider.extractRawVendedCredentials(tableObject);
            provider.getTableName(tableObject);
        });
    }

    @Test
    public void testWorkflowWithMultipleCloudStorageTypes() {
        TestVendedCredentialsProvider provider = new TestVendedCredentialsProvider();

        // Setup credentials with multiple cloud storage types
        Map<String, String> rawCredentials = new HashMap<>();
        rawCredentials.put("s3.access-key-id", "s3AccessKey");
        rawCredentials.put("s3.secret-access-key", "s3SecretKey");
        rawCredentials.put("s3.region", "us-west-2");
        rawCredentials.put("oss.access-key-id", "ossAccessKey");
        rawCredentials.put("oss.secret-access-key", "ossSecretKey");
        rawCredentials.put("oss.endpoint", "oss-cn-beijing.aliyuncs.com");
        rawCredentials.put("cos.access-key", "cosAccessKey");
        rawCredentials.put("cos.secret-key", "cosSecretKey");
        rawCredentials.put("non.cloud.property", "should_be_filtered");

        provider.setVendedCredentialsEnabledResult(true);
        provider.setRawVendedCredentialsResult(rawCredentials);

        MetastoreProperties metastoreProperties = Mockito.mock(MetastoreProperties.class);
        Object tableObject = new Object();

        Map<Type, StorageProperties> result = provider.getStoragePropertiesMapWithVendedCredentials(
                metastoreProperties, tableObject);

        // Should process multiple cloud storage types
        Assertions.assertNotNull(result);
    }
}
