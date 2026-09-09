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

package org.apache.doris.cloud.storage;

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.datasource.storage.StorageTypeId;
import org.apache.doris.filesystem.properties.S3CompatibleFileSystemProperties;
import org.apache.doris.foundation.property.StoragePropertiesException;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class ObjectInfoAdapterTest {

    @Test
    public void testToStorageAdapterPreservesAwsRoleFields() {
        ObjectInfo objectInfo = new ObjectInfo(
                Cloud.ObjectStoreInfoPB.Provider.S3,
                "",
                "",
                "snapshot-bucket",
                "s3.us-west-2.amazonaws.com",
                "us-west-2",
                "snapshot/prefix",
                "snapshot-session",
                "arn:aws:iam::123456789012:role/snapshot-role",
                "snapshot-external-id",
                null);

        StorageAdapter adapter = ObjectInfoAdapter.toStorageAdapter(objectInfo);
        S3CompatibleFileSystemProperties s3 = (S3CompatibleFileSystemProperties) adapter.getSpiProperties();

        Assert.assertEquals(StorageTypeId.S3, adapter.getType());
        Assert.assertEquals("s3.us-west-2.amazonaws.com", s3.getEndpoint());
        Assert.assertEquals("us-west-2", s3.getRegion());
        Assert.assertEquals("snapshot-bucket", s3.getBucket());
        Assert.assertEquals("arn:aws:iam::123456789012:role/snapshot-role", s3.getRoleArn());
        Assert.assertEquals("snapshot-external-id", s3.getExternalId());
    }

    @Test
    public void testOssStageBindingCarriesEveryField() {
        // cloud_p0 test_copy_into regression shape: an OSS stage's ObjectInfo must round-trip
        // through the flattened property map into the OSS dialect binding. OSS aliases bucket
        // only as {OSS_BUCKET, AWS_BUCKET} — the s3.bucket-only flattening left the binding
        // bucketless and CREATE STAGE pings failed with "OSS bucket is required".
        ObjectInfo objectInfo = new ObjectInfo(
                Cloud.ObjectStoreInfoPB.Provider.OSS,
                "stage-ak",
                "stage-sk",
                "doris-regression-hk",
                "oss-cn-hongkong-internal.aliyuncs.com",
                "cn-hongkong",
                "smoke-test",
                null,
                null,
                null,
                "stage-token");

        StorageAdapter adapter = ObjectInfoAdapter.toStorageAdapter(objectInfo);
        S3CompatibleFileSystemProperties oss = (S3CompatibleFileSystemProperties) adapter.getSpiProperties();

        Assert.assertEquals(StorageTypeId.OSS, adapter.getType());
        Assert.assertEquals("doris-regression-hk", oss.getBucket());
        Assert.assertEquals("oss-cn-hongkong-internal.aliyuncs.com", oss.getEndpoint());
        Assert.assertEquals("cn-hongkong", oss.getRegion());
        Assert.assertEquals("stage-ak", oss.getAccessKey());
        Assert.assertEquals("stage-sk", oss.getSecretKey());
        Assert.assertEquals("stage-token", oss.getSessionToken());
    }

    @Test
    public void testAzureSharedKeyWithoutTokenPreservesNativeParameters() {
        for (String token : new String[] {null, ""}) {
            StorageAdapter adapter = ObjectInfoAdapter.toStorageAdapter(azureObjectInfo("account-key", token));
            Map<String, String> backend = adapter.getBackendConfigProperties();

            Assert.assertEquals(StorageTypeId.AZURE, adapter.getType());
            Assert.assertEquals("azure", backend.get("provider"));
            Assert.assertEquals("SHARED_KEY", backend.get("AZURE_AUTH_TYPE"));
            Assert.assertEquals("account-key", backend.get("AZURE_ACCOUNT_KEY"));
            Assert.assertEquals("account", backend.get("AZURE_ACCOUNT_NAME"));
            Assert.assertEquals("https://account.blob.core.windows.net", backend.get("AZURE_ENDPOINT"));
            Assert.assertEquals("container", backend.get("AZURE_CONTAINER"));
            Assert.assertFalse(backend.containsKey("AZURE_SAS_TOKEN"));
            Assert.assertFalse(backend.containsKey("AZURE_SAS_EXPIRY_MS"));
        }
    }

    @Test
    public void testAzureSasReplacesSharedKeyInsteadOfCombiningCredentials() {
        String token = "?sv=2024-01-01&se=2100-01-01T00%3A00%3A00Z&sig=temporary%2Bsignature";
        for (String key : new String[] {null, "", "old-account-key"}) {
            ObjectInfo objectInfo = azureObjectInfo(key, token);
            StorageAdapter adapter = ObjectInfoAdapter.toStorageAdapter(objectInfo);
            Map<String, String> backend = adapter.getBackendConfigProperties();

            Assert.assertEquals(StorageTypeId.AZURE, adapter.getType());
            Assert.assertEquals("SAS", adapter.getOrigProps().get("azure.auth_type"));
            Assert.assertFalse(adapter.getOrigProps().containsKey("azure.account_key"));
            Assert.assertEquals("azure", backend.get("provider"));
            Assert.assertEquals("SAS", backend.get("AZURE_AUTH_TYPE"));
            Assert.assertEquals(token.substring(1), backend.get("AZURE_SAS_TOKEN"));
            Assert.assertEquals("4102444800000", backend.get("AZURE_SAS_EXPIRY_MS"));
            Assert.assertEquals("account", backend.get("AZURE_ACCOUNT_NAME"));
            Assert.assertEquals("container", backend.get("AZURE_CONTAINER"));
            Assert.assertEquals("https://account.blob.core.windows.net", backend.get("AZURE_ENDPOINT"));
            Assert.assertFalse(backend.containsKey("AZURE_ACCOUNT_KEY"));
            Assert.assertFalse(backend.keySet().stream().anyMatch(name -> name.startsWith("AWS_")));
            Assert.assertEquals(key, objectInfo.getSk());
            Assert.assertEquals(token, objectInfo.getToken());
        }
    }

    @Test
    public void testAzureMalformedSasDoesNotFallBackToSharedKey() {
        String token = "se=invalid-expiry&sig=must-not-be-logged";
        StoragePropertiesException failure = Assert.assertThrows(StoragePropertiesException.class,
                () -> ObjectInfoAdapter.toStorageAdapter(azureObjectInfo("account-key", token)));

        Assert.assertEquals("Azure SAS credential has an invalid expiry", failure.getMessage());
        Assert.assertNull(failure.getCause());
        Assert.assertFalse(failure.getMessage().contains("must-not-be-logged"));
    }

    @Test
    public void testAzureBlankSuppliedSasDoesNotFallBackToSharedKey() {
        IllegalArgumentException failure = Assert.assertThrows(IllegalArgumentException.class,
                () -> ObjectInfoAdapter.toStorageAdapter(azureObjectInfo("account-key", " ")));

        Assert.assertTrue(failure.getMessage().contains("When auth_type is SAS, sas_token is required"));
    }

    @Test
    public void testAzureExpiredSasRejectsAccessInsteadOfFallingBackToSharedKey() {
        StorageAdapter adapter = ObjectInfoAdapter.toStorageAdapter(azureObjectInfo(
                "account-key", "se=2000-01-01T00%3A00%3A00Z&sig=expired"));

        Assert.assertEquals("SAS", adapter.getOrigProps().get("azure.auth_type"));
        Assert.assertFalse(adapter.getOrigProps().containsKey("azure.account_key"));
        StoragePropertiesException failure = Assert.assertThrows(StoragePropertiesException.class,
                adapter::getBackendConfigProperties);
        Assert.assertEquals("Azure SAS credential is expired", failure.getMessage());
        Assert.assertNull(failure.getCause());
    }

    @Test
    public void testObjectInfoMasksTokensWithoutChangingCredentials() {
        for (Cloud.ObjectStoreInfoPB.Provider provider : new Cloud.ObjectStoreInfoPB.Provider[] {
                Cloud.ObjectStoreInfoPB.Provider.AZURE,
                Cloud.ObjectStoreInfoPB.Provider.OSS,
                Cloud.ObjectStoreInfoPB.Provider.S3}) {
            ObjectInfo objectInfo = new ObjectInfo(provider, "account", "secret-key-plain", "container",
                    "endpoint", "region", "prefix", null, null, null, "token-plain");

            String rendered = objectInfo.toString();
            Assert.assertFalse(rendered.contains("token-plain"));
            Assert.assertFalse(rendered.contains("secret-key-plain"));
            Assert.assertTrue(rendered.contains("token='******'"));
            Assert.assertEquals("token-plain", objectInfo.getToken());
            Assert.assertEquals("secret-key-plain", objectInfo.getSk());
        }
    }

    private static ObjectInfo azureObjectInfo(String key, String token) {
        return new ObjectInfo(Cloud.ObjectStoreInfoPB.Provider.AZURE, "account", key, "container",
                "account.blob.core.windows.net", "", "stage-prefix", null, null, null, token);
    }
}
