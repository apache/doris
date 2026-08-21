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

package org.apache.doris.common.plugin;

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.datasource.property.common.AwsCredentialsProviderMode;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.datasource.storage.StorageTypeId;
import org.apache.doris.filesystem.properties.S3CompatibleFileSystemProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CloudPluginDownloaderStorageAdapterTest {

    @Test
    void testCreateStorageAdapterUsesMetaServiceProvider() {
        assertProvider(Cloud.ObjectStoreInfoPB.Provider.S3, "s3.us-east-1.amazonaws.com",
                "us-east-1", StorageTypeId.S3, "S3");
        assertProvider(Cloud.ObjectStoreInfoPB.Provider.GCP, "storage.googleapis.com",
                "us-east1", StorageTypeId.S3, "S3");
        assertProvider(Cloud.ObjectStoreInfoPB.Provider.OSS, "oss-cn-hangzhou.aliyuncs.com",
                "cn-hangzhou", StorageTypeId.OSS, "OSS");
        assertProvider(Cloud.ObjectStoreInfoPB.Provider.COS, "cos.ap-beijing.myqcloud.com",
                "ap-beijing", StorageTypeId.COS, "COS");
        assertProvider(Cloud.ObjectStoreInfoPB.Provider.OBS, "obs.cn-north-4.myhuaweicloud.com",
                "cn-north-4", StorageTypeId.OBS, "OBS");
        assertProvider(Cloud.ObjectStoreInfoPB.Provider.BOS, "s3.bj.bcebos.com",
                "bj", StorageTypeId.S3, "S3");
        assertProvider(Cloud.ObjectStoreInfoPB.Provider.TOS, "tos-cn-beijing.volces.com",
                "cn-beijing", StorageTypeId.S3, "S3");
        assertProvider(Cloud.ObjectStoreInfoPB.Provider.AZURE, "account.blob.core.windows.net",
                "", StorageTypeId.AZURE, "AZURE");
    }

    @Test
    void testCreateStorageAdapterPreservesClientOptions() {
        Cloud.ObjectStoreInfoPB objectStoreInfo = objectStoreInfo(
                Cloud.ObjectStoreInfoPB.Provider.S3, "s3.us-east-1.amazonaws.com", "us-east-1")
                .toBuilder()
                .setUsePathStyle(true)
                .setCredProviderType(Cloud.CredProviderTypePB.ENV)
                .build();

        StorageAdapter adapter = CloudPluginDownloader.createStorageAdapter(objectStoreInfo);
        S3CompatibleFileSystemProperties properties =
                (S3CompatibleFileSystemProperties) adapter.getSpiProperties();

        Assertions.assertEquals("true", properties.getUsePathStyle());
        Assertions.assertEquals(AwsCredentialsProviderMode.ENV, adapter.getAwsCredentialsProviderMode());
    }

    private static void assertProvider(Cloud.ObjectStoreInfoPB.Provider provider, String endpoint,
            String region, StorageTypeId expectedType, String expectedProviderName) {
        StorageAdapter adapter = CloudPluginDownloader.createStorageAdapter(
                objectStoreInfo(provider, endpoint, region));
        Assertions.assertEquals(expectedType, adapter.getType());
        Assertions.assertEquals(expectedProviderName, adapter.getSpiProperties().providerName());
    }

    private static Cloud.ObjectStoreInfoPB objectStoreInfo(Cloud.ObjectStoreInfoPB.Provider provider,
            String endpoint, String region) {
        return Cloud.ObjectStoreInfoPB.newBuilder()
                .setProvider(provider)
                .setEndpoint(endpoint)
                .setRegion(region)
                .setAk("test-ak")
                .setSk("test-sk")
                .setBucket("test-bucket")
                .build();
    }
}
