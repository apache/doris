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

import org.junit.Assert;
import org.junit.Test;

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
}
