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

package org.apache.doris.datasource.storage;

import org.apache.doris.cloud.proto.Cloud;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Frozen wire-contract test for the cloud meta-service PB: expected
 * {@link Cloud.ObjectStoreInfoPB} objects are built field-by-field to the values the legacy
 * implementation produced (verified against it before Phase D deleted it). Protobuf message
 * equality covers every builder field at once — containsKey gating, provider valueOf,
 * use_path_style validation and the role_arn/cred_provider_type interplay are all pinned.
 */
public class CloudObjectStoreAdapterParityTest {

    private static Map<String, String> map(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }


    @Test
    public void testAllBuilderFieldsPopulated() {
        Map<String, String> props = map(
                "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "s3.region", "us-east-1",
                "s3.access_key", "myAk",
                "s3.secret_key", "mySk",
                "s3.root.path", "warehouse/prefix",
                "s3.bucket", "mybucket",
                "s3.external_endpoint", "https://external.example.com",
                "provider", "oss",
                "use_path_style", "TRUE",
                "s3.credentials_provider_type", "ENV",
                "s3.role_arn", "arn:aws:iam::123456789012:role/doris",
                "s3.external_id", "ext-1");
        // Every field of the wire contract, frozen explicitly.
        Cloud.ObjectStoreInfoPB pb = CloudObjectStoreAdapter.getObjStoreInfoPB(props).build();
        Assertions.assertEquals("https://s3.us-east-1.amazonaws.com", pb.getEndpoint());
        Assertions.assertEquals("us-east-1", pb.getRegion());
        Assertions.assertEquals("myAk", pb.getAk());
        Assertions.assertEquals("mySk", pb.getSk());
        Assertions.assertEquals("warehouse/prefix", pb.getPrefix());
        Assertions.assertEquals("mybucket", pb.getBucket());
        Assertions.assertEquals("https://external.example.com", pb.getExternalEndpoint());
        Assertions.assertEquals(Cloud.ObjectStoreInfoPB.Provider.OSS, pb.getProvider());
        Assertions.assertTrue(pb.getUsePathStyle());
        Assertions.assertEquals(Cloud.CredProviderTypePB.ENV, pb.getCredProviderType());
        Assertions.assertEquals("arn:aws:iam::123456789012:role/doris", pb.getRoleArn());
        Assertions.assertEquals("ext-1", pb.getExternalId());
    }

    @Test
    public void testRoleArnWithoutProviderTypeDefaultsInstanceProfile() {
        Cloud.ObjectStoreInfoPB e = Cloud.ObjectStoreInfoPB.newBuilder()
                .setEndpoint("https://s3.us-east-1.amazonaws.com")
                .setRoleArn("arn:aws:iam::123456789012:role/doris")
                .setExternalId("ext-1")
                .setCredProviderType(Cloud.CredProviderTypePB.INSTANCE_PROFILE)
                .build();
        Assertions.assertEquals(e, CloudObjectStoreAdapter.getObjStoreInfoPB(map(
                "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "s3.role_arn", "arn:aws:iam::123456789012:role/doris",
                "s3.external_id", "ext-1")).build());
    }

    @Test
    public void testBlankRoleArnSetsNoRoleFields() {
        // blank role_arn: key present but empty -> no role fields, no cred provider type
        Cloud.ObjectStoreInfoPB e = Cloud.ObjectStoreInfoPB.newBuilder()
                .setEndpoint("https://s3.us-east-1.amazonaws.com")
                .build();
        Assertions.assertEquals(e, CloudObjectStoreAdapter.getObjStoreInfoPB(map(
                "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "s3.role_arn", "",
                "s3.external_id", "ext-1")).build());
    }

    @Test
    public void testLegacyEnvProviderTypeKey() {
        Cloud.ObjectStoreInfoPB e = Cloud.ObjectStoreInfoPB.newBuilder()
                .setEndpoint("https://s3.us-east-1.amazonaws.com")
                .setCredProviderType(Cloud.CredProviderTypePB.ANONYMOUS)
                .build();
        Assertions.assertEquals(e, CloudObjectStoreAdapter.getObjStoreInfoPB(map(
                "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "AWS_CREDENTIALS_PROVIDER_TYPE", "ANONYMOUS")).build());
    }

    @Test
    public void testUsePathStyleFalse() {
        Cloud.ObjectStoreInfoPB e = Cloud.ObjectStoreInfoPB.newBuilder()
                .setEndpoint("https://s3.us-east-1.amazonaws.com")
                .setUsePathStyle(false)
                .build();
        Assertions.assertEquals(e, CloudObjectStoreAdapter.getObjStoreInfoPB(map(
                "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "use_path_style", "false")).build());
    }

    @Test
    public void testEmptyMap() {
        Assertions.assertEquals(Cloud.ObjectStoreInfoPB.newBuilder().build(),
                CloudObjectStoreAdapter.getObjStoreInfoPB(new HashMap<>()).build());
    }
}
