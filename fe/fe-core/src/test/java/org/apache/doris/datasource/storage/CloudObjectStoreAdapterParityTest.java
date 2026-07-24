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
import org.apache.doris.datasource.property.storage.S3Properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Phase B3 golden comparison for the cloud meta-service PB WIRE CONTRACT (master plan §2.7-③):
 * {@link CloudObjectStoreAdapter#getObjStoreInfoPB} must equal the legacy
 * {@code S3Properties.getObjStoreInfoPB} for every builder field — endpoint, region, ak, sk,
 * prefix, bucket, external_endpoint, provider, use_path_style, cred_provider_type, role_arn,
 * external_id. Protobuf message equality covers all of them at once.
 */
public class CloudObjectStoreAdapterParityTest {

    private static Map<String, String> map(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static void assertSamePb(Map<String, String> props) {
        Cloud.ObjectStoreInfoPB oracle = S3Properties.getObjStoreInfoPB(new HashMap<>(props)).build();
        Cloud.ObjectStoreInfoPB actual = CloudObjectStoreAdapter.getObjStoreInfoPB(new HashMap<>(props)).build();
        Assertions.assertEquals(oracle, actual);
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
        assertSamePb(props);
        // Sanity: the adapter output really carries every field of the wire contract.
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
        assertSamePb(map(
                "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "s3.role_arn", "arn:aws:iam::123456789012:role/doris",
                "s3.external_id", "ext-1"));
    }

    @Test
    public void testBlankRoleArnSetsNoRoleFields() {
        assertSamePb(map(
                "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "s3.role_arn", "",
                "s3.external_id", "ext-1"));
    }

    @Test
    public void testLegacyEnvProviderTypeKey() {
        assertSamePb(map(
                "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "AWS_CREDENTIALS_PROVIDER_TYPE", "ANONYMOUS"));
    }

    @Test
    public void testUsePathStyleFalse() {
        assertSamePb(map(
                "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "use_path_style", "false"));
    }

    @Test
    public void testEmptyMap() {
        assertSamePb(new HashMap<>());
    }
}
