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

import org.apache.doris.thrift.TCredProviderType;
import org.apache.doris.thrift.TS3StorageParam;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Frozen wire-contract test for {@link S3ThriftAdapter#getS3TStorageParam}: the expected
 * {@link TS3StorageParam} objects are built field-by-field to the values the legacy
 * implementation produced (verified against it before Phase D deleted it). TS3StorageParam has
 * a generated equals(), so whole-object equality covers every thrift field — any drift in
 * defaults ("50"/"3000"/"1000"), containsKey gating or credProviderType resolution turns red.
 */
public class S3ThriftAdapterParityTest {

    private static Map<String, String> map(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    /** Baseline expected object: the unconditional fields with their frozen defaults. */
    private static TS3StorageParam expected(String endpoint, String region) {
        TS3StorageParam e = new TS3StorageParam();
        e.setEndpoint(endpoint);
        e.setRegion(region);
        e.setAk(null);
        e.setSk(null);
        e.setToken(null);
        e.setRootPath(null);
        e.setBucket(null);
        e.setMaxConn(50);
        e.setRequestTimeoutMs(3000);
        e.setConnTimeoutMs(1000);
        e.setUsePathStyle(false);
        return e;
    }

    @Test
    public void testFullUserKeyMap() {
        TS3StorageParam e = expected("https://s3.us-east-1.amazonaws.com", "us-east-1");
        e.setAk("myAk");
        e.setSk("mySk");
        e.setToken("tok");
        e.setRootPath("warehouse/prefix");
        e.setBucket("mybucket");
        e.setMaxConn(77);
        e.setRequestTimeoutMs(4000);
        e.setConnTimeoutMs(1500);
        e.setUsePathStyle(true);
        Assertions.assertEquals(e, S3ThriftAdapter.getS3TStorageParam(map(
                "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "s3.region", "us-east-1",
                "s3.access_key", "myAk",
                "s3.secret_key", "mySk",
                "s3.session_token", "tok",
                "s3.root.path", "warehouse/prefix",
                "s3.bucket", "mybucket",
                "s3.connection.maximum", "77",
                "s3.connection.request.timeout", "4000",
                "s3.connection.timeout", "1500",
                "use_path_style", "true")));
    }

    @Test
    public void testMinimalMapUsesDefaults() {
        Assertions.assertEquals(expected("https://s3.us-east-1.amazonaws.com", "us-east-1"),
                S3ThriftAdapter.getS3TStorageParam(map(
                        "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                        "s3.region", "us-east-1")));
    }

    @Test
    public void testAssumeRoleWithExternalIdAndDefaultProviderType() {
        // role_arn present, no credentials_provider_type -> INSTANCE_PROFILE default.
        TS3StorageParam e = expected("https://s3.us-east-1.amazonaws.com", "us-east-1");
        e.setRoleArn("arn:aws:iam::123456789012:role/doris");
        e.setExternalId("ext-1");
        e.setCredProviderType(TCredProviderType.INSTANCE_PROFILE);
        Assertions.assertEquals(e, S3ThriftAdapter.getS3TStorageParam(map(
                "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "s3.region", "us-east-1",
                "s3.role_arn", "arn:aws:iam::123456789012:role/doris",
                "s3.external_id", "ext-1")));
    }

    @Test
    public void testAssumeRoleWithExplicitProviderType() {
        TS3StorageParam e = expected("https://s3.us-east-1.amazonaws.com", "us-east-1");
        e.setRoleArn("arn:aws:iam::123456789012:role/doris");
        e.setCredProviderType(TCredProviderType.ENV);
        Assertions.assertEquals(e, S3ThriftAdapter.getS3TStorageParam(map(
                "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "s3.region", "us-east-1",
                "s3.role_arn", "arn:aws:iam::123456789012:role/doris",
                "s3.credentials_provider_type", "ENV")));
    }

    @Test
    public void testAssumeRoleWithLegacyEnvProviderTypeKey() {
        // The AWS_CREDENTIALS_PROVIDER_TYPE fallback key is honoured after the s3.* key.
        TS3StorageParam e = expected("https://s3.us-east-1.amazonaws.com", "us-east-1");
        e.setRoleArn("arn:aws:iam::123456789012:role/doris");
        e.setCredProviderType(TCredProviderType.SYSTEM_PROPERTIES);
        Assertions.assertEquals(e, S3ThriftAdapter.getS3TStorageParam(map(
                "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "s3.region", "us-east-1",
                "s3.role_arn", "arn:aws:iam::123456789012:role/doris",
                "AWS_CREDENTIALS_PROVIDER_TYPE", "SYSTEM_PROPERTIES")));
    }

    @Test
    public void testEmptyMap() {
        Assertions.assertEquals(expected(null, null),
                S3ThriftAdapter.getS3TStorageParam(new HashMap<>()));
    }
}
