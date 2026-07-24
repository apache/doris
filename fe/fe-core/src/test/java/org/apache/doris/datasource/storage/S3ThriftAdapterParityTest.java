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

import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.thrift.TS3StorageParam;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Phase B3 golden comparison: {@link S3ThriftAdapter#getS3TStorageParam} must equal the legacy
 * {@code S3Properties.getS3TStorageParam} (oracle) for every input shape — TS3StorageParam has
 * a generated equals(), so whole-object equality covers every thrift field.
 */
public class S3ThriftAdapterParityTest {

    private static Map<String, String> map(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static void assertSameThrift(Map<String, String> props) {
        TS3StorageParam oracle = S3Properties.getS3TStorageParam(new HashMap<>(props));
        TS3StorageParam actual = S3ThriftAdapter.getS3TStorageParam(new HashMap<>(props));
        Assertions.assertEquals(oracle, actual);
    }

    @Test
    public void testFullUserKeyMap() {
        assertSameThrift(map(
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
                "use_path_style", "true"));
    }

    @Test
    public void testMinimalMapUsesDefaults() {
        assertSameThrift(map(
                "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "s3.region", "us-east-1"));
    }

    @Test
    public void testAssumeRoleWithExternalIdAndDefaultProviderType() {
        // role_arn present, no credentials_provider_type -> INSTANCE_PROFILE default.
        assertSameThrift(map(
                "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "s3.region", "us-east-1",
                "s3.role_arn", "arn:aws:iam::123456789012:role/doris",
                "s3.external_id", "ext-1"));
    }

    @Test
    public void testAssumeRoleWithExplicitProviderType() {
        assertSameThrift(map(
                "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "s3.region", "us-east-1",
                "s3.role_arn", "arn:aws:iam::123456789012:role/doris",
                "s3.credentials_provider_type", "ENV"));
    }

    @Test
    public void testAssumeRoleWithLegacyEnvProviderTypeKey() {
        // The AWS_CREDENTIALS_PROVIDER_TYPE fallback key is honoured after the s3.* key.
        assertSameThrift(map(
                "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "s3.region", "us-east-1",
                "s3.role_arn", "arn:aws:iam::123456789012:role/doris",
                "AWS_CREDENTIALS_PROVIDER_TYPE", "SYSTEM_PROPERTIES"));
    }

    @Test
    public void testEmptyMap() {
        assertSameThrift(new HashMap<>());
    }
}
