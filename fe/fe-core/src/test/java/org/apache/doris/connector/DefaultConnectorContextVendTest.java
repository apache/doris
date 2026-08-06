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

package org.apache.doris.connector;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * FIX-REST-VENDED fe-core bridge test: pins that
 * {@link DefaultConnectorContext#vendStorageCredentials} reuses the engine's
 * {@code StorageProperties} normalization (the same chain legacy
 * {@code AbstractVendedCredentialsProvider} runs) to turn a raw per-table OSS vended token into the
 * BE-facing {@code AWS_*} storage properties. The connector cannot import that machinery, so this
 * hook is the single source of truth — without it a REST native-reader table reaches BE with no
 * usable credentials (403). FAILS before the fix (the method is a no-op default returning empty).
 */
public class DefaultConnectorContextVendTest {

    private static DefaultConnectorContext context() {
        return new DefaultConnectorContext("c", 1L);
    }

    @Test
    public void normalizesOssTokenToBackendAwsProps() {
        // Mirrors the raw OSS vended token shape from PaimonVendedCredentialsProviderTest.
        Map<String, String> token = new HashMap<>();
        token.put("fs.oss.accessKeyId", "STS.testAccessKey123");
        token.put("fs.oss.accessKeySecret", "testSecretKey456");
        token.put("fs.oss.securityToken", "testSessionToken789");
        token.put("fs.oss.endpoint", "oss-cn-beijing.aliyuncs.com");

        Map<String, String> be = context().vendStorageCredentials(token);

        // WHY: the BE native S3/object-store client consumes ONLY normalized AWS_* keys; the raw
        // fs.oss.* token is unintelligible to it. The bridge must run StorageProperties.createAll +
        // getBackendPropertiesFromStorageMap to produce them. MUTATION: leaving the default no-op
        // (empty) or skipping the normalization -> AWS_ACCESS_KEY absent -> red.
        Assertions.assertFalse(be.isEmpty(), "a valid OSS token must normalize to non-empty BE props");
        Assertions.assertEquals("STS.testAccessKey123", be.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("testSecretKey456", be.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("testSessionToken789", be.get("AWS_TOKEN"));
    }

    @Test
    public void emptyOrNullInputYieldsEmpty() {
        // WHY: a non-REST / no-token table passes an empty map; the bridge must short-circuit to
        // empty (no overlay), never NPE. MUTATION: NPE on null, or fabricating props from nothing -> red.
        Assertions.assertTrue(context().vendStorageCredentials(Collections.emptyMap()).isEmpty());
        Assertions.assertTrue(context().vendStorageCredentials(null).isEmpty());
    }
}
