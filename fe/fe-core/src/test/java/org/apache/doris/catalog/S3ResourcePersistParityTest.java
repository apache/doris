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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Guard tests (2.7-④) freezing the exact persisted property map S3Resource.setProperties
 * produces. S3Resource is image-bearing and its logic is frozen for the SPI migration
 * ("move, don't rewrite"): it deliberately writes BOTH the user (s3.*) and the legacy env
 * (AWS_*) namespaces into the persisted map. Any migration of its constants/utilities must
 * keep these input→persisted-map mappings byte-identical.
 */
public class S3ResourcePersistParityTest {

    private static void assertExactMap(Map<String, String> expected, Map<String, String> actual) {
        Assertions.assertEquals(new TreeMap<>(expected), new TreeMap<>(actual));
    }

    @Test
    public void testStdKeysInputPersistedMapExact() throws DdlException {
        Map<String, String> input = new HashMap<>();
        input.put("s3.endpoint", "s3.us-east-1.amazonaws.com");
        input.put("s3.access_key", "myAk");
        input.put("s3.secret_key", "mySk");
        input.put("s3.bucket", "mybucket");
        input.put("s3_validity_check", "false");

        S3Resource resource = new S3Resource("parity_std");
        resource.setProperties(ImmutableMap.copyOf(input));

        Map<String, String> golden = new HashMap<>();
        // Dual-namespace write: the ping endpoint gets an http:// prefix and lands under BOTH keys.
        golden.put("s3.endpoint", "http://s3.us-east-1.amazonaws.com");
        golden.put("AWS_ENDPOINT", "http://s3.us-east-1.amazonaws.com");
        // Region derived from the endpoint host (second dot-segment).
        golden.put("s3.region", "us-east-1");
        golden.put("s3.access_key", "myAk");
        golden.put("s3.secret_key", "mySk");
        golden.put("s3.bucket", "mybucket");
        golden.put("s3_validity_check", "false");
        // optionalS3Property defaults, again in both namespaces.
        golden.put("s3.connection.maximum", "50");
        golden.put("s3.connection.request.timeout", "3000");
        golden.put("s3.connection.timeout", "1000");
        golden.put("AWS_MAX_CONNECTIONS", "50");
        golden.put("AWS_REQUEST_TIMEOUT_MS", "3000");
        golden.put("AWS_CONNECTION_TIMEOUT_MS", "1000");
        assertExactMap(golden, resource.getCopiedProperties());
    }

    @Test
    public void testLegacyAwsKeysInputPersistedMapExact() throws DdlException {
        Map<String, String> input = new HashMap<>();
        input.put("AWS_ENDPOINT", "s3.us-east-1.amazonaws.com");
        input.put("AWS_ACCESS_KEY", "myAk");
        input.put("AWS_SECRET_KEY", "mySk");
        input.put("AWS_BUCKET", "mybucket");
        input.put("s3_validity_check", "false");

        S3Resource resource = new S3Resource("parity_legacy");
        resource.setProperties(ImmutableMap.copyOf(input));

        Map<String, String> golden = new HashMap<>();
        // convertToStdProperties mirrors legacy AWS_* keys into the s3.* namespace first;
        // the endpoint then gets the http:// prefix under both keys.
        golden.put("AWS_ENDPOINT", "http://s3.us-east-1.amazonaws.com");
        golden.put("s3.endpoint", "http://s3.us-east-1.amazonaws.com");
        golden.put("s3.region", "us-east-1");
        golden.put("AWS_ACCESS_KEY", "myAk");
        golden.put("s3.access_key", "myAk");
        golden.put("AWS_SECRET_KEY", "mySk");
        golden.put("s3.secret_key", "mySk");
        golden.put("AWS_BUCKET", "mybucket");
        golden.put("s3.bucket", "mybucket");
        golden.put("s3_validity_check", "false");
        golden.put("s3.connection.maximum", "50");
        golden.put("s3.connection.request.timeout", "3000");
        golden.put("s3.connection.timeout", "1000");
        golden.put("AWS_MAX_CONNECTIONS", "50");
        golden.put("AWS_REQUEST_TIMEOUT_MS", "3000");
        golden.put("AWS_CONNECTION_TIMEOUT_MS", "1000");
        assertExactMap(golden, resource.getCopiedProperties());
    }

    @Test
    public void testVaultAndValidityConstantsFrozen() {
        // 2.7-①/②: image/DDL-bearing literals — must survive the constants migration verbatim.
        Assertions.assertEquals("s3.access_key", S3StorageVault.PropertyKey.ACCESS_KEY);
        Assertions.assertEquals("s3.secret_key", S3StorageVault.PropertyKey.SECRET_KEY);
        Assertions.assertEquals("use_path_style", S3StorageVault.PropertyKey.USE_PATH_STYLE);
        Assertions.assertEquals("s3.root.path", S3StorageVault.PropertyKey.ROOT_PATH);
        Assertions.assertEquals("s3.region", S3StorageVault.PropertyKey.REGION);
        Assertions.assertEquals("s3.endpoint", S3StorageVault.PropertyKey.ENDPOINT);
        Assertions.assertEquals("s3.bucket", S3StorageVault.PropertyKey.BUCKET);
        Assertions.assertEquals("s3.role_arn", S3StorageVault.PropertyKey.ROLE_ARN);
        Assertions.assertEquals("s3.external_id", S3StorageVault.PropertyKey.EXTERNAL_ID);
        Assertions.assertEquals("s3_validity_check",
                org.apache.doris.datasource.property.storage.S3Properties.VALIDITY_CHECK);
        Assertions.assertEquals("s3_validity_check", HdfsStorageVault.S3_VALIDITY_CHECK);
    }
}
