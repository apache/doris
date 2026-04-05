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

package org.apache.doris.filesystem.s3;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link S3ObjStorage} focusing on property normalization
 * and constructor behavior. Does not require real AWS credentials — the
 * S3 client is never built.
 */
class S3ObjStorageTest {

    // ------------------------------------------------------------------
    // normalizeProperties()
    // ------------------------------------------------------------------

    @Test
    void normalizeProperties_canonicalKeysAlreadyPresent() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ACCESS_KEY", "canonical-ak");
        props.put("AWS_SECRET_KEY", "canonical-sk");
        props.put("AWS_ENDPOINT", "https://s3.amazonaws.com");
        props.put("AWS_REGION", "us-east-1");
        props.put("AWS_TOKEN", "tok");

        Map<String, String> result = S3ObjStorage.normalizeProperties(props);

        Assertions.assertEquals("canonical-ak", result.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("canonical-sk", result.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("https://s3.amazonaws.com", result.get("AWS_ENDPOINT"));
        Assertions.assertEquals("us-east-1", result.get("AWS_REGION"));
        Assertions.assertEquals("tok", result.get("AWS_TOKEN"));
    }

    @Test
    void normalizeProperties_s3DotPrefixAliasesNormalized() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.access_key", "ak-from-s3-prefix");
        props.put("s3.secret_key", "sk-from-s3-prefix");
        props.put("s3.endpoint", "https://minio.local");
        props.put("s3.region", "us-west-2");
        props.put("s3.session_token", "sess-tok");

        Map<String, String> result = S3ObjStorage.normalizeProperties(props);

        Assertions.assertEquals("ak-from-s3-prefix", result.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("sk-from-s3-prefix", result.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("https://minio.local", result.get("AWS_ENDPOINT"));
        Assertions.assertEquals("us-west-2", result.get("AWS_REGION"));
        Assertions.assertEquals("sess-tok", result.get("AWS_TOKEN"));
    }

    @Test
    void normalizeProperties_bareAliasesNormalized() {
        Map<String, String> props = new HashMap<>();
        props.put("access_key", "ak-bare");
        props.put("secret_key", "sk-bare");
        props.put("endpoint", "https://endpoint.bare");
        props.put("region", "ap-southeast-1");

        Map<String, String> result = S3ObjStorage.normalizeProperties(props);

        Assertions.assertEquals("ak-bare", result.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("sk-bare", result.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("https://endpoint.bare", result.get("AWS_ENDPOINT"));
        Assertions.assertEquals("ap-southeast-1", result.get("AWS_REGION"));
    }

    @Test
    void normalizeProperties_uppercaseAliasesNormalized() {
        Map<String, String> props = new HashMap<>();
        props.put("ACCESS_KEY", "ak-upper");
        props.put("SECRET_KEY", "sk-upper");
        props.put("ENDPOINT", "https://upper.endpoint");
        props.put("REGION", "eu-west-1");

        Map<String, String> result = S3ObjStorage.normalizeProperties(props);

        Assertions.assertEquals("ak-upper", result.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("sk-upper", result.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("https://upper.endpoint", result.get("AWS_ENDPOINT"));
        Assertions.assertEquals("eu-west-1", result.get("AWS_REGION"));
    }

    @Test
    void normalizeProperties_canonicalKeyTakesPrecedenceOverAlias() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ACCESS_KEY", "canonical");
        props.put("s3.access_key", "alias-should-be-ignored");
        props.put("access_key", "bare-should-be-ignored");

        Map<String, String> result = S3ObjStorage.normalizeProperties(props);

        Assertions.assertEquals("canonical", result.get("AWS_ACCESS_KEY"));
    }

    @Test
    void normalizeProperties_firstMatchingAliasWins() {
        // s3.access_key comes before access_key in alias order
        Map<String, String> props = new HashMap<>();
        props.put("s3.access_key", "s3-prefix-wins");
        props.put("access_key", "bare-loses");

        Map<String, String> result = S3ObjStorage.normalizeProperties(props);

        Assertions.assertEquals("s3-prefix-wins", result.get("AWS_ACCESS_KEY"));
    }

    @Test
    void normalizeProperties_noMatchingAliasLeavesNull() {
        Map<String, String> props = new HashMap<>();
        props.put("unrelated_key", "value");

        Map<String, String> result = S3ObjStorage.normalizeProperties(props);

        Assertions.assertNull(result.get("AWS_ACCESS_KEY"));
        Assertions.assertNull(result.get("AWS_SECRET_KEY"));
        Assertions.assertNull(result.get("AWS_ENDPOINT"));
        Assertions.assertNull(result.get("AWS_REGION"));
    }

    // ------------------------------------------------------------------
    // Constructor & getProperties()
    // ------------------------------------------------------------------

    @Test
    void constructor_propertiesAreNormalizedAndImmutable() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.access_key", "ak");
        props.put("s3.secret_key", "sk");
        props.put("s3.endpoint", "https://ep");
        props.put("AWS_BUCKET", "my-bucket");

        S3ObjStorage storage = new S3ObjStorage(props);
        Map<String, String> stored = storage.getProperties();

        Assertions.assertEquals("ak", stored.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("sk", stored.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("https://ep", stored.get("AWS_ENDPOINT"));
        Assertions.assertEquals("my-bucket", stored.get("AWS_BUCKET"));

        Assertions.assertThrows(UnsupportedOperationException.class, () -> stored.put("new", "val"),
                "getProperties() should return unmodifiable map");
    }

    @Test
    void constructor_usePathStyleDefaultsFalse() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://s3.amazonaws.com");

        S3ObjStorage storage = new S3ObjStorage(props);
        Map<String, String> stored = storage.getProperties();

        Assertions.assertEquals("false", stored.getOrDefault("use_path_style", "false"));
    }

    @Test
    void constructor_usePathStyleTrueWhenSet() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://minio.local");
        props.put("use_path_style", "true");

        S3ObjStorage storage = new S3ObjStorage(props);
        Map<String, String> stored = storage.getProperties();

        Assertions.assertEquals("true", stored.get("use_path_style"));
    }

    @Test
    void constructor_originalMapMutationDoesNotAffectStorage() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ACCESS_KEY", "original");
        props.put("AWS_ENDPOINT", "https://ep");

        S3ObjStorage storage = new S3ObjStorage(props);
        props.put("AWS_ACCESS_KEY", "mutated");

        Assertions.assertEquals("original", storage.getProperties().get("AWS_ACCESS_KEY"),
                "Constructor must copy the input map");
    }

    // ------------------------------------------------------------------
    // close()
    // ------------------------------------------------------------------

    @Test
    void close_doesNotThrowWhenClientNotBuilt() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://ep");

        S3ObjStorage storage = new S3ObjStorage(props);
        storage.close();
    }
}
