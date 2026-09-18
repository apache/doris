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

package org.apache.doris.filesystem.ozone;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class OzoneFileSystemPropertiesTest {

    @Test
    void bind_appliesLegacyOzoneDefaults() {
        OzoneFileSystemProperties properties = OzoneFileSystemProperties.of(Map.of(
                "ozone.endpoint", "http://ozone-s3g.local:9878",
                "ozone.access_key", "ak",
                "ozone.secret_key", "sk"));

        Assertions.assertEquals("http://ozone-s3g.local:9878", properties.getEndpoint());
        Assertions.assertEquals("us-east-1", properties.getRegion());
        Assertions.assertEquals("true", properties.getUsePathStyle());
        Assertions.assertEquals("OZONE", properties.providerName());
    }

    @Test
    void bind_prefersOzoneAliasOverS3Alias() {
        OzoneFileSystemProperties properties = OzoneFileSystemProperties.of(Map.of(
                "ozone.endpoint", "http://ozone-s3g.local:9878",
                "s3.endpoint", "https://s3.example.com",
                "ozone.access_key", "ozone-ak",
                "s3.access_key", "s3-ak",
                "ozone.secret_key", "ozone-sk",
                "s3.secret_key", "s3-sk"));

        Assertions.assertEquals("http://ozone-s3g.local:9878", properties.getEndpoint());
        Assertions.assertEquals("ozone-ak", properties.getAccessKey());
        Assertions.assertEquals("ozone-sk", properties.getSecretKey());
    }

    @Test
    void bind_acceptsCanonicalAwsShapedMap() {
        // A provider=OZONE map carries its normalized region as AWS_REGION; a non-default value
        // proves the alias binds rather than silently falling back to the us-east-1 default.
        OzoneFileSystemProperties properties = OzoneFileSystemProperties.of(Map.of(
                "provider", "OZONE",
                "AWS_ENDPOINT", "http://ozone-s3g.local:9878",
                "AWS_REGION", "eu-west-2",
                "AWS_ACCESS_KEY", "ak",
                "AWS_SECRET_KEY", "sk",
                "use_path_style", "true"));

        Assertions.assertEquals("http://ozone-s3g.local:9878", properties.getEndpoint());
        Assertions.assertEquals("eu-west-2", properties.getRegion());
        Assertions.assertEquals("eu-west-2", properties.toS3CompatibleKv().get("AWS_REGION"));
        Assertions.assertEquals("ak", properties.getAccessKey());
    }

    @Test
    void validate_requiresEndpoint() {
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> OzoneFileSystemProperties.of(Map.of(
                        "ozone.access_key", "ak",
                        "ozone.secret_key", "sk")));
        Assertions.assertTrue(e.getMessage().contains("ozone.endpoint is required"),
                e.getMessage());
    }

    @Test
    void validate_rejectsAwsOnlyCredentialOptions() {
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> OzoneFileSystemProperties.of(Map.of(
                        "ozone.endpoint", "http://ozone-s3g.local:9878",
                        "ozone.access_key", "ak",
                        "ozone.secret_key", "sk",
                        "AWS_ROLE_ARN", "arn:aws:iam::1:role/x")));
        Assertions.assertTrue(e.getMessage().contains("HMAC"), e.getMessage());
    }

    @Test
    void toS3CompatibleKv_forwardsCanonicalAwsKeysWithPathStyleDefaultTrue() {
        Map<String, String> kv = OzoneFileSystemProperties.of(Map.of(
                "ozone.endpoint", "http://ozone-s3g.local:9878",
                "ozone.access_key", "ak",
                "ozone.secret_key", "sk")).toS3CompatibleKv();

        Assertions.assertEquals("http://ozone-s3g.local:9878", kv.get("AWS_ENDPOINT"));
        Assertions.assertEquals("true", kv.get("use_path_style"));
    }

    @Test
    void toS3CompatibleKv_fallsBackToAnonymousWithoutStaticCredentials() {
        Map<String, String> kv = OzoneFileSystemProperties.of(Map.of(
                "ozone.endpoint", "http://127.0.0.1:9878")).toS3CompatibleKv();

        Assertions.assertEquals("ANONYMOUS", kv.get("AWS_CREDENTIALS_PROVIDER_TYPE"));
        Assertions.assertFalse(kv.containsKey("AWS_ACCESS_KEY"));
    }

    @Test
    void toS3CompatibleKv_preservesExplicitDefaultCredentialMode() {
        // An explicit DEFAULT (env/system/profile/instance chain) with no inline keys must survive
        // rather than being silently downgraded to unsigned ANONYMOUS.
        Map<String, String> kv = OzoneFileSystemProperties.of(Map.of(
                "ozone.endpoint", "http://127.0.0.1:9878",
                "AWS_CREDENTIALS_PROVIDER_TYPE", "DEFAULT")).toS3CompatibleKv();

        Assertions.assertEquals("DEFAULT", kv.get("AWS_CREDENTIALS_PROVIDER_TYPE"));
        Assertions.assertFalse(kv.containsKey("AWS_ACCESS_KEY"));
    }

    @Test
    void toString_masksSecretsButNotAccessKey() {
        String rendered = OzoneFileSystemProperties.of(Map.of(
                "ozone.endpoint", "http://ozone-s3g.local:9878",
                "ozone.access_key", "ozone-ak-plain",
                "ozone.secret_key", "ozone-sk-plain",
                "ozone.session_token", "ozone-token-plain")).toString();

        Assertions.assertFalse(rendered.contains("ozone-sk-plain"), rendered);
        Assertions.assertFalse(rendered.contains("ozone-token-plain"), rendered);
        Assertions.assertTrue(rendered.contains("accessKey=ozone-ak-plain"), rendered);
    }

    // ------------------------------------------------------------------
    // uri-derived endpoint (legacy AbstractS3CompatibleProperties
    // setEndpointIfPossible leg 2, inherited by fe-core OzoneProperties).
    // Expected values are hardcoded from the legacy fe-core S3URI algorithm.
    // ------------------------------------------------------------------

    @Test
    void uriOnly_defaultPathStyle_derivesEndpointBeforeEndpointRequiredCheck() {
        // Ozone defaults to path-style addressing, so the whole authority is the endpoint.
        OzoneFileSystemProperties properties = OzoneFileSystemProperties.of(Map.of(
                "uri", "http://ozone-s3g.local:9878/vol-bucket/key1",
                "ozone.access_key", "ak",
                "ozone.secret_key", "sk"));

        Assertions.assertEquals("ozone-s3g.local:9878", properties.getEndpoint());
        // Region keeps the legacy Ozone default.
        Assertions.assertEquals("us-east-1", properties.getRegion());
    }

    @Test
    void unparsableUri_isSwallowed_thenEndpointRequiredFires() {
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> OzoneFileSystemProperties.of(Map.of(
                        "uri", "ozone-s3g.local:9878/vol-bucket/key1", // no scheme
                        "ozone.access_key", "ak",
                        "ozone.secret_key", "sk")));
        Assertions.assertTrue(exception.getMessage().contains("Property ozone.endpoint is required."),
                exception.getMessage());
    }
}
