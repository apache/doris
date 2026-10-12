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

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.properties.FileSystemProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class S3FileSystemProviderTest {

    private final S3FileSystemProvider provider = new S3FileSystemProvider();

    @Test
    void supports_acceptsRoleBasedS3Configuration() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://s3.us-west-2.amazonaws.com");
        props.put("AWS_REGION", "us-west-2");
        props.put("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/snapshot-role");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_acceptsConfiguredCredentialsProviderType() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://s3.us-west-2.amazonaws.com");
        props.put("AWS_REGION", "us-west-2");
        props.put("s3.credentials_provider_type", "ENV");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_rejectsConfigurationWithoutCredentialsOrRole() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://s3.us-west-2.amazonaws.com");
        props.put("AWS_REGION", "us-west-2");

        Assertions.assertFalse(provider.supports(props));
    }

    @Test
    void supports_acceptsLegacyConvertedMapWithoutExplicitCredentials() {
        Map<String, String> props = new HashMap<>();
        props.put("_STORAGE_TYPE_", "S3");
        props.put("AWS_ENDPOINT", "https://s3.us-west-2.amazonaws.com");
        props.put("AWS_REGION", "us-west-2");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_acceptsExplicitS3ProviderWithoutCredentials() {
        Map<String, String> props = new HashMap<>();
        props.put("provider", "S3");
        props.put("s3.endpoint", "https://s3.us-west-2.amazonaws.com");
        props.put("s3.region", "us-west-2");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_acceptsExplicitS3SupportWithoutCredentials() {
        Map<String, String> props = new HashMap<>();
        props.put("fs.s3.support", "true");
        props.put("s3.endpoint", "https://s3.us-west-2.amazonaws.com");
        props.put("s3.region", "us-west-2");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_yieldsToGuessedDedicatedDialects() {
        // These maps satisfy the generic credential+location fallback, but a dedicated dialect
        // provider of this module recognizes them, so S3 must yield. The decision comes from the
        // map alone -- no _STORAGE_TYPE_ marker and no reliance on SPI registration order.
        Map<String, String> gcs = new HashMap<>();
        gcs.put("AWS_ENDPOINT", "https://storage.googleapis.com");
        gcs.put("AWS_ACCESS_KEY", "ak");
        gcs.put("AWS_SECRET_KEY", "sk");

        Map<String, String> minio = new HashMap<>();
        minio.put("minio.endpoint", "http://127.0.0.1:9000");
        minio.put("minio.access_key", "ak");
        minio.put("AWS_ENDPOINT", "http://127.0.0.1:9000");
        minio.put("AWS_ACCESS_KEY", "ak");

        Assertions.assertFalse(provider.supports(gcs));
        Assertions.assertFalse(provider.supports(minio));
    }

    @Test
    void supports_yieldsToExplicitlyRequestedDedicatedDialects() {
        // An explicit dialect request wins even when the endpoint carries no dialect signal at all
        // (Ozone in particular has no guess and can only be reached this way).
        for (Map.Entry<String, String> hint : Map.of(
                "fs.gcs.support", "true",
                "fs.minio.support", "true",
                "fs.ozone.support", "true",
                "provider", "OZONE").entrySet()) {
            Map<String, String> props = new HashMap<>();
            props.put(hint.getKey(), hint.getValue());
            props.put("s3.endpoint", "http://ozone-s3g.local:9878");
            props.put("s3.access_key", "ak");

            Assertions.assertFalse(provider.supports(props), "must yield for " + hint);
        }
    }

    @Test
    void supports_keepsGoogleapisMapWhenUserExplicitlyRequestsS3() {
        // An explicit fs.s3.support=true / provider=S3 overrides the dialect guess: the user has
        // the final say about which filesystem serves the map.
        Map<String, String> fsSupport = new HashMap<>();
        fsSupport.put("fs.s3.support", "true");
        fsSupport.put("s3.endpoint", "https://storage.googleapis.com");
        fsSupport.put("s3.access_key", "ak");

        Map<String, String> providerHint = new HashMap<>();
        providerHint.put("provider", "S3");
        providerHint.put("s3.endpoint", "https://storage.googleapis.com");
        providerHint.put("s3.access_key", "ak");

        Assertions.assertTrue(provider.supports(fsSupport));
        Assertions.assertTrue(provider.supports(providerHint));
    }

    @Test
    void supports_yieldsForConvertedGcsMapDespiteGenericS3Marker() {
        // StoragePropertiesConverter stamps _STORAGE_TYPE_="S3" on every S3-compatible map, GCS
        // included, so the marker must NOT be treated as an explicit S3 request by the yield rule.
        Map<String, String> props = new HashMap<>();
        props.put("_STORAGE_TYPE_", "S3");
        props.put("provider", "GCP");
        props.put("AWS_ENDPOINT", "https://storage.googleapis.com");
        props.put("AWS_ACCESS_KEY", "ak");
        props.put("AWS_REGION", "us-east1");

        Assertions.assertFalse(provider.supports(props));
    }

    @Test
    void supports_keepsUnflaggedOzoneMap() {
        // Ozone has no guessIsMe in legacy, so an unflagged Ozone gateway map is served by the
        // generic S3 provider. Documents that this is intended, not an accident.
        Map<String, String> props = new HashMap<>();
        props.put("ozone.endpoint", "http://ozone-s3g.local:9878");
        props.put("AWS_ENDPOINT", "http://ozone-s3g.local:9878");
        props.put("AWS_ACCESS_KEY", "ak");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_stillAcceptsUnmarkedGenericFallback() {
        // Cloud snapshot / stage flows pass unmarked maps; the generic fallback must survive.
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://example.com");
        props.put("AWS_ACCESS_KEY", "ak");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void bind_returnsValidatedS3FileSystemProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.endpoint", "https://minio.local");
        props.put("s3.region", "us-west-2");
        props.put("s3.access_key", "ak");
        props.put("s3.secret_key", "sk");

        FileSystemProperties bound = provider.bind(props);

        Assertions.assertInstanceOf(S3FileSystemProperties.class, bound);
        Assertions.assertEquals("https://minio.local", ((S3FileSystemProperties) bound).getEndpoint());
    }

    @Test
    void create_usesTypedS3FileSystemProperties() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("s3.endpoint", "https://minio.local");
        props.put("s3.region", "us-west-2");
        props.put("s3.access_key", "ak");
        props.put("s3.secret_key", "sk");

        FileSystem fileSystem = provider.create(provider.bind(props));

        Assertions.assertInstanceOf(S3FileSystem.class, fileSystem);
        S3FileSystem s3 = (S3FileSystem) fileSystem;
        Assertions.assertTrue(s3.properties().isPresent());
        Assertions.assertEquals("https://minio.local", s3.properties().orElseThrow().getEndpoint());
    }

    @Test
    void sensitivePropertyKeysCoverSecretsButNotAccessKey() {
        java.util.Set<String> keys = provider.sensitivePropertyKeys();

        Assertions.assertTrue(keys.contains("s3.secret_key"), keys.toString());
        Assertions.assertTrue(keys.contains("s3.session_token"), keys.toString());
        Assertions.assertFalse(keys.contains("s3.access_key"), keys.toString());
        Assertions.assertFalse(keys.contains("AWS_ACCESS_KEY"), keys.toString());
    }
}
