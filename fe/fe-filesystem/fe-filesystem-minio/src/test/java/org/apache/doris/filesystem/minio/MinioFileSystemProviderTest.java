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

package org.apache.doris.filesystem.minio;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.s3.S3FileSystem;
import org.apache.doris.filesystem.s3.S3ObjStorage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class MinioFileSystemProviderTest {

    private final MinioFileSystemProvider provider = new MinioFileSystemProvider();

    @Test
    void supports_acceptsExplicitHints() {
        Map<String, String> providerHint = new HashMap<>();
        providerHint.put("provider", "MINIO");

        Map<String, String> fsSupport = new HashMap<>();
        fsSupport.put("fs.minio.support", "true");
        fsSupport.put("s3.endpoint", "http://127.0.0.1:9000");

        Assertions.assertTrue(provider.supports(providerHint));
        Assertions.assertTrue(provider.supports(fsSupport));
    }

    @Test
    void supports_guessesFromMinioPrefixKeys() {
        Map<String, String> props = new HashMap<>();
        props.put("minio.endpoint", "http://127.0.0.1:9000");
        props.put("minio.access_key", "ak");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_ignoresGenericStorageTypeMarker() {
        // The marker names a storage FAMILY ("S3" for every S3-compatible map), so it must not
        // affect dialect selection either way.
        Map<String, String> props = new HashMap<>();
        props.put("_STORAGE_TYPE_", "S3");
        props.put("minio.endpoint", "http://127.0.0.1:9000");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_doesNotGuessWhenAnotherFilesystemIsDeclared() {
        for (Map.Entry<String, String> hint : Map.of(
                "fs.s3.support", "true",
                "fs.gcs.support", "true",
                "provider", "S3").entrySet()) {
            Map<String, String> props = new HashMap<>();
            props.put(hint.getKey(), hint.getValue());
            props.put("minio.endpoint", "http://127.0.0.1:9000");

            Assertions.assertFalse(provider.supports(props), "must not guess for " + hint);
        }
    }

    @Test
    void supports_rejectsMinioKeysAlongsideAwsOrGcsEndpoint() {
        // Port of legacy MinioProperties.guessIsMe, which yields to S3/GCS when their endpoints
        // are present.
        Map<String, String> aws = new HashMap<>();
        aws.put("minio.access_key", "ak");
        aws.put("s3.endpoint", "https://s3.us-east-1.amazonaws.com");

        Map<String, String> gcs = new HashMap<>();
        gcs.put("minio.access_key", "ak");
        gcs.put("s3.endpoint", "https://storage.googleapis.com");

        Assertions.assertFalse(provider.supports(aws));
        Assertions.assertFalse(provider.supports(gcs));
    }

    @Test
    void supports_rejectsForeignDialects() {
        Map<String, String> gcs = new HashMap<>();
        gcs.put("gs.access_key", "ak");

        Map<String, String> plainS3 = new HashMap<>();
        plainS3.put("s3.endpoint", "https://s3.us-east-1.amazonaws.com");
        plainS3.put("s3.access_key", "ak");

        Assertions.assertFalse(provider.supports(gcs));
        Assertions.assertFalse(provider.supports(plainS3));
    }

    @Test
    void create_delegatesToS3FileSystem() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("minio.endpoint", "http://127.0.0.1:9000");
        props.put("minio.access_key", "ak");
        props.put("minio.secret_key", "sk");

        FileSystem fileSystem = provider.create(provider.bind(props));

        Assertions.assertInstanceOf(S3FileSystem.class, fileSystem);
        Assertions.assertEquals("http://127.0.0.1:9000",
                ((S3FileSystem) fileSystem).properties().orElseThrow().getEndpoint());
    }

    @Test
    void create_threadsDialectSchemesIntoStorage() throws Exception {
        // Without the schemes override, S3ObjStorage would report the inner
        // S3FileSystemProperties' {s3, s3a, s3n} instead of MinIO's {s3, s3a}.
        Map<String, String> props = new HashMap<>();
        props.put("minio.endpoint", "http://127.0.0.1:9000");
        props.put("minio.access_key", "ak");
        props.put("minio.secret_key", "sk");

        S3FileSystem s3 = (S3FileSystem) provider.create(provider.bind(props));
        S3ObjStorage storage = (S3ObjStorage) s3.getObjStorage();

        Assertions.assertEquals(Set.of("s3", "s3a"), storage.getSupportedSchemes());
    }

    @Test
    void create_forwardsConfiguredHttpSchemeToS3Client() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("minio.endpoint", "127.0.0.1:9000");
        props.put("minio.access_key", "ak");
        props.put("minio.secret_key", "sk");
        props.put("s3_client_http_scheme", "http");

        S3FileSystem s3 = (S3FileSystem) provider.create(props);
        S3ObjStorage storage = (S3ObjStorage) s3.getObjStorage();
        try {
            Assertions.assertEquals(URI.create("http://127.0.0.1:9000"),
                    storage.getClient().serviceClientConfiguration().endpointOverride().orElseThrow());
        } finally {
            storage.close();
        }
    }

    @Test
    void sensitivePropertyKeys_coverSecretAliasesButNotAccessKey() {
        Set<String> keys = provider.sensitivePropertyKeys();

        Assertions.assertTrue(keys.contains("minio.secret_key"), keys.toString());
        Assertions.assertTrue(keys.contains("minio.session_token"), keys.toString());
        Assertions.assertFalse(keys.contains("minio.access_key"), keys.toString());
    }

    @Test
    void name_isMinio() {
        Assertions.assertEquals("MINIO", provider.name());
    }
}
