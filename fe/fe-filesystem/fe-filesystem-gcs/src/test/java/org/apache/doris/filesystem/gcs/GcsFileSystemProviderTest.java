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

package org.apache.doris.filesystem.gcs;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.s3.S3FileSystem;
import org.apache.doris.filesystem.s3.S3ObjStorage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class GcsFileSystemProviderTest {

    private final GcsFileSystemProvider provider = new GcsFileSystemProvider();

    @Test
    void supports_acceptsConvertedGcsMapViaProviderHint() {
        // What StoragePropertiesConverter produces for a GCSProperties instance: the generic "S3"
        // family marker plus provider=GCP from getBackendConfigProperties().
        Map<String, String> props = new HashMap<>();
        props.put("_STORAGE_TYPE_", "S3");
        props.put("provider", "GCP");
        props.put("AWS_ENDPOINT", "https://storage.googleapis.com");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_ignoresGenericStorageTypeMarker() {
        // The marker names a storage FAMILY, not a dialect, so it neither selects nor blocks GCS;
        // the googleapis endpoint alone decides here.
        Map<String, String> googleapis = new HashMap<>();
        googleapis.put("_STORAGE_TYPE_", "S3");
        googleapis.put("AWS_ENDPOINT", "https://storage.googleapis.com");

        Map<String, String> plain = new HashMap<>();
        plain.put("_STORAGE_TYPE_", "S3");
        plain.put("AWS_ENDPOINT", "https://s3.us-east-1.amazonaws.com");

        Assertions.assertTrue(provider.supports(googleapis));
        Assertions.assertFalse(provider.supports(plain));
    }

    @Test
    void supports_acceptsGsEndpointKey() {
        Map<String, String> props = new HashMap<>();
        props.put("gs.endpoint", "https://storage.googleapis.com");
        props.put("gs.access_key", "ak");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_rejectsGsCredentialKeysWithoutEndpointSignal() {
        // Faithful to legacy GCSProperties.guessIsMe, which keys off gs.endpoint / a googleapis
        // endpoint only -- gs.access_key alone is not a selection signal.
        Map<String, String> props = new HashMap<>();
        props.put("gs.access_key", "ak");
        props.put("gs.secret_key", "sk");

        Assertions.assertFalse(provider.supports(props));
        // ... but an explicit flag still routes such a map to GCS.
        props.put("fs.gcs.support", "true");
        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_acceptsUnmarkedGoogleapisEndpoint() {
        Map<String, String> props = new HashMap<>();
        props.put("endpoint", "https://storage.googleapis.com");
        props.put("access_key", "ak");
        props.put("secret_key", "sk");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_doesNotGuessWhenAnotherFilesystemIsDeclared() {
        // Mirrors StorageProperties: any explicit fs.xx.support=true (or provider=S3) disables the
        // guess heuristics, so an explicit declaration is never overridden.
        Map<String, String> foreignFlag = new HashMap<>();
        foreignFlag.put("fs.s3.support", "true");
        foreignFlag.put("endpoint", "https://storage.googleapis.com");

        Map<String, String> s3Provider = new HashMap<>();
        s3Provider.put("provider", "S3");
        s3Provider.put("endpoint", "https://storage.googleapis.com");

        Assertions.assertFalse(provider.supports(foreignFlag));
        Assertions.assertFalse(provider.supports(s3Provider));
    }

    @Test
    void supports_acceptsProviderAndFsSupportHints() {
        Map<String, String> gcsProvider = new HashMap<>();
        gcsProvider.put("provider", "GCS");

        Map<String, String> gcpProvider = new HashMap<>();
        gcpProvider.put("provider", "GCP");

        Map<String, String> fsGcsSupport = new HashMap<>();
        fsGcsSupport.put("fs.gcs.support", "true");

        Assertions.assertTrue(provider.supports(gcsProvider));
        Assertions.assertTrue(provider.supports(gcpProvider));
        Assertions.assertTrue(provider.supports(fsGcsSupport));
    }

    @Test
    void supports_acceptsGoogleapisEndpointViaS3Alias() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.endpoint", "https://storage.googleapis.com");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_rejectsForeignDialects() {
        Map<String, String> minio = new HashMap<>();
        minio.put("minio.endpoint", "http://127.0.0.1:9000");
        minio.put("minio.access_key", "ak");

        Map<String, String> plainS3 = new HashMap<>();
        plainS3.put("s3.endpoint", "https://s3.us-east-1.amazonaws.com");
        plainS3.put("s3.access_key", "ak");

        Assertions.assertFalse(provider.supports(minio));
        Assertions.assertFalse(provider.supports(plainS3));
    }

    @Test
    void create_delegatesToS3FileSystemWithGcsDefaults() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("gs.access_key", "ak");
        props.put("gs.secret_key", "sk");

        FileSystem fileSystem = provider.create(provider.bind(props));

        Assertions.assertInstanceOf(S3FileSystem.class, fileSystem);
        S3FileSystem s3 = (S3FileSystem) fileSystem;
        Assertions.assertEquals("https://storage.googleapis.com",
                s3.properties().orElseThrow().getEndpoint());
    }

    @Test
    void create_threadsDialectSchemesIntoStorage() throws Exception {
        // Without the schemes override, S3ObjStorage would report the inner
        // S3FileSystemProperties' {s3, s3a, s3n} and reject gs:// URIs.
        Map<String, String> props = new HashMap<>();
        props.put("gs.access_key", "ak");
        props.put("gs.secret_key", "sk");

        S3FileSystem s3 = (S3FileSystem) provider.create(provider.bind(props));
        S3ObjStorage storage = (S3ObjStorage) s3.getObjStorage();

        Assertions.assertEquals(Set.of("gs", "s3", "s3a"), storage.getSupportedSchemes());
    }

    @Test
    void sensitivePropertyKeys_coverSecretAliasesButNotAccessKey() {
        Set<String> keys = provider.sensitivePropertyKeys();

        Assertions.assertTrue(keys.contains("gs.secret_key"), keys.toString());
        Assertions.assertTrue(keys.contains("gs.session_token"), keys.toString());
        Assertions.assertFalse(keys.contains("gs.access_key"), keys.toString());
    }

    @Test
    void name_isGcs() {
        Assertions.assertEquals("GCS", provider.name());
    }
}
