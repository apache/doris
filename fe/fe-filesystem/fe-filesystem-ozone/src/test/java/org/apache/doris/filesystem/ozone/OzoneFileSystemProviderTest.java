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

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.s3.S3FileSystem;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class OzoneFileSystemProviderTest {

    private final OzoneFileSystemProvider provider = new OzoneFileSystemProvider();

    @Test
    void supports_acceptsExplicitHints() {
        Map<String, String> providerHint = new HashMap<>();
        providerHint.put("provider", "OZONE");

        Map<String, String> fsSupport = new HashMap<>();
        fsSupport.put("fs.ozone.support", "true");
        fsSupport.put("s3.endpoint", "http://ozone-s3g.local:9878");

        Assertions.assertTrue(provider.supports(providerHint));
        Assertions.assertTrue(provider.supports(fsSupport));
    }

    @Test
    void supports_neverGuesses() {
        // Legacy OzoneProperties has no guessIsMe: Ozone is reachable ONLY through an explicit
        // fs.ozone.support=true / provider=OZONE. An unflagged Ozone map (even with ozone.* keys
        // or the generic "S3" family marker) falls through to the generic S3 provider, which
        // serves the S3-compatible gateway correctly.
        Map<String, String> ozoneKeys = new HashMap<>();
        ozoneKeys.put("ozone.endpoint", "http://ozone-s3g.local:9878");
        ozoneKeys.put("ozone.access_key", "ak");

        Map<String, String> marked = new HashMap<>();
        marked.put("_STORAGE_TYPE_", "S3");
        marked.put("AWS_ENDPOINT", "http://ozone-s3g.local:9878");

        Assertions.assertFalse(provider.supports(ozoneKeys));
        Assertions.assertFalse(provider.supports(marked));
    }

    @Test
    void supports_rejectsForeignDialects() {
        Map<String, String> minio = new HashMap<>();
        minio.put("minio.endpoint", "http://127.0.0.1:9000");

        Map<String, String> plainS3 = new HashMap<>();
        plainS3.put("s3.endpoint", "https://s3.us-east-1.amazonaws.com");
        plainS3.put("s3.access_key", "ak");

        Map<String, String> gcs = new HashMap<>();
        gcs.put("fs.gcs.support", "true");

        Assertions.assertFalse(provider.supports(minio));
        Assertions.assertFalse(provider.supports(plainS3));
        Assertions.assertFalse(provider.supports(gcs));
    }

    @Test
    void create_delegatesToS3FileSystem() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("ozone.endpoint", "http://ozone-s3g.local:9878");
        props.put("ozone.access_key", "ak");
        props.put("ozone.secret_key", "sk");

        FileSystem fileSystem = provider.create(provider.bind(props));

        Assertions.assertInstanceOf(S3FileSystem.class, fileSystem);
        Assertions.assertEquals("http://ozone-s3g.local:9878",
                ((S3FileSystem) fileSystem).properties().orElseThrow().getEndpoint());
    }

    @Test
    void sensitivePropertyKeys_coverSecretAliasesButNotAccessKey() {
        Set<String> keys = provider.sensitivePropertyKeys();

        Assertions.assertTrue(keys.contains("ozone.secret_key"), keys.toString());
        Assertions.assertTrue(keys.contains("ozone.session_token"), keys.toString());
        Assertions.assertFalse(keys.contains("ozone.access_key"), keys.toString());
    }

    @Test
    void name_isOzone() {
        Assertions.assertEquals("OZONE", provider.name());
    }
}
