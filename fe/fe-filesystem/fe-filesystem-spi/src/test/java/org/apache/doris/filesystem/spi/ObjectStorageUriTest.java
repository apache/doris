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

package org.apache.doris.filesystem.spi;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

/**
 * Unit tests for the single shared object-storage URI parser used by every S3-compatible
 * provider (S3, OSS, COS, OBS).
 */
class ObjectStorageUriTest {

    @Test
    void parseVirtualHostedStyle() {
        ObjectStorageUri uri = ObjectStorageUri.parse("s3://my-bucket/path/to/file.csv", false);
        Assertions.assertEquals("my-bucket", uri.bucket());
        Assertions.assertEquals("path/to/file.csv", uri.key());
    }

    @Test
    void parseS3aScheme() {
        ObjectStorageUri uri = ObjectStorageUri.parse("s3a://my-bucket/key", false);
        Assertions.assertEquals("my-bucket", uri.bucket());
        Assertions.assertEquals("key", uri.key());
    }

    @Test
    void parseS3nScheme() {
        ObjectStorageUri uri = ObjectStorageUri.parse("s3n://my-bucket/dir/file", false);
        Assertions.assertEquals("my-bucket", uri.bucket());
        Assertions.assertEquals("dir/file", uri.key());
    }

    @Test
    void parseBucketOnly() {
        ObjectStorageUri uri = ObjectStorageUri.parse("s3://my-bucket", false);
        Assertions.assertEquals("my-bucket", uri.bucket());
        Assertions.assertEquals("", uri.key());
    }

    @Test
    void parseNormalizesDoubleSlashInKey() {
        ObjectStorageUri uri = ObjectStorageUri.parse("s3://bucket//path", false);
        Assertions.assertEquals("bucket", uri.bucket());
        Assertions.assertEquals("path", uri.key());
    }

    @Test
    void parseHttpsEndpoint() {
        ObjectStorageUri uri = ObjectStorageUri.parse("https://s3.amazonaws.com/my-bucket/key", false);
        Assertions.assertEquals("s3.amazonaws.com", uri.bucket());
        Assertions.assertEquals("my-bucket/key", uri.key());
    }

    @Test
    void nullPathThrows() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> ObjectStorageUri.parse(null, false));
    }

    @Test
    void noSchemeThrows() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> ObjectStorageUri.parse("bucket/key", false));
    }

    // ------------------------------------------------------------------
    // Scheme is not a discriminator: the provider is chosen by properties
    // (endpoint domain / explicit storage type), not by URI scheme. The parser is
    // intentionally lenient so that any provider's native scheme AND the historically
    // normalized s3:// form parse the same way. e.g. a path supplied to the OSS/COS/OBS
    // backend is frequently "s3://...".
    // ------------------------------------------------------------------

    @Test
    void parseAcceptsCloudNativeSchemes() {
        Assertions.assertEquals("b", ObjectStorageUri.parse("oss://b/k", false).bucket());
        Assertions.assertEquals("b", ObjectStorageUri.parse("cos://b/k", false).bucket());
        Assertions.assertEquals("b", ObjectStorageUri.parse("cosn://b/k", false).bucket());
        Assertions.assertEquals("b", ObjectStorageUri.parse("obs://b/k", false).bucket());
        Assertions.assertEquals("k", ObjectStorageUri.parse("cosn://b/k", false).key());
    }

    @Test
    void parseAcceptsS3SchemeForCloudBackends() {
        // The legacy normalized form: an s3:// path that actually targets an OSS/COS/OBS
        // bucket must parse identically to the native-scheme form.
        ObjectStorageUri uri = ObjectStorageUri.parse("s3://oss-bucket/dir/file", false);
        Assertions.assertEquals("oss-bucket", uri.bucket());
        Assertions.assertEquals("dir/file", uri.key());
    }

    // ------------------------------------------------------------------
    // Path-style HTTP(S) parsing
    // ------------------------------------------------------------------

    @Test
    void parsePathStyleHttpsTreatsFirstPathSegmentAsBucket() {
        ObjectStorageUri uri = ObjectStorageUri.parse("https://endpoint.example.com/my-bucket/key/x.csv", true);
        Assertions.assertEquals("my-bucket", uri.bucket());
        Assertions.assertEquals("key/x.csv", uri.key());
    }

    @Test
    void parsePathStyleHttpAlsoSupported() {
        ObjectStorageUri uri = ObjectStorageUri.parse("http://10.0.0.1:9000/data/dir/file", true);
        Assertions.assertEquals("data", uri.bucket());
        Assertions.assertEquals("dir/file", uri.key());
    }

    @Test
    void parsePathStyleBucketOnly() {
        ObjectStorageUri uri = ObjectStorageUri.parse("https://endpoint/my-bucket", true);
        Assertions.assertEquals("my-bucket", uri.bucket());
        Assertions.assertEquals("", uri.key());
    }

    @Test
    void parsePathStyleTrailingSlashIsEmptyKey() {
        ObjectStorageUri uri = ObjectStorageUri.parse("https://endpoint/my-bucket/", true);
        Assertions.assertEquals("my-bucket", uri.bucket());
        Assertions.assertEquals("", uri.key());
    }

    @Test
    void parsePathStyleMissingBucketThrows() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ObjectStorageUri.parse("https://endpoint", true));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ObjectStorageUri.parse("https://endpoint/", true));
    }

    @Test
    void parseS3SchemeIgnoresPathStyleFlag() {
        // pathStyleAccess only affects http/https URIs; s3:// (and other non-http schemes) are
        // always virtual-hosted regardless of the flag.
        ObjectStorageUri uri = ObjectStorageUri.parse("s3://my-bucket/path/to/file", true);
        Assertions.assertEquals("my-bucket", uri.bucket());
        Assertions.assertEquals("path/to/file", uri.key());
    }

    // ------------------------------------------------------------------
    // Per-provider scheme validation (the parse(path, pathStyle, supportedSchemes) overload)
    // ------------------------------------------------------------------

    @Test
    void parseAcceptsSchemeInSupportedSet() {
        ObjectStorageUri uri = ObjectStorageUri.parse("s3://b/k", false, Set.of("s3", "s3a", "oss"));
        Assertions.assertEquals("b", uri.bucket());
        Assertions.assertEquals("k", uri.key());
    }

    @Test
    void parseRejectsForeignScheme() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ObjectStorageUri.parse("oss://b/k", false, Set.of("cos", "cosn", "s3", "s3a")));
    }

    @Test
    void parseAcceptsHttpsEndpointEvenWhenNotInSupportedSet() {
        // http/https is the endpoint-URL form (used by TVF like s3()), not a provider-identity
        // scheme, so it is accepted regardless of supportedSchemes (which stays explicit for
        // catalog scheme-to-storage routing and intentionally omits http/https). Path-style on:
        // first path segment is the bucket.
        ObjectStorageUri uri = ObjectStorageUri.parse(
                "https://endpoint.example.com/my-bucket/dir/file", true, Set.of("oss", "s3", "s3a"));
        Assertions.assertEquals("my-bucket", uri.bucket());
        Assertions.assertEquals("dir/file", uri.key());
    }

    @Test
    void parseAcceptsHttpEndpointEvenWhenNotInSupportedSet() {
        ObjectStorageUri uri = ObjectStorageUri.parse(
                "http://10.0.0.1:9000/data/dir/file", true, Set.of("cos", "cosn", "s3", "s3a"));
        Assertions.assertEquals("data", uri.bucket());
        Assertions.assertEquals("dir/file", uri.key());
    }

    @Test
    void parseHttpsWithoutPathStyleKeepsHostAsBucket() {
        // When pathStyleAccess=false, https://endpoint/bucket/key parses the host as the
        // "bucket" (existing behaviour preserved).
        ObjectStorageUri uri = ObjectStorageUri.parse("https://s3.amazonaws.com/my-bucket/key", false);
        Assertions.assertEquals("s3.amazonaws.com", uri.bucket());
        Assertions.assertEquals("my-bucket/key", uri.key());
    }
}
