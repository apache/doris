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

/**
 * Parsed object-storage URI. Extracts bucket and key without any fe-core dependency.
 *
 * <p>FIXME: this should be an interface, with each provider implementing its own
 * {@code normalize}. The current single shared parser is essentially the legacy approach:
 * historically OBS/COS/OSS/S3 URIs all had to be normalized into S3 form, either to hand
 * to the BE or for the FE's own use. With per-provider native SDKs that constraint no
 * longer holds. {@code pathStyleAccess} is part of the same legacy and should not appear
 * here either — it is an S3-family endpoint concern that belongs in the provider
 * implementations, not in generic URI parsing.
 *
 * <p>FIXME: the input is a plain {@code String} that is then split on {@code '/'} below,
 * so URI escaping must already have been applied before a value can be used as input here
 * (see the note on {@link #parse}). An interface-based design should take a properly
 * typed/encoded URI instead of a raw string.
 */
public final class ObjectStorageUri {

    private final String bucket;
    private final String key;

    private ObjectStorageUri(String bucket, String key) {
        this.bucket = bucket;
        this.key = key;
    }

    /**
     * Parses URIs of the form: scheme://bucket/key.
     *
     * <p>When {@code pathStyleAccess} is {@code true} <strong>and</strong> the scheme is
     * {@code http} or {@code https}, the URI is treated as path-style:
     * {@code scheme://endpoint/bucket/key}. The first path segment after the host is the
     * bucket, the remainder is the key. For all other schemes the first authority
     * component is always treated as the bucket, regardless of {@code pathStyleAccess}.
     *
     * <p><strong>Implicit dependency:</strong> {@code path} is parsed by literal string
     * splitting and is <em>not</em> percent-decoded. Callers must pass an already-escaped URI;
     * any {@code /} in a bucket or key must be encoded by the caller, otherwise it will be
     * mistaken for a path separator. The returned {@code bucket}/{@code key} preserve the
     * original (escaped) form.
     */
    public static ObjectStorageUri parse(String path, boolean pathStyleAccess) {
        if (path == null) {
            throw new IllegalArgumentException("Object storage path must not be null");
        }
        int schemeEnd = path.indexOf("://");
        if (schemeEnd < 0) {
            throw new IllegalArgumentException("Cannot parse object storage URI without scheme: " + path);
        }
        String scheme = path.substring(0, schemeEnd);
        String withoutScheme = path.substring(schemeEnd + 3);

        boolean httpScheme = "http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme);
        if (pathStyleAccess && httpScheme) {
            int hostEnd = withoutScheme.indexOf('/');
            if (hostEnd < 0) {
                throw new IllegalArgumentException(
                        "Path-style URI is missing bucket segment: " + path);
            }
            String afterHost = withoutScheme.substring(hostEnd + 1);
            while (afterHost.startsWith("/")) {
                afterHost = afterHost.substring(1);
            }
            int bucketEnd = afterHost.indexOf('/');
            if (bucketEnd < 0) {
                if (afterHost.isEmpty()) {
                    throw new IllegalArgumentException(
                            "Path-style URI is missing bucket segment: " + path);
                }
                return new ObjectStorageUri(afterHost, "");
            }
            String rawKey = afterHost.substring(bucketEnd + 1);
            while (rawKey.startsWith("/")) {
                rawKey = rawKey.substring(1);
            }
            return new ObjectStorageUri(afterHost.substring(0, bucketEnd), rawKey);
        }

        int slashIdx = withoutScheme.indexOf('/');
        if (slashIdx < 0) {
            return new ObjectStorageUri(withoutScheme, "");
        }
        String rawKey = withoutScheme.substring(slashIdx + 1);
        while (rawKey.startsWith("/")) {
            rawKey = rawKey.substring(1);
        }
        return new ObjectStorageUri(withoutScheme.substring(0, slashIdx), rawKey);
    }

    public String bucket() {
        return bucket;
    }

    public String key() {
        return key;
    }
}
