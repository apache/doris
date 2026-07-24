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

import java.util.Set;

/**
 * Parsed object-storage URI (bucket + key). Has no fe-core dependency.
 *
 * <p>This is the single shared parser for every S3-compatible provider (S3, OSS, COS, OBS).
 * The bucket/key extraction mechanics are genuinely uniform across them; the per-provider
 * variation is supplied by the caller: the {@code pathStyleAccess} flag and the set of accepted
 * schemes. The provider itself is selected from connection properties (endpoint domain / explicit
 * storage type), not from the URI scheme, but each provider still constrains which schemes it will
 * accept via {@link #parse(String, boolean, Set)} — so COS accepts {@code s3}/{@code s3a}/{@code
 * cos}/{@code cosn} but rejects {@code oss}. Each provider owns its accepted-scheme set on its
 * {@code S3CompatibleFileSystemProperties}; this class only enforces it.
 *
 * <p>{@code pathStyleAccess} stays here because parsing a path-style HTTP(S) endpoint URL
 * genuinely needs it to locate the bucket segment; it is independent of whether a provider's
 * client performs path-style <em>addressing</em> (e.g. COS parses path-style URLs but its SDK
 * always issues virtual-hosted requests).
 *
 * <p><strong>Encoding:</strong> the input is a plain {@code String} split on {@code '/'} (see
 * {@link #parse}); URI escaping must already have been applied by the caller. Any {@code /}
 * inside a bucket or key must be percent-encoded, otherwise it is mistaken for a separator.
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
        return parse(path, pathStyleAccess, null);
    }

    /**
     * Same as {@link #parse(String, boolean)} but validates the URI scheme against the set of
     * schemes the calling provider accepts. A scheme not in {@code supportedSchemes} (matched
     * case-insensitively) is rejected, so e.g. a COS provider refuses an {@code oss://} URI.
     * Passing {@code null} or an empty set skips scheme validation.
     *
     * <p>{@code http}/{@code https} are always accepted regardless of {@code supportedSchemes}:
     * they are the endpoint-URL form (e.g. a TVF such as {@code s3()} pointed at a path-style
     * endpoint), not a provider-identity scheme. {@code supportedSchemes} therefore stays explicit
     * (e.g. {@code s3}/{@code oss}/{@code cos}) for catalog scheme-to-storage routing and omits
     * {@code http}/{@code https} on purpose.
     */
    public static ObjectStorageUri parse(String path, boolean pathStyleAccess, Set<String> supportedSchemes) {
        if (path == null) {
            throw new IllegalArgumentException("Object storage path must not be null");
        }
        if (supportedSchemes != null && !supportedSchemes.isEmpty()) {
            int schemeEnd = path.indexOf("://");
            if (schemeEnd < 0) {
                throw new IllegalArgumentException("Cannot parse object storage URI without scheme: " + path);
            }
            String scheme = path.substring(0, schemeEnd).toLowerCase();
            // http/https is the endpoint-URL form (e.g. TVF s3() with a path-style endpoint),
            // not a provider-identity scheme, so it bypasses the supportedSchemes check. The set
            // stays explicit (s3/oss/cos/...) for catalog scheme-to-storage routing; the actual
            // bucket/key extraction for http(s) is handled by the path-style branch in doParse.
            boolean httpEndpoint = scheme.equals("http") || scheme.equals("https");
            if (!httpEndpoint && !supportedSchemes.contains(scheme)) {
                throw new IllegalArgumentException("Unsupported scheme '" + scheme
                        + "' for this storage; supported schemes: " + supportedSchemes);
            }
        }
        return doParse(path, pathStyleAccess);
    }

    private static ObjectStorageUri doParse(String path, boolean pathStyleAccess) {
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
