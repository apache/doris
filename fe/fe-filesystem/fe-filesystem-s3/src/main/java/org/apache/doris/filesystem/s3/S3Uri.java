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

/**
 * Parsed S3/S3A URI. Extracts bucket and key without any fe-core dependency.
 */
public final class S3Uri {

    private final String bucket;
    private final String key;

    private S3Uri(String bucket, String key) {
        this.bucket = bucket;
        this.key = key;
    }

    /**
     * Parses URIs of the form: s3://bucket/key, s3a://bucket/key, s3n://bucket/key.
     *
     * <p>When {@code pathStyleAccess} is {@code true} <strong>and</strong> the scheme is
     * {@code http} or {@code https}, the URI is treated as path-style:
     * {@code scheme://endpoint/bucket/key}. The first path segment after the host is the
     * bucket, the remainder is the key. For all other schemes (including {@code s3://},
     * {@code s3a://}, {@code s3n://}) the first authority component is always treated as
     * the bucket, regardless of {@code pathStyleAccess}.
     */
    public static S3Uri parse(String path, boolean pathStyleAccess) {
        if (path == null) {
            throw new IllegalArgumentException("S3 path must not be null");
        }
        int schemeEnd = path.indexOf("://");
        if (schemeEnd < 0) {
            throw new IllegalArgumentException("Cannot parse S3 URI without scheme: " + path);
        }
        String scheme = path.substring(0, schemeEnd);
        String withoutScheme = path.substring(schemeEnd + 3);

        boolean httpScheme = "http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme);
        if (pathStyleAccess && httpScheme) {
            // Path-style: scheme://host/bucket/key
            int hostEnd = withoutScheme.indexOf('/');
            if (hostEnd < 0) {
                // scheme://host with no bucket
                throw new IllegalArgumentException(
                        "Path-style URI is missing bucket segment: " + path);
            }
            String afterHost = withoutScheme.substring(hostEnd + 1);
            // Strip extra leading slashes between host and bucket.
            while (afterHost.startsWith("/")) {
                afterHost = afterHost.substring(1);
            }
            int bucketEnd = afterHost.indexOf('/');
            if (bucketEnd < 0) {
                if (afterHost.isEmpty()) {
                    throw new IllegalArgumentException(
                            "Path-style URI is missing bucket segment: " + path);
                }
                return new S3Uri(afterHost, "");
            }
            String rawKey = afterHost.substring(bucketEnd + 1);
            while (rawKey.startsWith("/")) {
                rawKey = rawKey.substring(1);
            }
            return new S3Uri(afterHost.substring(0, bucketEnd), rawKey);
        }

        // Virtual-hosted style: scheme://bucket/key
        int slashIdx = withoutScheme.indexOf('/');
        if (slashIdx < 0) {
            return new S3Uri(withoutScheme, "");
        }
        String rawKey = withoutScheme.substring(slashIdx + 1);
        // Strip leading slashes to normalize keys (e.g., "s3://bucket//path" → key "path")
        while (rawKey.startsWith("/")) {
            rawKey = rawKey.substring(1);
        }
        return new S3Uri(withoutScheme.substring(0, slashIdx), rawKey);
    }

    public String bucket() {
        return bucket;
    }

    public String key() {
        return key;
    }
}
