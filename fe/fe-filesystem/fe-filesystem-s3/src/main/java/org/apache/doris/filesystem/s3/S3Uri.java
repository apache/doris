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
     * Also handles path-style: https://endpoint/bucket/key (when pathStyle=true).
     */
    public static S3Uri parse(String path, boolean pathStyleAccess) {
        if (path == null) {
            throw new IllegalArgumentException("S3 path must not be null");
        }
        // Handle virtual-hosted style: s3://bucket/key, s3a://bucket/key
        int schemeEnd = path.indexOf("://");
        if (schemeEnd >= 0) {
            String withoutScheme = path.substring(schemeEnd + 3);
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
        // Treat bare string as key (shouldn't normally happen)
        throw new IllegalArgumentException("Cannot parse S3 URI without scheme: " + path);
    }

    public String bucket() {
        return bucket;
    }

    public String key() {
        return key;
    }
}
