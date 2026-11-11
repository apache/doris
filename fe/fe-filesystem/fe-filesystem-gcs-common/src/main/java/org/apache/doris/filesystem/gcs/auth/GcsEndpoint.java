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

package org.apache.doris.filesystem.gcs.auth;

import java.net.URI;
import java.util.regex.Pattern;

/** Shared GCS host recognition and native OAuth endpoint validation. */
public final class GcsEndpoint {
    // Endpoint overrides must be service-base hosts; SDK resolvers add the bucket.
    private static final Pattern STORAGE_BASE_HOST = Pattern.compile(
            "(?:storage\\.googleapis\\.com|[a-z0-9-]+-storage\\.googleapis\\.com|"
                    + "storage\\.[a-z0-9-]+\\.rep\\.googleapis\\.com)", Pattern.CASE_INSENSITIVE);
    // Resolved virtual-hosted requests legitimately contain the bucket prefix.
    private static final Pattern STORAGE_HOST = Pattern.compile(
            "(?:[a-z0-9-]+\\.)*" + STORAGE_BASE_HOST.pattern(), Pattern.CASE_INSENSITIVE);

    private GcsEndpoint() {
    }

    /** Identifies GCS by host; authentication-specific restrictions are checked after binding. */
    public static boolean isGcsEndpoint(String endpoint) {
        try {
            String host = parse(endpoint).getHost();
            return host != null && STORAGE_HOST.matcher(host).matches();
        } catch (IllegalArgumentException e) {
            // A malformed URI cannot identify a filesystem provider.
            return false;
        }
    }

    private static URI parse(String endpoint) {
        return URI.create(endpoint.contains("://") ? endpoint : "https://" + endpoint);
    }

    public static URI validateEndpoint(String endpoint) {
        URI uri = parse(endpoint);
        validateRequestUri(uri);
        if (!STORAGE_BASE_HOST.matcher(uri.getHost()).matches()) {
            throw new IllegalArgumentException(
                    "Native GCP OAuth endpoint must be a service-base host without a bucket");
        }
        if (uri.getRawQuery() != null || (!uri.getRawPath().isEmpty() && !"/".equals(uri.getRawPath()))) {
            throw new IllegalArgumentException("Native GCP OAuth endpoint must not contain a path or query");
        }
        return uri;
    }

    public static void validateRequestUri(URI uri) {
        if (!"https".equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null
                || !STORAGE_HOST.matcher(uri.getHost()).matches()
                || (uri.getPort() != -1 && uri.getPort() != 443)
                || uri.getRawUserInfo() != null || uri.getRawFragment() != null) {
            throw new IllegalArgumentException(
                    "Native GCP OAuth requires a trusted Google Cloud Storage HTTPS endpoint on port 443");
        }
    }
}
