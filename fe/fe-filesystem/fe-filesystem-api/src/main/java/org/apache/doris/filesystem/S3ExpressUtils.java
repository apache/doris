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

package org.apache.doris.filesystem;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Shared syntax helpers for AWS S3 Express directory buckets. */
public final class S3ExpressUtils {

    private static final Pattern DIRECTORY_BUCKET_PATTERN = Pattern.compile(
            "^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?--([a-z0-9-]+-az[0-9]+)--x-s3$");

    private S3ExpressUtils() {
    }

    /** Returns the Zone ID encoded in a complete directory-bucket name. */
    public static Optional<String> directoryBucketZoneId(String bucket) {
        if (bucket == null) {
            return Optional.empty();
        }
        Matcher matcher = DIRECTORY_BUCKET_PATTERN.matcher(bucket);
        return matcher.matches() ? Optional.of(matcher.group(1)) : Optional.empty();
    }

    public static boolean isDirectoryBucket(String bucket) {
        return directoryBucketZoneId(bucket).isPresent();
    }

    /**
     * Converts an arbitrary object-key prefix to the containing directory prefix accepted by
     * directory-bucket ListObjectsV2. Empty and already slash-terminated prefixes are unchanged.
     */
    public static String directoryPrefix(String keyPrefix) {
        if (keyPrefix == null || keyPrefix.isEmpty() || keyPrefix.endsWith("/")) {
            return keyPrefix;
        }
        int slash = keyPrefix.lastIndexOf('/');
        return slash < 0 ? "" : keyPrefix.substring(0, slash + 1);
    }
}
