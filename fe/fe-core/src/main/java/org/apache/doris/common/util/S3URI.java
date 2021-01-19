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

package org.apache.doris.common.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.io.FilenameUtils;

import java.util.Set;

/**
 * This class represents a fully qualified location in S3 for input/output
 * operations expressed as as URI.  This implementation is provided to
 * ensure compatibility with Hadoop Path implementations that may introduce
 * encoding issues with native URI implementation.
 */

public class S3URI {
    private static final String SCHEME_DELIM = "://";
    private static final String PATH_DELIM = "/";
    private static final String QUERY_DELIM = "\\?";
    private static final String FRAGMENT_DELIM = "#";
    private static final Set<String> VALID_SCHEMES = ImmutableSet.of("http", "https", "s3", "s3a", "s3n", "bos");

    private final String location;
    private final String bucket;
    private final String key;

    // Since s3 does not support wildcards, we can only use wildcard filtering to list all possible files.
    // searchPath is the parent path of all possible files
    private final String searchPath;
    // normalized wildcard
    private final String wildcard;

    /**
     * Creates a new S3URI based on the bucket and key parsed from the location as defined in:
     * https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html#access-bucket-intro
     * <p>
     * Supported access styles are Virtual Hosted addresses and s3://... URIs with additional
     * 's3n' and 's3a' schemes supported for backwards compatibility.
     *
     * @param location fully qualified URI
     */
    public S3URI(String location) {
        Preconditions.checkNotNull(location, "Location cannot be null.");

        this.location = location;
        String[] schemeSplit = location.split(SCHEME_DELIM);
        Preconditions.checkState(schemeSplit.length == 2, "Invalid S3 URI: %s", location);

        String scheme = schemeSplit[0];
        Preconditions.checkState(VALID_SCHEMES.contains(scheme.toLowerCase()), "Invalid scheme: %s", scheme);

        String[] authoritySplit = schemeSplit[1].split(PATH_DELIM, 2);
        Preconditions.checkState(authoritySplit.length == 2, "Invalid S3 URI: %s", location);
        Preconditions.checkState(!authoritySplit[1].trim().isEmpty(), "Invalid S3 key: %s", location);
        this.bucket = authoritySplit[0];

        // Strip query and fragment if they exist
        String path = authoritySplit[1];
        path = path.split(QUERY_DELIM)[0];
        path = path.split(FRAGMENT_DELIM)[0];
        key = path;
        if (key.endsWith("/*")) {
            searchPath = key.substring(0, key.length() - 1);
            wildcard = key;
        } else if (key.endsWith("*")) {
            int lastDelim = key.lastIndexOf(PATH_DELIM);
            searchPath = key.substring(0, lastDelim > 0 ? lastDelim : 0) + PATH_DELIM;
            wildcard = key;
        } else if (key.endsWith("/")) {
            searchPath = key;
            wildcard = key + "*";
        } else {
            searchPath = key + PATH_DELIM;
            wildcard = searchPath + "*";
        }
    }

    /**
     * @return S3 bucket
     */
    public String getBucket() {
        return bucket;
    }

    /**
     * @return S3 key
     */
    public String getKey() {
        return key;
    }

    /*
     * @return original, unmodified location
     */
    public String getLocation() {
        return location;
    }


    public String getSearchPath() {
        return searchPath;
    }

    public String getWildcard() {
        return wildcard;
    }

    public String filterFile(String fileName) {
        if (FilenameUtils.wildcardMatch(fileName, wildcard)) {
            return fileName.substring(searchPath.length());
        }
        return null;
    }

    public String fullPath(String fileName) {
        String base = location;
        if (base.endsWith("*")) {
            base = base.substring(0, base.length() - 1);
        }
        if (!base.endsWith("/")) {
            base = base + "/";
        }
        return base + fileName;
    }

    @Override
    public String toString() {
        return location;
    }
}