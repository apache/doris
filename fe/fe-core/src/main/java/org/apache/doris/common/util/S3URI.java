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

import org.apache.parquet.glob.GlobExpander;

import java.util.List;
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

    private String scheme;
    private final String location;
    private final String virtualBucket;
    private final String bucket;
    private final String key;
    private boolean forceHosted;

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
        this(location, false);
    }

    public S3URI(String location, boolean forceHosted) {
        Preconditions.checkNotNull(location, "Location cannot be null.");
        this.location = location;
        this.forceHosted = forceHosted;
        String[] schemeSplit = location.split(SCHEME_DELIM);
        Preconditions.checkState(schemeSplit.length == 2, "Invalid S3 URI: %s", location);

        this.scheme = schemeSplit[0];
        Preconditions.checkState(VALID_SCHEMES.contains(scheme.toLowerCase()), "Invalid scheme: %s", scheme);
        String[] authoritySplit = schemeSplit[1].split(PATH_DELIM, 2);
        Preconditions.checkState(authoritySplit.length == 2, "Invalid S3 URI: %s", location);
        Preconditions.checkState(!authoritySplit[1].trim().isEmpty(), "Invalid S3 key: %s", location);
        // Strip query and fragment if they exist
        String path = authoritySplit[1];
        path = path.split(QUERY_DELIM)[0];
        path = path.split(FRAGMENT_DELIM)[0];
        if (forceHosted) {
            this.virtualBucket = authoritySplit[0];
            String[] paths = path.split("/", 2);
            this.bucket = paths[0];
            if (paths.length > 1) {
                key = paths[1];
            } else {
                key = "";
            }
        } else {
            this.virtualBucket = "";
            this.bucket = authoritySplit[0];
            key = path;
        }
    }

    public List<String> expand(String path) {
        return GlobExpander.expand(path);
    }

    public String getScheme() {
        return this.scheme;
    }

    public String getBucketScheme() {
        return scheme + "://" + bucket;
    }

    public String getVirtualBucket() {
        return virtualBucket;
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

    @Override
    public String toString() {
        return location;
    }
}

