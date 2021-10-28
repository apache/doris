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

import org.apache.doris.common.UserException;

import com.google.common.collect.ImmutableSet;

import org.apache.parquet.Strings;
import org.apache.parquet.glob.GlobExpander;

import java.net.URI;
import java.net.URISyntaxException;
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
    private boolean forceVirtualHosted;

    /**
     * Creates a new S3URI based on the bucket and key parsed from the location as defined in:
     * https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html#access-bucket-intro
     * <p>
     * Supported access styles are Virtual Hosted addresses and s3://... URIs with additional
     * 's3n' and 's3a' schemes supported for backwards compatibility.
     *
     * @param location fully qualified URI
     */

    public static S3URI create(String location) throws UserException {
        return create(location, false);
    }

    public static S3URI create(String location, boolean forceVirtualHosted) throws UserException {
        S3URI s3URI = new S3URI(location, forceVirtualHosted);
        return s3URI;
    }

    private S3URI(String location, boolean forceVirtualHosted) throws UserException {
        if (Strings.isNullOrEmpty(location)) {
            throw new UserException("s3 location can not be null");
        }

        try {
            // the location need to be normalized to eliminate double "/", or the hadoop aws api
            // won't handle it correctly.
            this.location = new URI(location).normalize().toString();
        } catch (URISyntaxException e) {
            throw new UserException("Invalid s3 uri: " + e.getMessage());
        }

        this.forceVirtualHosted = forceVirtualHosted;
        String[] schemeSplit = this.location.split(SCHEME_DELIM);
        if (schemeSplit.length != 2) {
            throw new UserException("Invalid s3 uri: " + this.location);
        }

        this.scheme = schemeSplit[0];
        if (!VALID_SCHEMES.contains(scheme.toLowerCase())) {
            throw new UserException("Invalid scheme: " + this.location);
        }

        String[] authoritySplit = schemeSplit[1].split(PATH_DELIM, 2);
        if (authoritySplit.length != 2) {
            throw new UserException("Invalid s3 uri: " + this.location);
        }
        if (authoritySplit[1].trim().isEmpty()) {
            throw new UserException("Invalid s3 key: " + this.location);
        }

        // Strip query and fragment if they exist
        String path = authoritySplit[1];
        path = path.split(QUERY_DELIM)[0];
        path = path.split(FRAGMENT_DELIM)[0];
        if (this.forceVirtualHosted) {
            // If forceVirtualHosted is true, the s3 client will NOT automatically convert to virtual-hosted style.
            // So we do some convert manually. Eg:
            //          endpoint:           http://cos.ap-beijing.myqcloud.com
            //          bucket/path:        my_bucket/file.txt
            // `virtualBucket` will be "my_bucket"
            // `bucket` will be `file.txt`
            // So that when assembling the real endpoint will be: http://my_bucket.cos.ap-beijing.myqcloud.com/file.txt
            this.virtualBucket = authoritySplit[0];
            String[] paths = path.split("/", 2);
            this.bucket = paths[0];
            if (paths.length > 1) {
                key = paths[1];
            } else {
                key = "";
            }
        } else {
            // If forceVirtualHosted is false, let the s3 client to determine how to covert endpoint, eg:
            // For s3 endpoint(start with "s3."), it will convert to virtual-hosted style.
            // For others, keep as it is (maybe path-style, maybe virtual-hosted style.)
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

