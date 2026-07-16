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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.regex.Pattern;

/** Request-level S3 capabilities derived from the bucket and service endpoint. */
public final class S3BucketCapabilities {

    public enum BucketType {
        GENERAL_PURPOSE,
        DIRECTORY
    }

    public enum EndpointMode {
        EXPLICIT_OVERRIDE,
        AWS_SDK_RULES
    }

    public enum ChecksumPolicy {
        CONTENT_MD5,
        CRC32C
    }

    private static final Pattern DIRECTORY_BUCKET_PATTERN = Pattern.compile(
            "^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?--[a-z0-9]+-az[0-9]+--x-s3$");

    private final BucketType bucketType;
    private final EndpointMode endpointMode;
    private final ChecksumPolicy checksumPolicy;
    private final boolean officialAwsService;
    private final String directoryBucketZone;

    private S3BucketCapabilities(BucketType bucketType, EndpointMode endpointMode,
            ChecksumPolicy checksumPolicy, boolean officialAwsService,
            String directoryBucketZone) {
        this.bucketType = bucketType;
        this.endpointMode = endpointMode;
        this.checksumPolicy = checksumPolicy;
        this.officialAwsService = officialAwsService;
        this.directoryBucketZone = directoryBucketZone;
    }

    public static S3BucketCapabilities resolve(String bucket, String endpoint) {
        boolean officialAws = isAwsS3Endpoint(endpoint);
        if (!isDirectoryBucketName(bucket) || !officialAws) {
            return new S3BucketCapabilities(BucketType.GENERAL_PURPOSE,
                    EndpointMode.EXPLICIT_OVERRIDE, ChecksumPolicy.CONTENT_MD5, officialAws, "");
        }
        EndpointMode mode = isExplicitAwsEndpoint(endpoint)
                ? EndpointMode.EXPLICIT_OVERRIDE : EndpointMode.AWS_SDK_RULES;
        return new S3BucketCapabilities(BucketType.DIRECTORY, mode,
                ChecksumPolicy.CRC32C, true, bucketZone(bucket));
    }

    public static boolean isDirectoryBucketName(String bucket) {
        return bucket != null && bucket.length() >= 3 && bucket.length() <= 63
                && DIRECTORY_BUCKET_PATTERN.matcher(bucket).matches();
    }

    /** Returns true when an S3-style URI targets an AWS Directory Bucket. */
    public static boolean isDirectoryBucketUri(String location, String endpoint) {
        if (location == null || location.isBlank()) {
            return false;
        }
        try {
            URI uri = new URI(location);
            String scheme = uri.getScheme();
            if (scheme == null || !(scheme.equalsIgnoreCase("s3")
                    || scheme.equalsIgnoreCase("s3a") || scheme.equalsIgnoreCase("s3n"))) {
                return false;
            }
            String bucket = uri.getHost() == null ? uri.getAuthority() : uri.getHost();
            return resolve(bucket, endpoint).isDirectoryBucket();
        } catch (URISyntaxException e) {
            return false;
        }
    }

    public static boolean isAwsS3Endpoint(String endpoint) {
        if (endpoint == null || endpoint.isBlank()) {
            // No override means that the AWS SDK resolves the public AWS endpoint from region.
            return true;
        }
        String host = endpointHost(endpoint);
        if (host.isEmpty()) {
            return false;
        }
        boolean awsDns = host.endsWith(".amazonaws.com")
                || host.endsWith(".amazonaws.com.cn") || host.endsWith(".api.aws");
        if (!awsDns) {
            return false;
        }
        return host.startsWith("s3.") || host.startsWith("s3-")
                || host.startsWith("s3express-") || host.startsWith("s3express-control.")
                || host.contains(".s3.") || host.contains(".s3-")
                || host.contains(".s3express-");
    }

    public static boolean isS3ExpressControlEndpoint(String endpoint) {
        String host = endpointHost(endpoint);
        return host.startsWith("s3express-control.") || host.contains(".s3express-control.");
    }

    public static boolean isS3ExpressZonalEndpoint(String endpoint) {
        if (isS3ExpressControlEndpoint(endpoint)) {
            return false;
        }
        String host = endpointHost(endpoint);
        return host.startsWith("s3express-") || host.contains(".s3express-");
    }

    public void validateDirectoryConfiguration(String endpoint, String region, boolean usePathStyle) {
        if (officialAwsService && isS3ExpressControlEndpoint(endpoint)) {
            throw new IllegalArgumentException(
                    "s3express-control endpoint cannot be used for object operations");
        }
        if (officialAwsService && isS3ExpressZonalEndpoint(endpoint) && !isDirectoryBucket()) {
            throw new IllegalArgumentException(
                    "An S3 Express endpoint requires a valid AWS Directory Bucket name");
        }
        if (!isDirectoryBucket()) {
            return;
        }
        if (region == null || region.isBlank()) {
            throw new IllegalArgumentException("AWS Directory Bucket requires AWS_REGION");
        }
        if (usePathStyle) {
            throw new IllegalArgumentException(
                    "Path-style addressing is not supported for AWS Directory Bucket");
        }
        if (endpoint != null && endpoint.toLowerCase(Locale.ROOT).startsWith("http://")) {
            throw new IllegalArgumentException("AWS Directory Bucket requires HTTPS");
        }
        if (endpointMode != EndpointMode.AWS_SDK_RULES) {
            throw new IllegalArgumentException(
                    "AWS Directory Bucket requires a standard regional or zonal S3 endpoint");
        }
        String endpointRegion = endpointRegion(endpoint);
        if (!endpointRegion.isEmpty() && !endpointRegion.equals(region)) {
            throw new IllegalArgumentException("Configured endpoint region " + endpointRegion
                    + " does not match AWS_REGION " + region + " for Directory Bucket");
        }
        String endpointZone = endpointZone(endpoint);
        if (!endpointZone.isEmpty() && !endpointZone.equals(directoryBucketZone)) {
            throw new IllegalArgumentException("Configured endpoint zone " + endpointZone
                    + " does not match Directory Bucket zone " + directoryBucketZone);
        }
    }

    public BucketType bucketType() {
        return bucketType;
    }

    public EndpointMode endpointMode() {
        return endpointMode;
    }

    public ChecksumPolicy checksumPolicy() {
        return checksumPolicy;
    }

    public boolean officialAwsService() {
        return officialAwsService;
    }

    public boolean isDirectoryBucket() {
        return bucketType == BucketType.DIRECTORY;
    }

    public boolean supportsStartAfter() {
        return !isDirectoryBucket();
    }

    public boolean listIsLexicographic() {
        return !isDirectoryBucket();
    }

    public boolean supportsVersioning() {
        return !isDirectoryBucket();
    }

    public boolean supportsPresign() {
        return !isDirectoryBucket();
    }

    private static boolean isExplicitAwsEndpoint(String endpoint) {
        String host = endpointHost(endpoint);
        return host.contains("fips") || host.contains("dualstack")
                || host.contains("accelerate") || host.contains("vpce")
                || isS3ExpressControlEndpoint(endpoint);
    }

    private static String bucketZone(String bucket) {
        if (bucket == null || !bucket.endsWith("--x-s3")) {
            return "";
        }
        int separator = bucket.lastIndexOf("--", bucket.length() - "--x-s3".length() - 1);
        return separator < 0 ? "" : bucket.substring(separator + 2, bucket.length() - "--x-s3".length());
    }

    private static String endpointZone(String endpoint) {
        String host = endpointHost(endpoint);
        int marker = host.indexOf("s3express-");
        if (marker < 0 || host.startsWith("s3express-control", marker)) {
            return "";
        }
        int begin = marker + "s3express-".length();
        int end = host.indexOf('.', begin);
        return end < 0 ? host.substring(begin) : host.substring(begin, end);
    }

    private static String endpointRegion(String endpoint) {
        String host = endpointHost(endpoint);
        if (host.isEmpty() || "s3.amazonaws.com".equals(host)) {
            return "";
        }
        int control = host.indexOf("s3express-control.");
        if (control >= 0) {
            return nextHostLabel(host, control + "s3express-control.".length());
        }
        int express = host.indexOf("s3express-");
        if (express >= 0) {
            int dot = host.indexOf('.', express);
            return dot < 0 ? "" : nextHostLabel(host, dot + 1);
        }
        int standard = host.indexOf("s3.");
        if (standard >= 0) {
            int begin = standard + "s3.".length();
            if (host.startsWith("dualstack.", begin)) {
                begin += "dualstack.".length();
            }
            String value = nextHostLabel(host, begin);
            return "amazonaws".equals(value) ? "" : value;
        }
        if (host.startsWith("s3-")) {
            return nextHostLabel(host, "s3-".length());
        }
        return "";
    }

    private static String nextHostLabel(String host, int begin) {
        int end = host.indexOf('.', begin);
        return end < 0 ? host.substring(begin) : host.substring(begin, end);
    }

    private static String endpointHost(String endpoint) {
        if (endpoint == null || endpoint.isBlank()) {
            return "";
        }
        String value = endpoint.contains("://") ? endpoint : "https://" + endpoint;
        try {
            String host = new URI(value).getHost();
            return host == null ? "" : host.toLowerCase(Locale.ROOT);
        } catch (URISyntaxException e) {
            return "";
        }
    }
}
