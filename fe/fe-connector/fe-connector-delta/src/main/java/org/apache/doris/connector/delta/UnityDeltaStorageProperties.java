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

package org.apache.doris.connector.delta;

import io.unitycatalog.client.delta.model.DeltaCredentialOperation;
import io.unitycatalog.client.delta.model.DeltaCredentialsResponse;
import io.unitycatalog.client.delta.model.DeltaStorageCredential;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.internal.DeltaStorageCredentialUtil;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** Converts UC Delta credentials into the properties consumed by Doris BE readers. */
final class UnityDeltaStorageProperties {
    private static final String S3_ENDPOINT = DeltaStorageProperties.S3_ENDPOINT;
    private static final String S3_REGION = DeltaStorageProperties.S3_REGION;
    private static final String S3_ACCESS_KEY = DeltaStorageProperties.S3_ACCESS_KEY;
    private static final String S3_SECRET_KEY = DeltaStorageProperties.S3_SECRET_KEY;
    private static final String S3_TOKEN = DeltaStorageProperties.S3_TOKEN;
    private static final String STORAGE_PROVIDER = "provider";
    private static final String AZURE_PROVIDER = "AZURE";

    private UnityDeltaStorageProperties() {
    }

    static Map<String, String> toBackendProperties(String location,
            DeltaCredentialsResponse response, Map<String, String> catalogProperties) {
        return toBackendProperties(location, response, catalogProperties, null);
    }

    static Map<String, String> toBackendProperties(String location,
            DeltaCredentialsResponse response, Map<String, String> catalogProperties,
            DeltaCredentialOperation expectedOperation) {
        URI locationUri = URI.create(location);
        if ("file".equalsIgnoreCase(locationUri.getScheme())) {
            return Map.of();
        }

        if (response == null || response.getStorageCredentials() == null
                || response.getStorageCredentials().isEmpty()) {
            throw new IllegalArgumentException(
                    "Unity Catalog returned no storage credentials for " + location);
        }

        DeltaStorageCredential credential = DeltaStorageCredentialUtil.selectForLocation(
                location, response.getStorageCredentials());
        if (credential.getOperation() == null) {
            throw new IllegalArgumentException(
                    "Unity Catalog returned a storage credential without an operation for "
                            + credential.getPrefix());
        }
        if (expectedOperation != null && credential.getOperation() != expectedOperation) {
            throw new IllegalArgumentException(
                    "Unity Catalog returned a " + credential.getOperation()
                            + " credential for a " + expectedOperation + " operation");
        }
        long minimumLifetimeMs = DeltaConnectorProperties.positiveLongProperty(
                catalogProperties,
                DeltaConnectorProperties.UNITY_CREDENTIAL_MIN_LIFETIME_MS,
                DeltaConnectorProperties.DEFAULT_UNITY_CREDENTIAL_MIN_LIFETIME_MS);
        if (credential.getExpirationTimeMs() == null) {
            throw new IllegalArgumentException(
                    "Unity Catalog returned a storage credential without an expiration time for "
                            + credential.getPrefix());
        }
        if (credential.getExpirationTimeMs() <= System.currentTimeMillis() + minimumLifetimeMs) {
            throw new IllegalArgumentException(
                    "Unity Catalog returned a storage credential with less than "
                            + minimumLifetimeMs + " ms remaining for "
                            + credential.getPrefix());
        }
        TemporaryCredentials temporary =
                DeltaStorageCredentialUtil.toTemporaryCredentials(credential);
        if (temporary.getAwsTempCredentials() != null) {
            return awsProperties(temporary.getAwsTempCredentials(), credential, catalogProperties);
        }
        if (temporary.getAzureUserDelegationSas() != null) {
            String accountHost = locationUri.getHost();
            if (accountHost == null || accountHost.isEmpty()) {
                throw new IllegalArgumentException(
                        "Azure Delta location does not contain a storage account host");
            }
            String container = locationUri.getUserInfo();
            if (container == null || container.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "Azure Delta location does not contain a storage container");
            }
            String sasToken = temporary.getAzureUserDelegationSas().getSasToken();
            if (sasToken == null || sasToken.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "Unity Catalog returned an empty Azure SAS token for " + accountHost);
            }
            Map<String, String> properties = new LinkedHashMap<>();
            // FE's Kernel engine receives the Hadoop SAS configuration from the official
            // Unity Hadoop helper. BE native Parquet scans use the existing object-storage
            // client, so pass the same vended SAS through its provider/token contract.
            properties.put(STORAGE_PROVIDER, AZURE_PROVIDER);
            properties.put(S3_ENDPOINT, blobEndpoint(accountHost));
            properties.put(S3_REGION, "azure");
            properties.put(S3_TOKEN, sasToken);
            properties.put(DeltaStorageProperties.S3_TOKEN_EXPIRATION_TIME_MS,
                    String.valueOf(credential.getExpirationTimeMs()));
            return properties;
        }
        if (temporary.getGcpOauthToken() != null) {
            String gcsOauthToken = temporary.getGcpOauthToken().getOauthToken();
            if (gcsOauthToken == null || gcsOauthToken.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "Unity Catalog returned an empty GCS OAuth token");
            }
            return gcsProperties(locationUri, gcsOauthToken, credential, catalogProperties);
        }
        throw new UnsupportedOperationException(
                "Unity Catalog returned an unsupported temporary credential for " + location);
    }

    /**
     * Native BE has no GCS OAuth client. Use its range-capable HTTP reader against the GCS
     * HTTPS object endpoint while FE Delta Kernel continues to use the official gs filesystem.
     */
    private static Map<String, String> gcsProperties(URI locationUri, String oauthToken,
            DeltaStorageCredential credential, Map<String, String> catalogProperties) {
        if (!"gs".equalsIgnoreCase(locationUri.getScheme())) {
            throw new IllegalArgumentException(
                    "Unity Catalog returned a GCS credential for a non-GCS Delta location");
        }
        String endpoint = gcsEndpoint(catalogProperties);
        String bucket = locationUri.getHost();
        if (bucket == null || bucket.isEmpty()) {
            throw new IllegalArgumentException("GCS Delta location does not contain a bucket");
        }
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("provider", DeltaStorageProperties.GCS_PROVIDER);
        properties.put("uri", stripTrailingSlash(endpoint));
        properties.put("http.header.Authorization", "Bearer " + oauthToken);
        properties.put(DeltaStorageProperties.S3_TOKEN_EXPIRATION_TIME_MS,
                String.valueOf(credential.getExpirationTimeMs()));
        return properties;
    }

    static String toBackendPath(String path, Map<String, String> catalogProperties) {
        URI location = URI.create(path);
        if (!"gs".equalsIgnoreCase(location.getScheme())) {
            return path;
        }
        String endpoint = gcsEndpoint(catalogProperties);
        String bucket = location.getHost();
        if (bucket == null || bucket.isEmpty()) {
            throw new IllegalArgumentException("GCS Delta path does not contain a bucket");
        }
        return stripTrailingSlash(endpoint) + "/" + bucket
                + (location.getRawPath() == null ? "" : location.getRawPath());
    }

    static DeltaScanFile toBackendScanFile(
            DeltaScanFile file, Map<String, String> catalogProperties) {
        if (!isGcsPath(file.getPath())) {
            return file;
        }
        DeltaDeletionVector deletionVector = file.getDeletionVector();
        if (deletionVector != null && "p".equals(deletionVector.getStorageType())) {
            deletionVector = new DeltaDeletionVector(deletionVector.getStorageType(),
                    toBackendPath(deletionVector.getPathOrInlineDv(), catalogProperties),
                    deletionVector.getOffset(), deletionVector.getSizeInBytes(),
                    deletionVector.getCardinality());
        }
        String tablePath = file.getTablePath() == null ? null
                : toBackendPath(file.getTablePath(), catalogProperties);
        return new DeltaScanFile(toBackendPath(file.getPath(), catalogProperties),
                file.getSize(), file.getModificationTime(), file.getPartitionValues(),
                deletionVector, tablePath);
    }

    private static boolean isGcsPath(String path) {
        return "gs".equalsIgnoreCase(URI.create(path).getScheme());
    }

    private static String gcsEndpoint(Map<String, String> catalogProperties) {
        String endpoint = DeltaStorageProperties.firstNonBlank(catalogProperties,
                "gcs.endpoint", "gs.endpoint");
        if (endpoint == null) {
            endpoint = "https://storage.googleapis.com";
        }
        URI endpointUri = URI.create(endpoint);
        if (!"https".equalsIgnoreCase(endpointUri.getScheme())) {
            throw new IllegalArgumentException("GCS OAuth endpoint must use HTTPS");
        }
        if (endpointUri.getHost() == null || endpointUri.getHost().isEmpty()
                || endpointUri.getUserInfo() != null || endpointUri.getQuery() != null
                || endpointUri.getFragment() != null) {
            throw new IllegalArgumentException("GCS endpoint must contain only a host and path");
        }
        return endpoint;
    }

    private static String stripTrailingSlash(String value) {
        String result = value;
        while (result.endsWith("/") && result.length() > 1) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

    /** The BE Azure object client uses Blob APIs; map an ABFS DFS host to its Blob endpoint. */
    private static String blobEndpoint(String accountHost) {
        String lowerHost = accountHost.toLowerCase(Locale.ROOT);
        int dfsMarker = lowerHost.indexOf(".dfs.");
        if (dfsMarker < 0) {
            return accountHost;
        }
        return accountHost.substring(0, dfsMarker) + ".blob"
                + accountHost.substring(dfsMarker + 4);
    }

    private static Map<String, String> awsProperties(
            AwsCredentials credentials, DeltaStorageCredential credential,
            Map<String, String> catalogProperties) {
        String region = DeltaStorageProperties.firstNonBlank(catalogProperties,
                S3_REGION, "s3.region", "client.region", "aws.region");
        if (region == null) {
            region = UnityDeltaClient.credentialRegion(credential.getConfig());
        }
        if (region == null) {
            throw new IllegalArgumentException(
                    "Unity Delta credentials for S3 do not contain client.region; configure "
                            + "'s3.region' so Doris BE can create a native S3 client");
        }
        String endpoint = DeltaStorageProperties.firstNonBlank(catalogProperties,
                S3_ENDPOINT, "s3.endpoint", "aws.endpoint");
        if (endpoint == null) {
            endpoint = "s3." + region + ".amazonaws.com";
        }

        Map<String, String> properties = new LinkedHashMap<>();
        properties.put(S3_ENDPOINT, endpoint);
        properties.put(S3_REGION, region);
        properties.put(S3_ACCESS_KEY, requireCredentialValue(
                credentials.getAccessKeyId(), "access key"));
        properties.put(S3_SECRET_KEY, requireCredentialValue(
                credentials.getSecretAccessKey(), "secret key"));
        properties.put(S3_TOKEN, requireCredentialValue(
                credentials.getSessionToken(), "session token"));
        properties.put(DeltaStorageProperties.S3_TOKEN_EXPIRATION_TIME_MS,
                String.valueOf(credential.getExpirationTimeMs()));
        return properties;
    }

    private static String requireCredentialValue(String value, String credentialName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Unity Catalog returned an empty AWS " + credentialName);
        }
        return value;
    }
}
