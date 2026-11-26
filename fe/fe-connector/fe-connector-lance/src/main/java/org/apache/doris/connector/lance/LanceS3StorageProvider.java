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

package org.apache.doris.connector.lance;

import org.apache.doris.filesystem.properties.S3CompatibleFileSystemProperties;
import org.apache.doris.filesystem.properties.StorageProperties;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * S3-compatible storage, which Lance reaches through object_store's AWS provider.
 *
 * <p>The emitted spelling is the one object_store reports as canonical, because that is what
 * {@code StorageOptions::with_env_s3} looks for before pulling the same option out of the process
 * environment:
 *
 * <pre>
 * // lance-io/src/object_store/providers/aws.rs
 * if let Ok(config_key) = AmazonS3ConfigKey::from_str(&amp;key.to_ascii_lowercase())
 *     &amp;&amp; !self.0.contains_key(config_key.as_ref())   // "aws_access_key_id"
 * </pre>
 *
 * <p>Any other accepted alias leaves that check unsatisfied, so a stray {@code AWS_ACCESS_KEY_ID}
 * in the FE or BE environment is inserted next to the configured value, and object_store's
 * {@code as_s3_options()} then folds both onto one config key and keeps whichever its HashMap
 * yields last - independently per option and per process. Lance's OpenDAL S3 backend accepts these
 * same spellings as serde aliases of its own field names.
 */
final class LanceS3StorageProvider implements LanceStorageProvider {

    static final LanceS3StorageProvider INSTANCE = new LanceS3StorageProvider();

    private static final String ACCESS_KEY_ID = "aws_access_key_id";
    private static final String SECRET_ACCESS_KEY = "aws_secret_access_key";
    private static final String SESSION_TOKEN = "aws_session_token";
    private static final String REGION = "aws_region";
    private static final String ENDPOINT = "aws_endpoint";
    private static final String S3_ENDPOINT = "aws_endpoint_url_s3";
    private static final String VIRTUAL_HOSTED_STYLE = "aws_virtual_hosted_style_request";
    /**
     * Not prefixed: object_store carries this as a shared client option, and reports it as
     * canonical under this name. The spelling buys nothing against the environment here, though -
     * {@code StorageOptions::new} overwrites this key outright from {@code AWS_ALLOW_HTTP} before
     * {@code with_env_s3} runs.
     */
    private static final String ALLOW_HTTP = "allow_http";

    /**
     * Every spelling object_store accepts for the options above, mapped onto the one emitted.
     *
     * <p>Confined to those options on purpose. They are the only ones this class contributes, so
     * they are the only ones a vended option can collide with; anything else a namespace sends is
     * between the namespace and Lance.
     *
     * <p>{@code token} is included because this provider only ever speaks for an S3 dataset, where
     * it is unambiguously the session token. It means a bearer token to object_store's Azure
     * parser, which is why it can only be resolved once the provider is known.
     *
     * <p>{@code aws_endpoint_url_s3} remains a separate coordinate: object_store prefers it over
     * the generic endpoint. After the catalog/vended merge, inference fills a missing service
     * endpoint from the generic one to block an unrelated environment value.
     */
    private static final Map<String, String> CANONICAL_BY_ALIAS = Map.ofEntries(
            Map.entry("access_key_id", ACCESS_KEY_ID),
            Map.entry("aws_access_key_id", ACCESS_KEY_ID),
            Map.entry("secret_access_key", SECRET_ACCESS_KEY),
            Map.entry("aws_secret_access_key", SECRET_ACCESS_KEY),
            Map.entry("session_token", SESSION_TOKEN),
            Map.entry("aws_session_token", SESSION_TOKEN),
            Map.entry("aws_token", SESSION_TOKEN),
            Map.entry("token", SESSION_TOKEN),
            Map.entry("endpoint", ENDPOINT),
            Map.entry("endpoint_url", ENDPOINT),
            Map.entry("aws_endpoint", ENDPOINT),
            Map.entry("aws_endpoint_url", ENDPOINT),
            Map.entry("aws_endpoint_url_s3", S3_ENDPOINT),
            Map.entry("region", REGION),
            Map.entry("aws_region", REGION),
            Map.entry("virtual_hosted_style_request", VIRTUAL_HOSTED_STYLE),
            Map.entry("aws_virtual_hosted_style_request", VIRTUAL_HOSTED_STYLE),
            // OpenDAL's own field name, of which the two above are serde aliases. All three would
            // be the same field supplied more than once, which fails the operator build outright.
            Map.entry("enable_virtual_host_style", VIRTUAL_HOSTED_STYLE),
            Map.entry("allow_http", ALLOW_HTTP),
            Map.entry("aws_allow_http", ALLOW_HTTP));

    private LanceS3StorageProvider() {
    }

    @Override
    public Map<String, String> normalizeDorisStorageOptions(
            String datasetUri, List<StorageProperties> storageProperties) {
        Map<String, String> result = new HashMap<>();
        S3CompatibleFileSystemProperties properties = selectS3Compatible(storageProperties);
        if (properties == null) {
            return result;
        }
        if (properties.hasAssumeRole()) {
            throw new IllegalArgumentException("Lance S3 AssumeRole access is not supported yet");
        }
        LanceStorageOptions.putIfNotEmpty(result, ACCESS_KEY_ID, properties.getAccessKey());
        LanceStorageOptions.putIfNotEmpty(result, SECRET_ACCESS_KEY, properties.getSecretKey());
        // Keep the complete static identity: an absent key lets Lance's with_env_s3() import an
        // unrelated AWS_SESSION_TOKEN from the FE or BE environment. An explicit empty value blocks it.
        String token = properties.getSessionToken();
        result.put(SESSION_TOKEN, token == null ? "" : token);
        // Keep the service endpoint until vending has selected the final addressing mode.
        LanceStorageOptions.putIfNotEmpty(result, ENDPOINT,
                endpointForDataset(datasetUri, properties.getEndpoint(), false));
        LanceStorageOptions.putIfNotEmpty(result, REGION, properties.getRegion());
        result.put(VIRTUAL_HOSTED_STYLE, String.valueOf(!properties.isUsePathStyle()));

        return result;
    }

    /** object_store expects a bucket-qualified endpoint when virtual-hosted addressing is enabled. */
    private static String endpointForDataset(String datasetUri, String endpoint, boolean virtualHostedStyle) {
        if (endpoint == null || endpoint.isEmpty()) {
            return endpoint;
        }
        if (!endpoint.contains("://")) {
            endpoint = "https://" + endpoint;
        }
        if (!virtualHostedStyle) {
            return endpoint;
        }
        URI service = URI.create(endpoint);
        String bucket = URI.create(datasetUri).getAuthority();
        try {
            return new URI(service.getScheme(), service.getUserInfo(), bucket + "." + service.getHost(),
                    service.getPort(), service.getPath(), service.getQuery(), service.getFragment()).toString();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid virtual-hosted Lance S3 endpoint: " + endpoint, e);
        }
    }

    /**
     * Picks the one S3-compatible configuration to read, preferring a concrete provider over the
     * generic {@code S3} one: naming OSS or COS explicitly is a choice, while the generic binding is
     * also what a heuristic match lands on. The list is not a user-ordered one - it follows the
     * engine's provider registry and may lead with a default HDFS entry - so it has to be filtered by
     * type rather than indexed. OSS, COS and OBS all report the S3 family, so the provider name is
     * what tells them apart.
     *
     * <p>Same rule as the Iceberg connector's {@code IcebergCatalogFactory.chooseS3Compatible},
     * deliberately copied rather than shared: connectors do not depend on one another.
     */
    private static S3CompatibleFileSystemProperties selectS3Compatible(
            List<StorageProperties> storageProperties) {
        if (storageProperties == null) {
            return null;
        }
        S3CompatibleFileSystemProperties fallback = null;
        S3CompatibleFileSystemProperties concrete = null;
        for (StorageProperties candidate : storageProperties) {
            if (!(candidate instanceof S3CompatibleFileSystemProperties)) {
                continue;
            }
            S3CompatibleFileSystemProperties s3 = (S3CompatibleFileSystemProperties) candidate;
            if (fallback == null) {
                fallback = s3;
            }
            if (concrete == null && !"S3".equals(s3.providerName())) {
                concrete = s3;
            }
        }
        return concrete != null ? concrete : fallback;
    }

    @Override
    public Map<String, String> normalizeVendedStorageOptions(Map<String, String> options) {
        Map<String, String> normalized = LanceStorageOptions.normalizeAliases(options, CANONICAL_BY_ALIAS);
        if (normalized.containsKey(ACCESS_KEY_ID) && normalized.containsKey(SECRET_ACCESS_KEY)) {
            // A vended key pair is its own identity. Without a token of its own, it must not inherit
            // either the catalog's STS token or an unrelated token from the FE/BE environment.
            normalized.putIfAbsent(SESSION_TOKEN, "");
        }
        return normalized;
    }

    @Override
    public Map<String, String> mergeStorageOptions(String datasetUri, Map<String, String> catalogOptions,
            Map<String, String> vendedOptions) {
        Map<String, String> merged = LanceStorageProvider.super.mergeStorageOptions(
                datasetUri, catalogOptions, vendedOptions);
        if (!vendedOptions.containsKey(ENDPOINT)) {
            // Only Doris endpoints need bucket qualification. Namespace endpoints already use
            // object_store's native vocabulary and must keep the meaning the namespace assigned.
            LanceStorageOptions.putIfNotEmpty(merged, ENDPOINT, endpointForDataset(datasetUri,
                    catalogOptions.get(ENDPOINT), parseObjectStoreBoolean(
                            merged.getOrDefault(VIRTUAL_HOSTED_STYLE, "false"))));
        }
        return merged;
    }

    /** Matches object_store's {@code ConfigValue<bool>} parser, including case-insensitive alternate spellings. */
    private static boolean parseObjectStoreBoolean(String value) {
        switch (value.toLowerCase(Locale.ROOT)) {
            case "1":
            case "true":
            case "on":
            case "yes":
            case "y":
                return true;
            case "0":
            case "false":
            case "off":
            case "no":
            case "n":
                return false;
            default:
                throw new IllegalArgumentException("Unrecognized value for S3 storage option '"
                        + VIRTUAL_HOSTED_STYLE + "': '" + value
                        + "'. Expected one of 1, true, on, yes, y, 0, false, off, no, n");
        }
    }

    @Override
    public void validateStorageOptions(List<StorageProperties> storageProperties,
            Map<String, String> effectiveOptions) {
        // REST catalogs may bind just an endpoint and receive their credentials from DescribeTable.
        // Check the identity that will actually reach Lance, after those options have been merged.
        if (selectS3Compatible(storageProperties) != null
                && (effectiveOptions.getOrDefault(ACCESS_KEY_ID, "").isBlank()
                || effectiveOptions.getOrDefault(SECRET_ACCESS_KEY, "").isBlank())) {
            throw new IllegalArgumentException("Lance S3 access currently requires static s3.access_key and"
                    + " s3.secret_key or namespace-vended credentials; anonymous and credential-chain access"
                    + " are not supported yet");
        }
    }

    @Override
    public Map<String, String> inferStorageOptions(Map<String, String> effectiveOptions) {
        Map<String, String> inferred = new HashMap<>();
        String endpoint = effectiveOptions.getOrDefault(S3_ENDPOINT, effectiveOptions.get(ENDPOINT));
        // with_env_s3 imports this separate key, which object_store prefers to aws_endpoint.
        // Fill it only after vending has selected the endpoint, preserving an explicit service value.
        LanceStorageOptions.putIfNotEmpty(inferred, S3_ENDPOINT, endpoint);
        if (effectiveOptions.containsKey(ALLOW_HTTP)) {
            return inferred;
        }
        if (endpoint != null && endpoint.startsWith("http://")) {
            // object_store rejects a plain-HTTP endpoint unless this client option is enabled.
            inferred.put(ALLOW_HTTP, "true");
        }
        return inferred;
    }

}
