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

package org.apache.doris.datasource.storage;

import org.apache.doris.common.UserException;
import org.apache.doris.common.util.S3URI;
import org.apache.doris.foundation.property.StoragePropertiesException;

import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * fe-core-only URI validation/normalization glue for {@link StorageAdapter}.
 *
 * <p>Ports the URI subset of the legacy {@code S3PropertyUtils} and {@code AzurePropertyUtils}
 * pure functions so the facade does not depend on the to-be-deleted
 * {@code datasource.property.storage} package. Behavior is kept identical, with one deliberate
 * exception: the legacy full-parse path could throw the checked {@code UserException} (from
 * {@link S3URI#create}); the facade surface is unchecked, so that exception is rethrown as
 * {@link StoragePropertiesException} with the same message.</p>
 */
public final class StorageUriUtils {

    private static final String URI_KEY = "uri";
    private static final String SCHEME_DELIM = "://";
    private static final String S3_SCHEME_PREFIX = "s3://";

    // S3-compatible schemes that can be converted to s3:// with simple string replacement
    // Format: scheme://bucket/key -> s3://bucket/key
    private static final String[] SIMPLE_S3_COMPATIBLE_SCHEMES = {
            "s3a", "s3n", "oss", "cos", "cosn", "obs", "bos", "gs"
    };

    private static final Pattern ONELAKE_PATTERN = Pattern.compile(
            "abfs[s]?://([^@]+)@([^/]+)\\.dfs\\.fabric\\.microsoft\\.com(/.*)?", Pattern.CASE_INSENSITIVE);

    private StorageUriUtils() {
    }

    /**
     * Port of legacy {@code S3PropertyUtils.validateAndNormalizeUri}: validates and normalizes
     * the given path into a standard {@code s3://bucket/key} URI.
     *
     * <p>Also carries the legacy {@code OSSProperties.validateAndNormalizeUri} override: OSS
     * bindings first rewrote a virtual-hosted {@code scheme://bucket.endpoint/key} authority to
     * {@code scheme://bucket/key} ({@code rewriteOssBucketIfNecessary}) before the base
     * normalization, so an OSS bucket-domain URI under a non-http scheme never reached BE or the
     * filesystem with the whole host as the bucket. The facade has no provider key at this call
     * site (StorageAdapter routes every S3-compatible binding here), so the rewrite is gated on
     * the URI shape instead — see {@link #rewriteOssBucketIfNecessary(String)}.</p>
     */
    public static String validateAndNormalizeS3Uri(String path,
            String stringUsePathStyle,
            String stringForceParsingByStandardUri) {
        if (StringUtils.isBlank(path)) {
            throw new StoragePropertiesException("path is null");
        }

        // Legacy OSSProperties applied the bucket-domain rewrite before the base normalization.
        path = rewriteOssBucketIfNecessary(path);

        // Fast path 1: s3:// paths are already in the normalized format expected by BE
        if (path.startsWith(S3_SCHEME_PREFIX)) {
            return path;
        }

        // Fast path 2: simple S3-compatible schemes (oss://, cos://, s3a://, etc.)
        // can be converted with simple string replacement: scheme://bucket/key -> s3://bucket/key
        String normalized = trySimpleSchemeConversion(path);
        if (normalized != null) {
            return normalized;
        }

        // Full parsing path: for HTTP URLs and other complex formats
        boolean usePathStyle = Boolean.parseBoolean(stringUsePathStyle);
        boolean forceParsingByStandardUri = Boolean.parseBoolean(stringForceParsingByStandardUri);
        S3URI s3uri;
        try {
            s3uri = S3URI.create(path, usePathStyle, forceParsingByStandardUri);
        } catch (UserException e) {
            // Facade surface is unchecked; keep the legacy message.
            throw new StoragePropertiesException(e.getMessage(), e);
        }
        return "s3" + S3URI.SCHEME_DELIM + s3uri.getBucket() + S3URI.PATH_DELIM + s3uri.getKey();
    }

    /**
     * Port of legacy {@code OSSProperties.rewriteOssBucketIfNecessary}: rewrites the bucket part
     * of an OSS URI when the bucket is written in the virtual-hosted {@code bucket.endpoint}
     * form. HTTP/HTTPS URIs are returned unchanged (the standard-URI parser handles those); for
     * other schemes the authority is chopped at the first dot:
     * {@code oss://bucket.endpoint/path -> oss://bucket/path},
     * {@code s3://bucket.endpoint/path -> s3://bucket/path}.
     *
     * <p>Legacy applied this rewrite whenever the binding was OSS; this facade entry point has no
     * provider key, so the rewrite is gated on the URI shape instead: an {@code oss://} scheme
     * (an OSS-identity scheme, mirroring legacy {@code isKnownObjectStorage}), or an authority
     * whose post-first-dot remainder is an Aliyun OSS domain ({@code *.aliyuncs.com}) — the exact
     * bucket-domain form the legacy rewrite existed for. {@code oss-dls.aliyuncs} authorities are
     * OSS-HDFS territory (excluded from the OSS binding via legacy {@code DLS_URI_KEYWORDS}) and
     * are never rewritten.</p>
     */
    private static String rewriteOssBucketIfNecessary(String uri) {
        if (uri == null || uri.isEmpty()) {
            return uri;
        }

        URI parsed;
        try {
            parsed = URI.create(uri);
        } catch (IllegalArgumentException e) {
            // Invalid URI, do not rewrite
            return uri;
        }

        String scheme = parsed.getScheme();
        if (scheme == null || "http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme)) {
            return uri;
        }

        // For non-standard schemes (oss / s3), authority is more reliable than host
        String authority = parsed.getAuthority();
        if (authority == null || authority.isEmpty()) {
            return uri;
        }

        // Handle bucket.endpoint format
        int dotIndex = authority.indexOf('.');
        if (dotIndex <= 0) {
            return uri;
        }

        // Provider-key-less stand-in for legacy "binding is OSS": the dotted remainder must be
        // an Aliyun OSS bucket-domain endpoint, or the scheme itself must be OSS identity.
        String domain = authority.substring(dotIndex + 1);
        if (domain.contains("oss-dls.aliyuncs")) {
            return uri;
        }
        if (!"oss".equalsIgnoreCase(scheme) && !domain.contains("aliyuncs.com")) {
            return uri;
        }

        String bucket = authority.substring(0, dotIndex);

        try {
            URI rewritten = new URI(
                    scheme,
                    bucket,
                    parsed.getPath(),
                    parsed.getQuery(),
                    parsed.getFragment()
            );
            return rewritten.toString();
        } catch (URISyntaxException e) {
            // Be conservative: fallback to original URI
            return uri;
        }
    }

    private static String trySimpleSchemeConversion(String path) {
        int delimIndex = path.indexOf(SCHEME_DELIM);
        if (delimIndex <= 0) {
            return null;
        }

        String scheme = path.substring(0, delimIndex).toLowerCase();
        for (String compatibleScheme : SIMPLE_S3_COMPATIBLE_SCHEMES) {
            if (compatibleScheme.equals(scheme)) {
                String rest = path.substring(delimIndex + SCHEME_DELIM.length());
                if (rest.isEmpty() || rest.startsWith(S3URI.PATH_DELIM) || rest.contains(SCHEME_DELIM)) {
                    return null;
                }
                // Simple conversion: replace scheme with "s3"
                // e.g., "oss://bucket/key" -> "s3://bucket/key"
                return S3_SCHEME_PREFIX + rest;
            }
        }
        return null;
    }

    /**
     * Port of legacy {@code S3PropertyUtils.validateAndGetUri}: extracts the raw URI string
     * from the given props map (case-insensitive {@code uri} key).
     */
    public static String validateAndGetS3Uri(Map<String, String> props) {
        if (props.isEmpty()) {
            throw new StoragePropertiesException("props is empty");
        }
        Optional<String> uriOptional = props.entrySet().stream()
                .filter(e -> e.getKey().equalsIgnoreCase(URI_KEY))
                .map(Map.Entry::getValue)
                .findFirst();

        if (!uriOptional.isPresent()) {
            throw new StoragePropertiesException("props must contain uri");
        }
        return uriOptional.get();
    }

    /**
     * Port of legacy {@code AzurePropertyUtils.validateAndNormalizeUri}: accepts Azure Blob
     * URI schemes only, keeps OneLake locations as-is, and converts everything else to the
     * unified {@code s3://container/path} style.
     */
    public static String validateAndNormalizeAzureUri(String path) {
        if (StringUtils.isBlank(path)) {
            throw new StoragePropertiesException("Path cannot be null or empty");
        }
        // Only accept Azure Blob Storage-related URI schemes
        if (!(path.startsWith("wasb://") || path.startsWith("wasbs://")
                || path.startsWith("abfs://") || path.startsWith("abfss://")
                || path.startsWith("https://") || path.startsWith("http://")
                || path.startsWith("s3://"))) {
            throw new StoragePropertiesException("Unsupported Azure URI scheme: " + path);
        }
        if (isOneLakeLocation(path)) {
            return path;
        }
        return convertAzureToS3Style(path);
    }

    /**
     * Port of legacy {@code AzurePropertyUtils.isOneLakeLocation}: true when the location is a
     * Microsoft Fabric OneLake abfs/abfss URI (kept as-is instead of s3-style normalization).
     */
    public static boolean isOneLakeLocation(String location) {
        return ONELAKE_PATTERN.matcher(location).matches();
    }

    /** True when the location's URI scheme is {@code jfs} (JuiceFS riding the HDFS binding). */
    public static boolean isJfsLocation(String location) {
        return location != null && location.regionMatches(true, 0, "jfs:", 0, 4);
    }

    /**
     * Port of the {@code jfs://} subset of legacy {@code HdfsPropertiesUtils
     * .validateAndNormalizeUri}: fe-core's HDFS typed class accepted {@code {hdfs, viewfs, jfs}}
     * while the HDFS filesystem plugin's scheme identity is {@code {hdfs, viewfs}} only (jfs has
     * its own plugin), so the facade owns the jfs leg. The legacy hdfs-prefix/host fix-ups can
     * never match a jfs URI, so this reduces to the legacy encode/normalize/decode round-trip
     * (host-less jfs URIs return the encoded form, exactly like the legacy code path did).
     */
    public static String validateAndNormalizeJfsUri(String location) {
        if (StringUtils.isBlank(location)) {
            throw new IllegalArgumentException("Property 'uri' is required.");
        }
        try {
            // Encode the location string, but keep '/' and ':' unescaped to preserve URI structure
            String newLocation = URLEncoder.encode(location, StandardCharsets.UTF_8.name())
                    .replace("%2F", "/")
                    .replace("%3A", ":");
            URI uri = new URI(newLocation).normalize();
            if (!"jfs".equalsIgnoreCase(uri.getScheme())) {
                throw new IllegalArgumentException("Unsupported schema: " + uri.getScheme());
            }
            // Legacy hdfs-prefix/host fix-ups are all no-ops for jfs URIs; both legacy branches
            // reduce to returning the decoded, normalized location.
            return URLDecoder.decode(newLocation, StandardCharsets.UTF_8.name());
        } catch (URISyntaxException | UnsupportedEncodingException e) {
            throw new StoragePropertiesException("Failed to parse URI: " + location, e);
        }
    }

    private static String convertAzureToS3Style(String uri) {
        if (StringUtils.isBlank(uri)) {
            throw new StoragePropertiesException("URI is blank");
        }
        if (uri.startsWith("s3://")) {
            return uri;
        }
        // Handle Azure HDFS-style URIs (wasb://, wasbs://, abfs://, abfss://)
        if (uri.startsWith("wasb://") || uri.startsWith("wasbs://")
                || uri.startsWith("abfs://") || uri.startsWith("abfss://")) {

            // Example: wasbs://container@account.blob.core.windows.net/path/file.txt
            String schemeRemoved = uri.replaceFirst("^[a-z]+s?://", "");
            int atIndex = schemeRemoved.indexOf('@');
            if (atIndex < 0) {
                throw new StoragePropertiesException("Invalid Azure URI, missing '@': " + uri);
            }

            // Extract container name (before '@')
            String container = schemeRemoved.substring(0, atIndex);

            // Extract remaining part after '@'
            String remainder = schemeRemoved.substring(atIndex + 1);
            int slashIndex = remainder.indexOf('/');

            // Extract the path part if it exists
            String path = (slashIndex != -1) ? remainder.substring(slashIndex + 1) : "";

            // Normalize to s3-style URI: s3://<container>/<path>
            return StringUtils.isBlank(path)
                    ? String.format("s3://%s", container)
                    : String.format("s3://%s/%s", container, path);
        }

        // Handle HTTPS/HTTP Azure Blob Storage URLs
        if (uri.startsWith("https://") || uri.startsWith("http://")) {
            try {
                URI parsed = new URI(uri);
                String host = parsed.getHost();
                String path = parsed.getPath();

                if (StringUtils.isBlank(host)) {
                    throw new StoragePropertiesException("Invalid Azure HTTPS URI, missing host: " + uri);
                }

                // Path usually looks like: /<container>/<path>
                String[] parts = path.split("/", 3);
                if (parts.length < 2) {
                    throw new StoragePropertiesException("Invalid Azure Blob URL, missing container: " + uri);
                }

                String container = parts[1];
                String remainder = (parts.length == 3) ? parts[2] : "";

                // Convert HTTPS URL to s3-style format
                return StringUtils.isBlank(remainder)
                        ? String.format("s3://%s", container)
                        : String.format("s3://%s/%s", container, remainder);

            } catch (URISyntaxException e) {
                throw new StoragePropertiesException("Invalid HTTPS URI: " + uri, e);
            }
        }

        throw new StoragePropertiesException("Unsupported Azure URI scheme: " + uri);
    }

    /**
     * Port of legacy {@code AzurePropertyUtils.validateAndGetUri}: extracts the raw URI string
     * from the given props map (case-insensitive {@code uri} key).
     */
    public static String validateAndGetAzureUri(Map<String, String> props) {
        if (props == null || props.isEmpty()) {
            throw new StoragePropertiesException("Properties map cannot be null or empty");
        }

        return props.entrySet().stream()
                .filter(e -> URI_KEY.equalsIgnoreCase(e.getKey()))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElseThrow(() -> new StoragePropertiesException("Properties must contain 'uri' key"));
    }
}
