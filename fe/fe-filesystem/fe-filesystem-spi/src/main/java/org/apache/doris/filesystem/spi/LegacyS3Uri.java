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

package org.apache.doris.filesystem.spi;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Verbatim port of the legacy fe-core {@code org.apache.doris.common.util.S3URI} parser plus the
 * {@code S3PropertyUtils.constructEndpointFromUrl} raw-property leg, kept here so every
 * S3-compatible SPI provider can derive the endpoint from a raw {@code uri} property exactly the
 * way legacy {@code AbstractS3CompatibleProperties.setEndpointIfPossible()} did.
 *
 * <p>Behavioural contract (do not "improve" — parity with fe-core is the whole point):
 * <ul>
 *   <li>the {@code uri} key is looked up case-insensitively in the raw property map;</li>
 *   <li>object-store schemes (s3/s3a/s3n/oss/cos/cosn/obs/bos/gs/azure) parse AWS-CLI style
 *       ({@code scheme://bucket/key}) unless {@code forceParsingByStandardUri} is set — such URIs
 *       carry no endpoint, so derivation yields nothing;</li>
 *   <li>http/https URIs parse virtual-hosted style by default (first host label is the bucket,
 *       the rest is the endpoint) and path-style when {@code usePathStyle} is true (the full
 *       authority is the endpoint);</li>
 *   <li>invalid input throws {@link IllegalArgumentException} with the legacy messages — callers
 *       that mirror {@code setEndpointIfPossible()} must swallow it (see
 *       {@link #deriveEndpointQuietly(Map, String, String)}).</li>
 * </ul>
 */
public final class LegacyS3Uri {

    /** Raw property key carrying the location, matched case-insensitively (legacy URI_KEY). */
    public static final String URI_KEY = "uri";

    private static final Pattern URI_PATTERN =
            Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");

    private static final Set<String> VALID_SCHEMES = Set.of("http", "https", "s3", "s3a", "s3n",
            "bos", "oss", "cos", "cosn", "obs", "gs", "azure");

    private static final Set<String> OS_SCHEMES = Set.of("s3", "s3a", "s3n",
            "bos", "oss", "cos", "cosn", "gs", "obs", "azure");

    private URI uri;
    private String bucket;
    private String key;
    private String endpoint;
    private String region;
    private boolean isStandardURL;
    private boolean isPathStyle;

    private LegacyS3Uri(String location, boolean isPathStyle, boolean forceParsingByStandardUri) {
        if (location == null || location.isEmpty()) {
            throw new IllegalArgumentException("s3 location can not be null");
        }
        this.isPathStyle = isPathStyle;
        parseUri(location, forceParsingByStandardUri);
    }

    /** Mirrors legacy {@code S3URI.create(location, isPathStyle, forceParsingByStandardUri)}. */
    public static LegacyS3Uri create(String location, boolean isPathStyle, boolean forceParsingByStandardUri) {
        return new LegacyS3Uri(location, isPathStyle, forceParsingByStandardUri);
    }

    /**
     * Mirrors legacy {@code S3PropertyUtils.constructEndpointFromUrl}: reads the {@code uri} key
     * case-insensitively from {@code props}, parses it and returns the endpoint, or {@code null}
     * when there is no usable uri/endpoint. Parse failures throw {@link IllegalArgumentException}
     * exactly like the legacy helper.
     */
    public static String constructEndpointFromUri(Map<String, String> props,
                                                  String stringUsePathStyle,
                                                  String stringForceParsingByStandardUri) {
        Optional<String> uriOptional = props.entrySet().stream()
                .filter(e -> e.getKey().equalsIgnoreCase(URI_KEY))
                .map(Map.Entry::getValue)
                .findFirst();
        if (!uriOptional.isPresent()) {
            return null;
        }
        String uri = uriOptional.get();
        if (uri == null || uri.trim().isEmpty()) {
            return null;
        }
        boolean usePathStyle = Boolean.parseBoolean(stringUsePathStyle);
        boolean forceParsingByStandardUri = Boolean.parseBoolean(stringForceParsingByStandardUri);
        LegacyS3Uri s3uri;
        try {
            s3uri = create(uri, usePathStyle, forceParsingByStandardUri);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid S3 URI: " + uri + ",usePathStyle: " + usePathStyle
                    + " forceParsingByStandardUri: " + forceParsingByStandardUri, e);
        }
        return s3uri.getEndpoint().orElse(null);
    }

    /**
     * The uri leg of legacy {@code AbstractS3CompatibleProperties.setEndpointIfPossible()}:
     * derivation errors are swallowed (legacy only logged them at debug level), so an unparsable
     * uri simply leaves the endpoint unset and the regular "Region/Endpoint is not set"
     * validation fires downstream. Returns {@code null} when nothing could be derived.
     */
    public static String deriveEndpointQuietly(Map<String, String> props,
                                               String stringUsePathStyle,
                                               String stringForceParsingByStandardUri) {
        try {
            return constructEndpointFromUri(props, stringUsePathStyle, stringForceParsingByStandardUri);
        } catch (Exception e) {
            return null;
        }
    }

    private void parseUri(String location, boolean forceParsingStandardUri) {
        parseUriLocation(location);
        validateUri();
        if (!forceParsingStandardUri && OS_SCHEMES.contains(uri.getScheme().toLowerCase())) {
            parseAwsCliStyleUri();
        } else {
            parseStandardUri();
        }
        parseEndpointAndRegion();
    }

    private void parseUriLocation(String location) {
        Matcher matcher = URI_PATTERN.matcher(location);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Failed to parse uri: " + location);
        }
        String scheme = matcher.group(2);
        String authority = matcher.group(4);
        String path = matcher.group(5);
        String query = matcher.group(7);
        String fragment = matcher.group(9);
        try {
            uri = new URI(scheme, authority, path, query, fragment).normalize();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private void validateUri() {
        if (uri.getScheme() == null || !VALID_SCHEMES.contains(uri.getScheme().toLowerCase())) {
            throw new IllegalArgumentException("Invalid scheme: " + this.uri);
        }
    }

    private void parseAwsCliStyleUri() {
        bucket = uri.getAuthority();
        if (bucket == null) {
            throw new IllegalArgumentException("missing bucket: " + uri);
        }
        String path = uri.getPath();
        if (path.length() > 1) {
            key = path.substring(1);
        } else {
            throw new IllegalArgumentException("missing key: " + uri);
        }
        isStandardURL = false;
        this.isPathStyle = false;
    }

    private void parseStandardUri() {
        if (uri.getHost() == null) {
            throw new IllegalArgumentException("Invalid S3 URI: no hostname: " + uri);
        }
        if (isPathStyle) {
            parsePathStyleUri();
        } else {
            parseVirtualHostedStyleUri();
        }
        isStandardURL = true;
    }

    private void parsePathStyleUri() {
        String path = uri.getPath();
        if (path != null && !path.isEmpty() && !"/".equals(path)) {
            int index = path.indexOf('/', 1);
            if (index == -1) {
                // No trailing slash, e.g., "https://s3.amazonaws.com/bucket"
                bucket = path.substring(1);
                throw new IllegalArgumentException("missing key: " + uri);
            } else {
                bucket = path.substring(1, index);
                if (index != path.length() - 1) {
                    key = path.substring(index + 1);
                } else {
                    throw new IllegalArgumentException("missing key: " + uri);
                }
            }
        } else {
            throw new IllegalArgumentException("missing bucket: " + this.uri);
        }
    }

    private void parseVirtualHostedStyleUri() {
        bucket = uri.getHost().split("\\.")[0];
        String path = uri.getPath();
        if (path != null && !path.isEmpty() && !"/".equals(path)) {
            key = path.substring(1);
        } else {
            throw new IllegalArgumentException("missing key from uri: " + this.uri);
        }
    }

    private void parseEndpointAndRegion() {
        // parse endpoint
        if (isStandardURL) {
            if (isPathStyle) {
                endpoint = uri.getAuthority();
            } else { // virtual_host_style
                if (uri.getAuthority() == null) {
                    endpoint = null;
                    return;
                }
                String[] splits = uri.getAuthority().split("\\.", 2);
                if (splits.length < 2) {
                    endpoint = null;
                    return;
                }
                endpoint = splits[1];
            }
        } else {
            endpoint = null;
        }
        if (endpoint == null) {
            return;
        }

        // parse region
        String[] endpointSplits = endpoint.split("\\.");
        if (endpointSplits.length < 2) {
            return;
        }
        if (endpointSplits[0].contains("oss-")) {
            // compatible with the endpoint: oss-cn-bejing.aliyuncs.com
            region = endpointSplits[0];
            return;
        }
        region = endpointSplits[1];
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }

    public Optional<String> getEndpoint() {
        return Optional.ofNullable(endpoint);
    }

    public Optional<String> getRegion() {
        return Optional.ofNullable(region);
    }
}
