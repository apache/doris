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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * This class represents a fully qualified location in S3 for input/output
 * operations expressed as as URI.
 * <p>
 * For AWS S3, uri common styles should be:
 * 1. AWS Client Style(Hadoop S3 Style): s3://my-bucket/path/to/file?versionId=abc123&partNumber=77&partNumber=88
 * or
 * 2. Virtual Host Style: https://my-bucket.s3.us-west-1.amazonaws.com/resources/doc.txt?versionId=abc123&partNumber=77&partNumber=88
 * or
 * 3. Path Style: https://s3.us-west-1.amazonaws.com/my-bucket/resources/doc.txt?versionId=abc123&partNumber=77&partNumber=88
 *
 * Regarding the above-mentioned common styles, we can use <code>isPathStyle</code> to control whether to use path style
 * or virtual host style.
 * "Virtual host style" is the currently mainstream and recommended approach to use, so the default value of
 * <code>isPathStyle</code> is false.
 *
 * Other Styles:
 * 1. Virtual Host AWS Client (Hadoop S3) Mixed Style:
 * s3://my-bucket.s3.us-west-1.amazonaws.com/resources/doc.txt?versionId=abc123&partNumber=77&partNumber=88
 * or
 * 2. Path AWS Client (Hadoop S3) Mixed Style:
 * s3://s3.us-west-1.amazonaws.com/my-bucket/resources/doc.txt?versionId=abc123&partNumber=77&partNumber=88
 *
 * For these two styles, we can use <code>isPathStyle</code> and <code>forceParsingByStandardUri</code>
 * to control whether to use.
 * Virtual Host AWS Client (Hadoop S3) Mixed Style: <code>isPathStyle = false && forceParsingByStandardUri = true</code>
 * Path AWS Client (Hadoop S3) Mixed Style: <code>isPathStyle = true && forceParsingByStandardUri = true</code>
 *
 */

public class S3URI {

    private static final Pattern URI_PATTERN =
            Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");
    public static final String SCHEME_DELIM = "://";
    public static final String PATH_DELIM = "/";
    private static final Set<String> VALID_SCHEMES = ImmutableSet.of("http", "https", "s3", "s3a", "s3n",
            "bos", "oss", "cos", "cosn", "obs");

    private static final Set<String> OS_SCHEMES = ImmutableSet.of("s3", "s3a", "s3n",
            "bos", "oss", "cos", "cosn", "obs");

    private URI uri;

    private String bucket;
    private String key;

    private String endpoint;

    private String region;

    private boolean isStandardURL;
    private boolean isPathStyle;
    private Map<String, List<String>> queryParams;

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
        return create(location, false, false);
    }

    public static S3URI create(String location, boolean isPathStyle) throws UserException {
        return new S3URI(location, isPathStyle, false);
    }

    public static S3URI create(String location, boolean isPathStyle, boolean forceParsingByStandardUri)
            throws UserException {
        return new S3URI(location, isPathStyle, forceParsingByStandardUri);
    }

    private S3URI(String location, boolean isPathStyle, boolean forceParsingByStandardUri) throws UserException {
        if (Strings.isNullOrEmpty(location)) {
            throw new UserException("s3 location can not be null");
        }
        this.isPathStyle = isPathStyle;
        parseUri(location, forceParsingByStandardUri);
    }

    private void parseUri(String location, boolean forceParsingStandardUri) throws UserException {
        parseURILocation(location);
        validateUri();

        if (!forceParsingStandardUri && OS_SCHEMES.contains(uri.getScheme().toLowerCase())) {
            parseAwsCliStyleUri();
        } else {
            parseStandardUri();
        }
        parseEndpointAndRegion();
    }

    /**
     * parse uri location and encode to a URI.
     * @param location
     * @throws UserException
     */
    private void parseURILocation(String location) throws UserException {
        Matcher matcher = URI_PATTERN.matcher(location);
        if (!matcher.matches()) {
            throw new UserException("Failed to parse uri: " + location);
        }
        String scheme = matcher.group(2);
        String authority = matcher.group(4);
        String path = matcher.group(5);
        String query = matcher.group(7);
        String fragment = matcher.group(9);
        try {
            uri = new URI(scheme, authority, path, query, fragment).normalize();
        } catch (URISyntaxException e) {
            throw new UserException(e);
        }
    }

    private void validateUri() throws UserException {
        if (uri.getScheme() == null || !VALID_SCHEMES.contains(uri.getScheme().toLowerCase())) {
            throw new UserException("Invalid scheme: " + this.uri);
        }
    }

    private void parseAwsCliStyleUri() throws UserException {
        bucket = uri.getAuthority();
        if (bucket == null) {
            throw new UserException("missing bucket: " + uri);
        }
        String path = uri.getPath();
        if (path.length() > 1) {
            key = path.substring(1);
        } else {
            throw new UserException("missing key: " + uri);
        }

        addQueryParamsIfNeeded();

        isStandardURL = false;
        this.isPathStyle = false;
    }

    private void parseStandardUri() throws UserException {
        if (uri.getHost() == null) {
            throw new UserException("Invalid S3 URI: no hostname: " + uri);
        }

        addQueryParamsIfNeeded();

        if (isPathStyle) {
            parsePathStyleUri();
        } else {
            parseVirtualHostedStyleUri();
        }
        isStandardURL = true;
    }

    private void addQueryParamsIfNeeded() {
        if (uri.getQuery() != null) {
            queryParams = splitQueryString(uri.getQuery()).stream().map((s) -> s.split("="))
                    .map((s) -> s.length == 1 ? new String[] {s[0], null} : s).collect(
                            Collectors.groupingBy((a) -> a[0],
                                    Collectors.mapping((a) -> a[1], Collectors.toList())));
        }
    }

    private static List<String> splitQueryString(String queryString) {
        List<String> results = new ArrayList<>();
        StringBuilder result = new StringBuilder();

        for (int i = 0; i < queryString.length(); ++i) {
            char character = queryString.charAt(i);
            if (character != '&') {
                result.append(character);
            } else {
                String param = result.toString();
                results.add(param);
                result.setLength(0);
            }
        }

        String param = result.toString();
        results.add(param);
        return results;
    }

    private void parsePathStyleUri() throws UserException {
        String path = uri.getPath();

        if (!StringUtils.isEmpty(path) && !"/".equals(path)) {
            int index = path.indexOf('/', 1);

            if (index == -1) {
                // No trailing slash, e.g., "https://s3.amazonaws.com/bucket"
                bucket = path.substring(1);
                throw new UserException("missing key: " + uri);
            } else {
                bucket = path.substring(1, index);
                if (index != path.length() - 1) {
                    key = path.substring(index + 1);
                } else {
                    throw new UserException("missing key: " + uri);
                }
            }
        } else {
            throw new UserException("missing bucket: " + this.uri);
        }
    }

    private void parseVirtualHostedStyleUri() throws UserException {
        bucket = uri.getHost().split("\\.")[0];

        String path = uri.getPath();
        if (!StringUtils.isEmpty(path) && !"/".equals(path)) {
            key = path.substring(1);
        } else {
            throw new UserException("missing key: " + this.uri);
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

    public Optional<Map<String, List<String>>> getQueryParams() {
        return Optional.ofNullable(queryParams);
    }

    public Optional<String> getEndpoint() {
        return Optional.ofNullable(endpoint);
    }

    public Optional<String> getRegion() {
        return Optional.ofNullable(region);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("S3URI{");
        sb.append("uri=").append(uri);
        sb.append(", bucket='").append(bucket).append('\'');
        sb.append(", key='").append(key).append('\'');
        sb.append(", endpoint='").append(endpoint).append('\'');
        sb.append(", region='").append(region).append('\'');
        sb.append(", isStandardURL=").append(isStandardURL);
        sb.append(", isPathStyle=").append(isPathStyle);
        sb.append(", queryParams=").append(queryParams);
        sb.append('}');
        return sb.toString();
    }
}
