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

package org.apache.doris.filesystem.azure;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Parsed Azure Blob Storage URI.
 *
 * <p>Supported formats:
 * <ul>
 *   <li>wasb[s]://container@account.blob.core.windows.net/path</li>
 *   <li>abfs[s]://container@account.dfs.core.windows.net/path</li>
 *   <li>https://account.blob.core.windows.net/container/path</li>
 *   <li>s3[a|n]://container/key (S3-compatibility mode)</li>
 * </ul>
 */
public final class AzureUri {

    /**
     * Azure Blob Service container naming rules: 3-63 lower-case letters/digits/hyphens,
     * may not start or end with a hyphen. The single-character form is also accepted by
     * the SDK and is permitted here for parity with existing test fixtures.
     */
    private static final Pattern CONTAINER_NAME_PATTERN =
            Pattern.compile("^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?$");

    private final String scheme;
    private final String accountName;
    private final String container;
    private final String key;
    private final String containerUri;

    private AzureUri(String scheme, String accountName, String container, String key, String containerUri) {
        this.scheme = scheme;
        this.accountName = accountName;
        this.container = container;
        this.key = key;
        this.containerUri = containerUri;
    }

    /**
     * Parses an Azure path into its components.
     *
     * <p>Strips any URL query string (everything after the first {@code ?}) and
     * fragment (everything after the first {@code #}) before extracting the key,
     * then percent-decodes the key as UTF-8. The container name is validated
     * against Azure's naming rules and is NOT decoded (Azure containers are
     * required to be ASCII per the storage service contract).
     *
     * @param path the Azure storage path
     * @return parsed AzureUri
     * @throws IOException if the path cannot be parsed or the container name is invalid
     */
    public static AzureUri parse(String path) throws IOException {
        return parse(path, null);
    }

    static AzureUri parse(String path, ServiceEndpoint endpoint) throws IOException {
        if (path == null || path.isEmpty()) {
            throw new IOException("Azure path must not be null or empty");
        }
        // Strip a query or fragment before splitting the key or building locations for listed blobs.
        String location = StringUtils.substringBefore(StringUtils.substringBefore(path, "?"), "#");
        int schemeEnd = location.indexOf("://");
        if (schemeEnd < 0) {
            throw new IOException("Cannot parse Azure URI without scheme");
        }
        String scheme = location.substring(0, schemeEnd).toLowerCase(Locale.ROOT);
        int authorityStart = schemeEnd + 3;
        int keySeparator = nextSlash(location, authorityStart);
        String authority = location.substring(authorityStart, keySeparator);
        String container;
        String host;
        switch (scheme) {
            case "wasb":
            case "wasbs":
            case "abfs":
            case "abfss":
                int at = authority.indexOf('@');
                if (at < 0 || at + 1 == authority.length() || at != authority.lastIndexOf('@')) {
                    throw new IOException("Invalid Azure URI: expected container@host");
                }
                container = authority.substring(0, at);
                host = authority.substring(at + 1);
                break;
            case "https":
            case "http":
                host = authority;
                String servicePath = endpoint == null ? "" : endpoint.path;
                if (host.isEmpty() || host.indexOf('@') >= 0
                        || !location.startsWith(servicePath + "/", keySeparator)) {
                    throw new IOException("Azure location is outside the service endpoint");
                }
                int containerStart = keySeparator + servicePath.length() + 1;
                keySeparator = nextSlash(location, containerStart);
                container = location.substring(containerStart, keySeparator);
                break;
            case "s3":
            case "s3a":
            case "s3n":
                host = "";
                container = authority;
                break;
            default:
                throw new IOException("Unsupported Azure URI scheme: " + scheme);
        }
        if (endpoint != null && !host.isEmpty() && !endpoint.matches(host)) {
            throw new IOException("Azure file location does not match the service endpoint");
        }
        if (!CONTAINER_NAME_PATTERN.matcher(container).matches()) {
            throw new IOException("Invalid Azure container name: " + container);
        }
        String rawKey = keySeparator == location.length() ? "" : location.substring(keySeparator + 1);
        // URLDecoder is form-based. In an HTTP URL path '+' is a literal blob-name character.
        String key = decodeKey(isHttp(scheme) ? rawKey.replace("+", "%2B") : rawKey);
        return new AzureUri(scheme, StringUtils.substringBefore(host, "."), container, key,
                location.substring(0, keySeparator) + "/");
    }

    private static int nextSlash(String path, int start) {
        int slash = path.indexOf('/', start);
        return slash < 0 ? path.length() : slash;
    }

    /** Immutable binding shared by the filesystem and its Blob client for every parsed location. */
    static final class ServiceEndpoint {
        private final String path;
        private final String authority;
        private final String alias;
        private final String defaultPort;

        ServiceEndpoint(String endpoint) {
            URI uri;
            try {
                uri = new URI(endpoint);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid Azure storage endpoint");
            }
            if (uri.getRawAuthority() == null) {
                throw new IllegalArgumentException("Azure storage endpoint requires a host");
            }
            path = StringUtils.stripEnd(uri.getRawPath(), "/");
            defaultPort = "http".equalsIgnoreCase(uri.getScheme()) ? ":80" : ":443";
            authority = StringUtils.removeEnd(uri.getRawAuthority(), defaultPort);
            alias = path.isEmpty() ? blobDfsAlias(authority) : "";
        }

        private boolean matches(String host) {
            String file = StringUtils.removeEnd(host, defaultPort);
            return authority.equalsIgnoreCase(file) || (!alias.isEmpty() && alias.equalsIgnoreCase(file));
        }

        private static String blobDfsAlias(String authority) {
            String[] labels = authority.split("\\.", 3);
            if (labels.length != 3) {
                return "";
            }
            String cloud = StringUtils.substringBefore(labels[2], ":").toLowerCase(Locale.ROOT);
            if (!AzureFileSystemProperties.AZURE_BLOB_HOST_SUFFIXES.contains("blob." + cloud)
                    && !"fabric.microsoft.com".equals(cloud)) {
                return "";
            }
            if (labels[1].equalsIgnoreCase("blob")) {
                return labels[0] + ".dfs." + labels[2];
            }
            if (labels[1].equalsIgnoreCase("dfs")) {
                return labels[0] + ".blob." + labels[2];
            }
            return "";
        }
    }

    /** Original URI through the container, including a path-style service's base path. */
    String containerUri() {
        return containerUri;
    }

    private static boolean isHttp(String scheme) {
        return scheme.equals("https") || scheme.equals("http");
    }

    String appendListedKey(String prefix, String blobKey) throws IOException {
        if (!isHttp(scheme)) {
            return prefix + blobKey;
        }
        try {
            // Escape the blob name as a path, preserving '/' and '+'. An explicit empty authority
            // keeps leading slashes in the blob name in the path component.
            return prefix + new URI(null, "", "/" + blobKey, null, null).getRawPath().substring(1);
        } catch (URISyntaxException e) {
            throw new IOException("Invalid Azure blob name", e);
        }
    }

    private static String decodeKey(String raw) {
        if (raw.isEmpty()) {
            return raw;
        }
        return URLDecoder.decode(raw, StandardCharsets.UTF_8);
    }

    public String scheme() {
        return scheme;
    }

    public String accountName() {
        return accountName;
    }

    public String container() {
        return container;
    }

    public String key() {
        return key;
    }

    /**
     * Renders this URI in a canonical {@code scheme://container@accountName/key} form.
     *
     * <p>The key is percent-encoded with UTF-8 so that any reserved or non-ASCII
     * characters round-trip safely through SDK calls. Path separators ({@code /})
     * are preserved literally; spaces are emitted as {@code %20} (not {@code +}).
     */
    @Override
    public String toString() {
        return scheme + "://" + container + "@" + accountName + "/" + encodeKey(key);
    }

    static String encodeKey(String raw) {
        if (raw.isEmpty()) {
            return raw;
        }
        String enc = URLEncoder.encode(raw, StandardCharsets.UTF_8);
        // URLEncoder is form-encoded: spaces become '+' and '/' becomes '%2F'.
        // Re-normalise to RFC 3986 path-encoding so directory separators stay literal.
        return enc.replace("+", "%20").replace("%2F", "/");
    }
}
