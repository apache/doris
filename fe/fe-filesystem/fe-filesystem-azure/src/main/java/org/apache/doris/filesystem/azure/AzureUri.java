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

import org.apache.doris.foundation.property.StoragePropertiesException;

import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Optional;
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
    private final String authority;
    private final AzureAccountHost accountHost;
    private final String container;
    private final String key;

    private AzureUri(String scheme, String authority, AzureAccountHost accountHost, String container, String key) {
        this.scheme = scheme;
        this.authority = authority;
        this.accountHost = accountHost;
        this.container = container;
        this.key = key;
    }

    /**
     * Parses an Azure path into its components.
     *
     * <p>Strips any URL query string (everything after the first {@code ?}) and
     * fragment (everything after the first {@code #}) before extracting the key,
     * then percent-decodes the key once as UTF-8, preserving literal {@code +}
     * characters and path separators. The container name is validated
     * against Azure's naming rules and is NOT decoded (Azure containers are
     * required to be ASCII per the storage service contract).
     *
     * @param path the Azure storage path
     * @return parsed AzureUri
     * @throws IOException if the path cannot be parsed or the container name is invalid
     */
    public static AzureUri parse(String path) throws IOException {
        if (path == null || path.isEmpty()) {
            throw new IOException("Azure path must not be null or empty");
        }
        int schemeEnd = path.indexOf("://");
        if (schemeEnd <= 0) {
            throw new IOException("Cannot parse Azure URI without scheme");
        }
        String scheme = path.substring(0, schemeEnd).toLowerCase(Locale.ROOT);
        String rest = path.substring(schemeEnd + 3);
        // Strip query and fragment so they do not pollute the key segment.
        int queryIdx = rest.indexOf('?');
        if (queryIdx >= 0) {
            rest = rest.substring(0, queryIdx);
        }
        int fragIdx = rest.indexOf('#');
        if (fragIdx >= 0) {
            rest = rest.substring(0, fragIdx);
        }

        AzureUri parsed;
        if (scheme.equals("wasb") || scheme.equals("wasbs")
                || scheme.equals("abfs") || scheme.equals("abfss")) {
            parsed = parseWasbAbfs(scheme, rest);
        } else if (scheme.equals("https") || scheme.equals("http")) {
            parsed = parseHttps(scheme, rest);
        } else if (scheme.equals("s3") || scheme.equals("s3a") || scheme.equals("s3n")) {
            parsed = parseS3Compat(scheme, rest);
        } else {
            throw new IOException("Unsupported Azure URI scheme");
        }
        if (!CONTAINER_NAME_PATTERN.matcher(parsed.container).matches()) {
            throw new IOException("Invalid Azure container name");
        }
        return parsed;
    }

    private static AzureUri parseWasbAbfs(String scheme, String rest) throws IOException {
        // wasb://container@account.blob.core.windows.net/path
        int slashIdx = rest.indexOf('/');
        String authority = slashIdx < 0 ? rest : rest.substring(0, slashIdx);
        int atIdx = authority.indexOf('@');
        if (atIdx <= 0 || atIdx == authority.length() - 1 || authority.indexOf('@', atIdx + 1) >= 0) {
            throw new IOException("Invalid Azure URI authority: expected container@account-host");
        }
        String container = authority.substring(0, atIdx);
        AzureAccountHost accountHost = parseAccountHost(authority.substring(atIdx + 1));
        String key = slashIdx < 0 ? "" : rest.substring(slashIdx + 1);
        return new AzureUri(scheme, authority, accountHost, container, decodeKey(key));
    }

    private static AzureUri parseHttps(String scheme, String rest) throws IOException {
        // https://account.blob.core.windows.net/container/path
        int slashIdx = rest.indexOf('/');
        String host = slashIdx < 0 ? rest : rest.substring(0, slashIdx);
        if (host.isEmpty() || host.indexOf('@') >= 0) {
            throw new IOException("Invalid Azure URI account host");
        }
        String pathAfterHost = slashIdx < 0 ? "" : rest.substring(slashIdx + 1);
        AzureAccountHost accountHost = parseAccountHost(scheme + "://" + host);
        int containerEnd = pathAfterHost.indexOf('/');
        String container;
        String key;
        if (containerEnd < 0) {
            container = pathAfterHost;
            key = "";
        } else {
            container = pathAfterHost.substring(0, containerEnd);
            key = pathAfterHost.substring(containerEnd + 1);
        }
        return new AzureUri(scheme, host, accountHost, container, decodeKey(key));
    }

    private static AzureUri parseS3Compat(String scheme, String rest) throws IOException {
        // s3://container/key — S3-compatibility mode; accountName from properties
        int slashIdx = rest.indexOf('/');
        String container;
        String key;
        if (slashIdx < 0) {
            container = rest;
            key = "";
        } else {
            container = rest.substring(0, slashIdx);
            key = rest.substring(slashIdx + 1);
        }
        return new AzureUri(scheme, container, null, container, decodeKey(key));
    }

    private static AzureAccountHost parseAccountHost(String host) throws IOException {
        try {
            return AzureAccountHost.parse(host);
        } catch (StoragePropertiesException e) {
            throw new IOException("Invalid Azure URI account host");
        }
    }

    private static String decodeKey(String raw) throws IOException {
        if (raw.isEmpty()) {
            return raw;
        }
        try {
            // URLDecoder uses HTML form rules, but '+' is a literal character in an object path.
            return URLDecoder.decode(raw.replace("+", "%2B"), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            // The decoder error may contain the input. Do not retain URI credential material.
            throw new IOException("Invalid percent encoding in Azure object path");
        }
    }

    public String scheme() {
        return scheme;
    }

    public String accountName() {
        return accountHost == null ? "" : accountHost.accountName();
    }

    /** The URI's account and cloud suffix; absent for legacy S3-compatible locations. */
    public Optional<AzureAccountHost> accountHost() {
        return Optional.ofNullable(accountHost);
    }

    public String container() {
        return container;
    }

    public String key() {
        return key;
    }

    /**
     * Renders this URI with its original scheme and complete authority, including the cloud suffix.
     *
     * <p>The key is percent-encoded with UTF-8 so that any reserved or non-ASCII
     * characters round-trip safely through SDK calls. Path separators ({@code /})
     * are preserved literally; spaces are emitted as {@code %20} (not {@code +}).
     */
    @Override
    public String toString() {
        String containerPath = scheme.equals("http") || scheme.equals("https") ? "/" + container : "";
        return scheme + "://" + authority + containerPath + "/" + encodeKey(key);
    }

    private static String encodeKey(String raw) {
        if (raw.isEmpty()) {
            return raw;
        }
        String enc = URLEncoder.encode(raw, StandardCharsets.UTF_8);
        // URLEncoder is form-encoded: spaces become '+' and '/' becomes '%2F'.
        // Re-normalise to RFC 3986 path-encoding so directory separators stay literal.
        return enc.replace("+", "%20").replace("%2F", "/");
    }
}
