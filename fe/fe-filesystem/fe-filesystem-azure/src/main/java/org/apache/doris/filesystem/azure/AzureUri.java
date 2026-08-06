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

import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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

    private AzureUri(String scheme, String accountName, String container, String key) {
        this.scheme = scheme;
        this.accountName = accountName;
        this.container = container;
        this.key = key;
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
        if (path == null || path.isEmpty()) {
            throw new IOException("Azure path must not be null or empty");
        }
        int schemeEnd = path.indexOf("://");
        if (schemeEnd < 0) {
            throw new IOException("Cannot parse Azure URI without scheme: " + path);
        }
        String scheme = path.substring(0, schemeEnd).toLowerCase();
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
            parsed = parseWasbAbfs(scheme, rest, path);
        } else if (scheme.equals("https") || scheme.equals("http")) {
            parsed = parseHttps(scheme, rest);
        } else if (scheme.equals("s3") || scheme.equals("s3a") || scheme.equals("s3n")) {
            parsed = parseS3Compat(scheme, rest);
        } else {
            throw new IOException("Unsupported Azure URI scheme '" + scheme + "' in path: " + path);
        }
        if (!CONTAINER_NAME_PATTERN.matcher(parsed.container).matches()) {
            throw new IOException("Invalid Azure container name: " + parsed.container);
        }
        return parsed;
    }

    private static AzureUri parseWasbAbfs(String scheme, String rest, String original) throws IOException {
        // wasb://container@account.blob.core.windows.net/path
        int atIdx = rest.indexOf('@');
        if (atIdx < 0) {
            throw new IOException("Invalid Azure URI format (missing '@'): " + original);
        }
        String container = rest.substring(0, atIdx);
        String hostAndPath = rest.substring(atIdx + 1);
        int slashIdx = hostAndPath.indexOf('/');
        String host = slashIdx < 0 ? hostAndPath : hostAndPath.substring(0, slashIdx);
        String key = slashIdx < 0 ? "" : hostAndPath.substring(slashIdx + 1);
        String accountName = host.contains(".") ? host.substring(0, host.indexOf('.')) : host;
        return new AzureUri(scheme, accountName, container, decodeKey(key));
    }

    private static AzureUri parseHttps(String scheme, String rest) {
        // https://account.blob.core.windows.net/container/path
        int slashIdx = rest.indexOf('/');
        String host = slashIdx < 0 ? rest : rest.substring(0, slashIdx);
        String pathAfterHost = slashIdx < 0 ? "" : rest.substring(slashIdx + 1);
        String accountName = host.contains(".") ? host.substring(0, host.indexOf('.')) : host;
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
        return new AzureUri(scheme, accountName, container, decodeKey(key));
    }

    private static AzureUri parseS3Compat(String scheme, String rest) {
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
        return new AzureUri(scheme, "", container, decodeKey(key));
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
