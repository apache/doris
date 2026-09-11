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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Set;

/** Parsed Azure account authority and endpoint shared by provider configuration renderers. */
public final class AzureAccountHost {

    private static final String DEFAULT_CLOUD_SUFFIX = "core.windows.net";
    private static final String DFS_MARKER = ".dfs.";
    private static final String BLOB_MARKER = ".blob.";
    private static final Set<String> AZURE_CLOUD_SUFFIXES = Set.of(
            "core.windows.net", "core.chinacloudapi.cn", "core.usgovcloudapi.net",
            "core.cloudapi.de");

    private final String accountName;
    private final String cloudSuffix;
    private final boolean dfsHost;
    private final URI endpoint;

    private AzureAccountHost(String accountName, String cloudSuffix, boolean dfsHost, URI endpoint) {
        this.accountName = accountName;
        this.cloudSuffix = cloudSuffix;
        this.dfsHost = dfsHost;
        this.endpoint = endpoint;
    }

    /**
     * Parses an account host or endpoint such as {@code account.dfs.core.windows.net}.
     * A URI scheme is optional. Explicit transport settings are retained when rendering a Blob
     * endpoint; only official Azure DFS endpoints are rewritten.
     */
    public static AzureAccountHost parse(String hostOrEndpoint) {
        if (hostOrEndpoint == null || hostOrEndpoint.isBlank()) {
            throw new StoragePropertiesException("Azure account host must not be empty");
        }
        String value = hostOrEndpoint.trim();
        String uriValue = value.contains("://") ? value : "https://" + value;
        final URI endpoint;
        try {
            endpoint = new URI(uriValue);
        } catch (URISyntaxException e) {
            // URISyntaxException includes the input, which can contain a credential-bearing query.
            throw new StoragePropertiesException("Invalid Azure account host");
        }
        String host = endpoint.getHost();
        if (host == null || host.isBlank()) {
            throw new StoragePropertiesException("Invalid Azure account host");
        }

        String lowerHost = host.toLowerCase(Locale.ROOT);
        validateEndpoint(endpoint);
        int dfsIndex = lowerHost.indexOf(DFS_MARKER);
        if (dfsIndex > 0) {
            return new AzureAccountHost(host.substring(0, dfsIndex),
                    host.substring(dfsIndex + DFS_MARKER.length()), true, endpoint);
        }
        int blobIndex = lowerHost.indexOf(BLOB_MARKER);
        if (blobIndex > 0) {
            return new AzureAccountHost(host.substring(0, blobIndex),
                    host.substring(blobIndex + BLOB_MARKER.length()), false, endpoint);
        }

        int dot = host.indexOf('.');
        String accountName = dot > 0 ? host.substring(0, dot) : host;
        return new AzureAccountHost(accountName, "", false, endpoint);
    }

    private static void validateEndpoint(URI endpoint) {
        if (!("http".equalsIgnoreCase(endpoint.getScheme())
                || "https".equalsIgnoreCase(endpoint.getScheme()))
                || endpoint.getRawUserInfo() != null || endpoint.getRawQuery() != null
                || endpoint.getRawFragment() != null) {
            throw new StoragePropertiesException(
                    "Azure endpoint must use HTTP(S) and must not contain credentials, query or fragment");
        }
    }

    /** Creates a public-cloud account host when only the account name is configured. */
    public static AzureAccountHost fromAccountName(String accountName) {
        if (accountName == null || accountName.isBlank()) {
            throw new StoragePropertiesException("Azure account name must not be empty");
        }
        return parse(accountName.trim() + BLOB_MARKER + DEFAULT_CLOUD_SUFFIX);
    }

    public String accountName() {
        return accountName;
    }

    public String cloudSuffix() {
        return cloudSuffix;
    }

    public String dfsHost() {
        return cloudSuffix.isEmpty() ? endpoint.getHost() : accountName + DFS_MARKER + cloudSuffix;
    }

    public String blobHost() {
        return cloudSuffix.isEmpty() ? endpoint.getHost() : accountName + BLOB_MARKER + cloudSuffix;
    }

    public String blobEndpoint() {
        if (!dfsHost || !AZURE_CLOUD_SUFFIXES.contains(cloudSuffix.toLowerCase(Locale.ROOT))) {
            return endpoint.toString();
        }
        String value = endpoint.toString();
        int hostStart = value.indexOf("://") + 3;
        // Replace only the authority host: URI reconstruction would decode/re-encode paths.
        return value.substring(0, hostStart) + blobHost()
                + value.substring(hostStart + endpoint.getHost().length());
    }

    public boolean isDfsHost() {
        return dfsHost;
    }
}
