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

/** Parsed Azure account authority shared by native, Hadoop, and FileIO renderers. */
public final class AzureAccountHost {

    private static final String DEFAULT_CLOUD_SUFFIX = "core.windows.net";
    private static final String DFS_MARKER = ".dfs.";
    private static final String BLOB_MARKER = ".blob.";

    private final String accountName;
    private final String cloudSuffix;
    private final boolean dfsHost;

    private AzureAccountHost(String accountName, String cloudSuffix, boolean dfsHost) {
        this.accountName = accountName;
        this.cloudSuffix = cloudSuffix;
        this.dfsHost = dfsHost;
    }

    /**
     * Parses an account host or endpoint such as {@code account.dfs.core.windows.net}.
     * A URI scheme is optional; only the authority host is retained.
     */
    public static AzureAccountHost parse(String hostOrEndpoint) {
        if (hostOrEndpoint == null || hostOrEndpoint.isBlank()) {
            throw new StoragePropertiesException("Azure account host must not be empty");
        }
        String value = hostOrEndpoint.trim();
        String uriValue = value.contains("://") ? value : "https://" + value;
        final String host;
        try {
            host = new URI(uriValue).getHost();
        } catch (URISyntaxException e) {
            throw new StoragePropertiesException("Invalid Azure account host", e);
        }
        if (host == null || host.isBlank()) {
            throw new StoragePropertiesException("Invalid Azure account host");
        }

        String lowerHost = host.toLowerCase(Locale.ROOT);
        int dfsIndex = lowerHost.indexOf(DFS_MARKER);
        if (dfsIndex > 0) {
            return new AzureAccountHost(host.substring(0, dfsIndex),
                    host.substring(dfsIndex + DFS_MARKER.length()), true);
        }
        int blobIndex = lowerHost.indexOf(BLOB_MARKER);
        if (blobIndex > 0) {
            return new AzureAccountHost(host.substring(0, blobIndex),
                    host.substring(blobIndex + BLOB_MARKER.length()), false);
        }

        int dot = host.indexOf('.');
        String accountName = dot > 0 ? host.substring(0, dot) : host;
        String suffix = dot > 0 ? host.substring(dot + 1) : DEFAULT_CLOUD_SUFFIX;
        return new AzureAccountHost(accountName, suffix, false);
    }

    /** Creates a public-cloud account host when only the account name is configured. */
    public static AzureAccountHost fromAccountName(String accountName) {
        if (accountName == null || accountName.isBlank()) {
            throw new StoragePropertiesException("Azure account name must not be empty");
        }
        return parse(accountName.trim());
    }

    public String accountName() {
        return accountName;
    }

    public String cloudSuffix() {
        return cloudSuffix;
    }

    public String dfsHost() {
        return accountName + DFS_MARKER.substring(1) + cloudSuffix;
    }

    public String blobHost() {
        return accountName + BLOB_MARKER.substring(1) + cloudSuffix;
    }

    public String blobEndpoint() {
        return "https://" + blobHost();
    }

    public boolean isDfsHost() {
        return dfsHost;
    }
}
