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
     * @param path the Azure storage path
     * @return parsed AzureUri
     * @throws IOException if the path cannot be parsed
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

        if (scheme.equals("wasb") || scheme.equals("wasbs")
                || scheme.equals("abfs") || scheme.equals("abfss")) {
            return parseWasbAbfs(scheme, rest, path);
        } else if (scheme.equals("https") || scheme.equals("http")) {
            return parseHttps(scheme, rest);
        } else if (scheme.equals("s3") || scheme.equals("s3a") || scheme.equals("s3n")) {
            return parseS3Compat(scheme, rest);
        } else {
            throw new IOException("Unsupported Azure URI scheme '" + scheme + "' in path: " + path);
        }
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
        return new AzureUri(scheme, accountName, container, key);
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
        return new AzureUri(scheme, accountName, container, key);
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
        return new AzureUri(scheme, "", container, key);
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

    @Override
    public String toString() {
        return scheme + "://" + container + "@" + accountName + "/" + key;
    }
}
