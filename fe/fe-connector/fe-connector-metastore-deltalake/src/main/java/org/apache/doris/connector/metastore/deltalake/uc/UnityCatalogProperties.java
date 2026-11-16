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

package org.apache.doris.connector.metastore.deltalake.uc;

import org.apache.doris.connector.metastore.spi.AbstractMetaStoreProperties;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;

import java.net.URI;
import java.util.Map;

/** Settings for one Unity Catalog namespace. The storage identity is bound separately. */
public final class UnityCatalogProperties extends AbstractMetaStoreProperties {
    public static final String URI_KEY = "deltalake.uc.uri";
    public static final String NAMESPACE = "deltalake.uc.catalog";
    public static final String TOKEN = "deltalake.uc.token";
    public static final String CLIENT_ID = "deltalake.uc.oauth.client-id";
    public static final String SECRET = "deltalake.uc.oauth.client-secret";
    public static final String TOKEN_URI = "deltalake.uc.oauth.token-uri";

    @ConnectorProperty(names = {URI_KEY}, required = true, description = "Unity Catalog server or workspace origin.")
    private String uri;

    @ConnectorProperty(names = {NAMESPACE}, required = true, description = "Unity Catalog catalog name.")
    private String catalog;

    @ConnectorProperty(names = {TOKEN}, required = false, sensitive = true, description = "UC bearer token.")
    private String token;

    @ConnectorProperty(names = {CLIENT_ID}, required = false, description = "OAuth service principal client ID.")
    private String clientId;

    @ConnectorProperty(names = {SECRET}, required = false, sensitive = true, description = "OAuth client secret.")
    private String secret;

    @ConnectorProperty(names = {TOKEN_URI}, required = false, description = "OAuth machine-to-machine token endpoint.")
    private String tokenUri;

    @ConnectorProperty(names = {"deltalake.uc.storage.mode"}, required = false,
            description = "Use UC-vended credentials, or configured storage credentials for external tables.")
    private String storageMode = "vended";

    private UnityCatalogProperties(Map<String, String> raw) {
        super(raw);
        ConnectorPropertiesUtils.bindConnectorProperties(this, raw);
    }

    static UnityCatalogProperties of(Map<String, String> raw) {
        return new UnityCatalogProperties(raw);
    }

    @Override
    public String providerName() {
        return "UC";
    }

    @Override
    public boolean needsStorage() {
        return true;
    }

    @Override
    public void validate() {
        if (uri == null || uri.isBlank() || catalog == null || catalog.isBlank() || catalog.contains(".")) {
            throw new IllegalArgumentException(URI_KEY + " and a single UC catalog name in " + NAMESPACE
                    + " are required");
        }
        if (!"vended".equals(storageMode) && !"configured".equals(storageMode)) {
            throw new IllegalArgumentException("deltalake.uc.storage.mode must be vended or configured");
        }
        URI endpoint = endpoint(uri, URI_KEY);
        if (!endpoint.getPath().isEmpty() && !"/".equals(endpoint.getPath())) {
            throw new IllegalArgumentException(URI_KEY + " must be the server origin without an API path");
        }
        if (clientId != null || secret != null || tokenUri != null) {
            if (token != null || clientId == null || clientId.isBlank() || secret == null || secret.isBlank()
                    || tokenUri == null || tokenUri.isBlank()) {
                throw new IllegalArgumentException("UC OAuth requires client-id, client-secret and token-uri together"
                        + " and cannot be combined with a static token");
            }
            endpoint(tokenUri, TOKEN_URI);
        }
    }

    private static URI endpoint(String value, String key) {
        URI result = URI.create(value);
        if (!("https".equals(result.getScheme()) || "http".equals(result.getScheme())) || result.getHost() == null
                || result.getUserInfo() != null || result.getQuery() != null || result.getFragment() != null) {
            throw new IllegalArgumentException("Invalid HTTP(S) endpoint for " + key);
        }
        return result;
    }

    public String origin() {
        return uri.endsWith("/") ? uri.substring(0, uri.length() - 1) : uri;
    }

    public boolean configuredStorage() {
        return "configured".equals(storageMode);
    }

    public String catalog() {
        return catalog;
    }

    public Map<String, String> auth() {
        if (clientId != null) {
            return Map.of("type", "oauth", "oauth.clientId", clientId, "oauth.clientSecret", secret,
                    "oauth.uri", tokenUri);
        }
        return Map.of("type", "static", "token", token == null ? "" : token);
    }
}
