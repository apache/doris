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

package org.apache.doris.connector.mongodb;

import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.ReadPreference;
import org.bson.Document;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Immutable catalog configuration; query policies and driver objects are never stored in table properties. */
public final class MongoCatalogProperties {
    @ConnectorProperty(names = {"mongodb.uri"}, sensitive = true, description = "MongoDB connection URI")
    private String uri;
    @ConnectorProperty(names = {"mongodb.username"}, required = false, description = "MongoDB user")
    private String username = "";
    @ConnectorProperty(names = {"mongodb.password"}, required = false, sensitive = true,
            description = "MongoDB password")
    private String password = "";
    @ConnectorProperty(names = {"mongodb.auth_database"}, required = false, description = "Authentication database")
    private String authDatabase = "admin";
    @ConnectorProperty(names = {"mongodb.databases"}, required = false, description = "Database allowlist")
    private String databases = "";
    @ConnectorProperty(names = {"mongodb.schema.mode"}, required = false, description = "Schema source policy")
    private String schemaMode = "auto";
    @ConnectorProperty(names = {"mongodb.schema"}, required = false, description = "Explicit schema manifest")
    private String schema = "{}";
    @ConnectorProperty(names = {"mongodb.type_mismatch"}, required = false, description = "error or null")
    private String typeMismatch = "error";
    @ConnectorProperty(names = {"mongodb.datetime_timezone"}, required = false,
            description = "BSON date interpretation")
    private String datetimeTimezone = "UTC";
    @ConnectorProperty(names = {"mongodb.read_preference"}, required = false, description = "Default read routing")
    private String readPreference = "primary";
    @ConnectorProperty(names = {"mongodb.read_concern"}, required = false, description = "Default read concern")
    private String readConcern = "majority";
    @ConnectorProperty(names = {"mongodb.connect_timeout_ms"}, required = false, description = "Connection timeout")
    private int connectTimeoutMs = 10000;
    @ConnectorProperty(names = {"mongodb.server_selection_timeout_ms"}, required = false,
            description = "Server selection timeout")
    private int serverSelectionTimeoutMs = 10000;
    @ConnectorProperty(names = {"mongodb.socket_timeout_ms"}, required = false, description = "Socket timeout")
    private int socketTimeoutMs = 1000;
    @ConnectorProperty(names = {"mongodb.pool_max_clients"}, required = false, description = "Per-catalog client bound")
    private int poolMaxClients = 16;

    private ConnectionString connectionString;

    private MongoCatalogProperties() {
    }

    public static MongoCatalogProperties of(Map<String, String> properties) {
        Set<String> known = Set.of("mongodb.uri", "mongodb.username", "mongodb.password", "mongodb.auth_database",
                "mongodb.databases", "mongodb.schema.mode", "mongodb.schema", "mongodb.type_mismatch",
                "mongodb.datetime_timezone", "mongodb.read_preference", "mongodb.read_concern",
                "mongodb.connect_timeout_ms", "mongodb.server_selection_timeout_ms", "mongodb.socket_timeout_ms",
                "mongodb.pool_max_clients");
        for (String key : properties.keySet()) {
            if (key.startsWith("mongodb.") && !known.contains(key)) {
                throw new IllegalArgumentException("Unknown MongoDB catalog property: " + key);
            }
        }
        MongoCatalogProperties p = new MongoCatalogProperties();
        ConnectorPropertiesUtils.bindConnectorProperties(p, properties);
        try {
            p.connectionString = new ConnectionString(p.uri);
        } catch (RuntimeException e) {
            // Driver parse exceptions may echo a credential-bearing URI; never retain that cause.
            throw new IllegalArgumentException("Invalid mongodb.uri");
        }
        if (p.connectionString.getCredential() != null && (!p.username.isEmpty() || !p.password.isEmpty())) {
            throw new IllegalArgumentException("MongoDB credentials must use one configuration source");
        }
        if (p.username.isEmpty() && !p.password.isEmpty()) {
            throw new IllegalArgumentException("mongodb.password requires mongodb.username");
        }
        if (Boolean.TRUE.equals(p.connectionString.getSslInvalidHostnameAllowed())) {
            throw new IllegalArgumentException("MongoDB TLS hostname validation must remain enabled");
        }
        p.schemaMode = p.schemaMode.toLowerCase(Locale.ROOT);
        require(Set.of("auto", "validator", "explicit", "document").contains(p.schemaMode), "mongodb.schema.mode");
        require(Set.of("error", "null").contains(p.typeMismatch), "mongodb.type_mismatch");
        require("UTC".equals(p.datetimeTimezone), "mongodb.datetime_timezone");
        require(Set.of("local", "majority", "available").contains(p.readConcern), "mongodb.read_concern");
        require(p.connectTimeoutMs > 0 && p.connectTimeoutMs <= 300000, "mongodb.connect_timeout_ms");
        require(p.serverSelectionTimeoutMs > 0 && p.serverSelectionTimeoutMs <= 300000,
                "mongodb.server_selection_timeout_ms");
        require(p.socketTimeoutMs > 0 && p.socketTimeoutMs <= 300000, "mongodb.socket_timeout_ms");
        require(p.poolMaxClients > 0 && p.poolMaxClients <= 1024, "mongodb.pool_max_clients");
        require(p.schema.length() <= 1048576, "mongodb.schema size");
        try {
            p.readPreference = ReadPreference.valueOf(p.readPreference).getName();
            Document manifest = Document.parse(p.schema);
            require(manifest.size() <= 1024, "mongodb.schema table count");
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Invalid MongoDB read preference or schema manifest");
        }
        if (p.connectionString.getReadConcern() != null) {
            String level = p.connectionString.getReadConcern().getLevel().getValue();
            require(Set.of("local", "majority", "available").contains(level), "URI readConcernLevel");
            p.readConcern = level;
        }
        if (p.connectionString.getReadPreference() != null) {
            p.readPreference = p.connectionString.getReadPreference().getName();
        }
        return p;
    }

    private static void require(boolean condition, String property) {
        if (!condition) {
            throw new IllegalArgumentException("Invalid or unsupported " + property);
        }
    }

    MongoClientSettings clientSettings() {
        MongoClientSettings.Builder builder = MongoClientSettings.builder().applyConnectionString(connectionString)
                .applyToSocketSettings(s -> s.connectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                        .readTimeout(socketTimeoutMs, TimeUnit.MILLISECONDS))
                .applyToClusterSettings(s -> s.serverSelectionTimeout(serverSelectionTimeoutMs, TimeUnit.MILLISECONDS))
                .applyToConnectionPoolSettings(s -> s.maxSize(poolMaxClients))
                // getName() is only the scan descriptor's mode; rebuilding from it drops URI
                // tag sets, max staleness and hedge options. URI routing keeps its precedence.
                .readPreference(connectionString.getReadPreference() != null
                        ? connectionString.getReadPreference() : ReadPreference.valueOf(readPreference))
                .readConcern(new ReadConcern(ReadConcernLevel.fromString(readConcern)));
        if (!username.isEmpty()) {
            builder.credential(MongoCredential.createCredential(username, authDatabase, password.toCharArray()));
        }
        return builder.build();
    }

    public String getUri() {
        return uri;
    }

    String getUsername() {
        return username;
    }

    String getPassword() {
        return password;
    }

    String getAuthDatabase() {
        return authDatabase;
    }

    String getSchemaMode() {
        return schemaMode;
    }

    String getSchema() {
        return schema;
    }

    String getTypeMismatch() {
        return typeMismatch;
    }

    String getReadPreference() {
        return readPreference;
    }

    String getReadConcern() {
        return readConcern;
    }

    int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    int getServerSelectionTimeoutMs() {
        return serverSelectionTimeoutMs;
    }

    int getSocketTimeoutMs() {
        return socketTimeoutMs;
    }

    int getPoolMaxClients() {
        return poolMaxClients;
    }

    List<String> getDatabases() {
        return Arrays.stream(databases.split(",")).map(String::trim).filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    String configurationVersion() {
        try {
            String value = new Document("uri", uri).append("username", username).append("password", password)
                    .append("auth_database", authDatabase).append("pool", poolMaxClients)
                    .append("connect", connectTimeoutMs).append("selection", serverSelectionTimeoutMs)
                    .append("socket", socketTimeoutMs).toJson();
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required", e);
        }
    }

    @Override
    public String toString() {
        return ConnectorPropertiesUtils.toMaskedString(this);
    }
}
