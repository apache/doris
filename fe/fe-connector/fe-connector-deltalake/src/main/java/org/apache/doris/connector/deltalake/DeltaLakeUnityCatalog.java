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

package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.metastore.deltalake.uc.UnityCatalogProperties;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.DorisConnectorException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.Snapshot;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.kernel.unitycatalog.UCTableIdentifier;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaTokenBasedRestClient;
import io.unitycatalog.client.auth.TokenProvider;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** UC resolves names and authorizes reads. The Delta catalog endpoint owns the managed commit window. */
final class DeltaLakeUnityCatalog implements DeltaLakeTableResolver {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final UnityCatalogProperties properties;
    private final DeltaLakeVendedFileIO.Clients clients;
    private final String api;
    private final String catalog;
    private final TokenProvider tokens;
    private final HttpClient http;
    private final UCDeltaTokenBasedRestClient commits;
    private final UCCatalogManagedClient managedTables;

    DeltaLakeUnityCatalog(UnityCatalogProperties properties, Configuration storage, ConnectorContext context) {
        this.properties = properties;
        clients = new DeltaLakeVendedFileIO.Clients(storage, context);
        catalog = properties.catalog();
        api = properties.origin() + "/api/2.1/unity-catalog/";
        tokens = TokenProvider.create(properties.auth());
        http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        Map<String, String> config = new HashMap<>();
        config.put("uri", properties.origin());
        properties.auth().forEach((key, value) -> config.put("auth." + key, value));
        commits = new UCDeltaTokenBasedRestClient(config, () -> storage);
        managedTables = new UCCatalogManagedClient(commits);
    }

    @Override
    public List<String> listDatabases() throws IOException {
        return list("schemas", "catalog_name=" + encode(catalog));
    }

    @Override
    public List<String> listTables(String database) throws IOException {
        return list("tables", "catalog_name=" + encode(catalog) + "&schema_name=" + encode(database));
    }

    private List<String> list(String field, String query) throws IOException {
        List<String> names = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        String token = "";
        do {
            String next = token.isEmpty() ? "" : "&page_token=" + encode(token);
            JsonNode response = request(field + "?" + query + "&max_results=100" + next, null, false);
            for (JsonNode entry : response.path(field)) {
                if (!"tables".equals(field) || isDelta(entry)) {
                    names.add(required(entry, "name"));
                }
            }
            token = response.path("next_page_token").asText("");
            if (!token.isEmpty() && !seen.add(token)) {
                throw new IOException("Unity Catalog returned a repeated pagination token for " + field);
            }
        } while (!token.isEmpty());
        Collections.sort(names);
        return names;
    }

    @Override
    public Optional<DeltaLakeTableReference> tableLocation(String database, String table) throws IOException {
        // UC's tables API identifies tables by one three-part name; dots in a part are not valid UC names.
        if (database.contains(".") || table.contains(".")) {
            return Optional.empty();
        }
        String name = catalog + "." + database + "." + table;
        JsonNode entry = request("tables/" + encode(name), null, true);
        if (entry == null || !isDelta(entry)) {
            return Optional.empty();
        }
        if (entry.path("browse_only").asBoolean() || entry.hasNonNull("row_filter")) {
            throw new DorisConnectorException("Unity Catalog table is browse-only or has a row filter: " + name);
        }
        for (JsonNode column : entry.path("columns")) {
            if (column.hasNonNull("mask")) {
                throw new DorisConnectorException("Unity Catalog column masks are unsupported for " + name);
            }
        }
        String id = required(entry, "table_id");
        String location = required(entry, "storage_location");
        // Metadata visibility alone does not authorize raw file reads. Ask UC for READ even when the
        // catalog supplies its own storage key. A denied grant never falls back to that key.
        boolean managed = "MANAGED".equals(entry.path("table_type").asText());
        DeltaLakeTableReference reference = new DeltaLakeTableReference(location, id, managed);
        JsonNode grant = readGrant(reference);
        if (needsVendedStorage(reference)) {
            reference.vendedEngine = DefaultEngine.create(new DeltaLakeVendedFileIO(clients,
                    grant(reference, grant), () -> grant(reference, readGrant(reference))));
        }
        return Optional.of(reference);
    }

    private JsonNode readGrant(DeltaLakeTableReference reference) throws IOException {
        return request("temporary-table-credentials", JSON.createObjectNode().put("table_id", reference.registrationId)
                .put("operation", "READ"), false);
    }

    private boolean needsVendedStorage(DeltaLakeTableReference reference) {
        String scheme = URI.create(reference.path).getScheme();
        if ("file".equalsIgnoreCase(scheme)) {
            String host = URI.create(properties.origin()).getHost();
            if (!Set.of("localhost", "127.0.0.1", "[::1]").contains(host)) {
                throw new DorisConnectorException("UC local-file storage requires a local Unity Catalog server");
            }
            return false;
        }
        if (properties.configuredStorage()) {
            if (reference.managed) {
                throw new DorisConnectorException("Configured cloud credentials require a UC EXTERNAL table");
            }
            return false;
        }
        if (!"s3".equalsIgnoreCase(scheme) && !"s3a".equalsIgnoreCase(scheme)) {
            throw new DorisConnectorException("Unsupported UC-vended storage scheme: " + scheme);
        }
        return true;
    }

    Engine engine(DeltaLakeTableReference reference, Engine configured) {
        if (!needsVendedStorage(reference)) {
            return configured;
        }
        synchronized (reference) {
            if (reference.vendedEngine == null) {
                // A deserialized handle and background statistics retain only the registered table coordinates.
                reference.vendedEngine = DefaultEngine.create(new DeltaLakeVendedFileIO(clients, null,
                        () -> grant(reference, readGrant(reference))));
            }
            return reference.vendedEngine;
        }
    }

    private static DeltaLakeVendedFileIO.Grant grant(DeltaLakeTableReference reference, JsonNode response)
            throws IOException {
        return new DeltaLakeVendedFileIO.Grant(reference.registrationId, awsCredentials(response),
                response.path("expiration_time").asLong(0));
    }

    Map<String, String> backendProperties(DeltaLakeTableReference reference, Map<String, String> configured)
            throws IOException {
        // A cached handle may predate a revocation. Recheck UC before preparing each BE scan.
        JsonNode grant = readGrant(reference);
        if (!needsVendedStorage(reference)) {
            return configured;
        }
        Map<String, String> result = new HashMap<>(awsCredentials(grant));
        // Only retain transport properties. A catalog role, secret or profile must not override a table grant.
        for (String key : List.of("AWS_REGION", "AWS_ENDPOINT", "use_path_style",
                "AWS_MAX_CONNECTIONS", "AWS_REQUEST_TIMEOUT_MS", "AWS_CONNECTION_TIMEOUT_MS")) {
            if (configured.containsKey(key)) {
                result.put(key, configured.get(key));
            }
        }
        return result;
    }

    private static Map<String, String> awsCredentials(JsonNode grant) throws IOException {
        // The OSS static-session vendor omits expiration_time. Match the UC SDK and let storage
        // validate that token. A supplied expiry must be current.
        if (grant.hasNonNull("expiration_time")
                && grant.path("expiration_time").asLong() <= System.currentTimeMillis()) {
            throw new IOException("UC returned an expired table READ grant");
        }
        JsonNode aws = grant.path("aws_temp_credentials");
        return Map.of("AWS_ACCESS_KEY", required(aws, "access_key_id"), "AWS_SECRET_KEY",
                required(aws, "secret_access_key"), "AWS_TOKEN", required(aws, "session_token"));
    }

    @Override
    public Snapshot managedSnapshot(Engine engine, String database, String table, DeltaLakeTableReference reference,
            Optional<Long> version) {
        return managedTables.loadSnapshot(engine, reference.registrationId, reference.path,
                new UCTableIdentifier(catalog, database, table), version, Optional.empty());
    }

    private JsonNode request(String resource, JsonNode body, boolean allowMissing) throws IOException {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(api + resource)).timeout(Duration.ofSeconds(30))
                .header("Accept", "application/json");
        String token = tokens.accessToken();
        if (!token.isEmpty()) {
            request.header("Authorization", "Bearer " + token);
        }
        if (body != null) {
            request.header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body.toString()));
        }
        HttpResponse<String> response;
        try {
            response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted reading Unity Catalog", e);
        }
        if (response.statusCode() == 404 && allowMissing) {
            return null;
        }
        if (response.statusCode() != 200) {
            // In particular, never include credential response bodies in diagnostics.
            throw new DorisConnectorException("Unity Catalog " + resource.split("[?/]")[0]
                    + " returned HTTP " + response.statusCode());
        }
        return JSON.readTree(response.body());
    }

    private static String required(JsonNode entry, String field) throws IOException {
        String value = entry.path(field).asText("");
        if (value.isEmpty()) {
            throw new IOException("Missing " + field + " in Unity Catalog response");
        }
        return value;
    }

    private static boolean isDelta(JsonNode entry) {
        String type = entry.path("table_type").asText();
        return ("EXTERNAL".equals(type) || "MANAGED".equals(type))
                && "DELTA".equals(entry.path("data_source_format").asText());
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    @Override
    public void close() throws IOException {
        try {
            commits.close();
        } finally {
            clients.close();
        }
    }
}
