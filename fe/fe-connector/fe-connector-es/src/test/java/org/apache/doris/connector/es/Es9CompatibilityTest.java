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

package org.apache.doris.connector.es;

import org.apache.doris.connector.spi.DorisConnectorException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Opt-in live compatibility checks. Set DORIS_ES9_TEST_URL, DORIS_ES9_TEST_USER and
 * DORIS_ES9_TEST_PASSWORD to an ES 9 instance with permission to create a temporary test index.
 */
@EnabledIfEnvironmentVariable(named = "DORIS_ES9_TEST_URL", matches = ".+")
class Es9CompatibilityTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    private final String endpoint = System.getenv("DORIS_ES9_TEST_URL").replaceAll("/+$", "");
    private final String user = System.getenv().getOrDefault("DORIS_ES9_TEST_USER", "elastic");
    private final String password = System.getenv("DORIS_ES9_TEST_PASSWORD");
    private final String index = "doris_es9_compat_" + UUID.randomUUID().toString().replace("-", "");

    @BeforeEach
    void createFixture() throws Exception {
        Assertions.assertNotNull(password, "DORIS_ES9_TEST_PASSWORD is required");
        Assertions.assertTrue(jsonRequest("GET", "/", "").path("version").path("number").asText().startsWith("9."));
        jsonRequest("PUT", "/" + index, "{\"settings\":{\"number_of_shards\":2,\"number_of_replicas\":0},"
                + "\"mappings\":{\"properties\":{\"id\":{\"type\":\"integer\"},"
                + "\"name\":{\"type\":\"keyword\"}}}}");
        StringBuilder bulk = new StringBuilder();
        for (int id = 1; id <= 3; id++) {
            bulk.append("{\"index\":{\"_id\":\"").append(id).append("\"}}\n");
            bulk.append("{\"id\":").append(id).append(",\"name\":")
                    .append(id == 3 ? "null" : id == 1 ? "\"alpha\"" : "\"beta\"").append("}\n");
        }
        Assertions.assertFalse(jsonRequest("POST", "/" + index + "/_bulk?refresh=true", bulk.toString())
                .path("errors").asBoolean());
    }

    @AfterEach
    void removeFixture() throws Exception {
        HttpResponse<String> response = request("DELETE", "/" + index, "");
        Assertions.assertTrue(response.statusCode() == 200 || response.statusCode() == 404);
    }

    @Test
    void metadataDiscoveryAndSeedRoutingAcceptNine() {
        EsConnectorRestClient client = client(password);
        Assertions.assertTrue(client.existIndex(index));
        Assertions.assertFalse(client.getHttpNodes().isEmpty());
        Assertions.assertTrue(client.listTable(false).contains(index));
        Assertions.assertEquals(2, client.searchShards(index).getShardRoutings().size());
        Assertions.assertEquals(2, EsTypeMapping.parseMapping(index, client.getMapping(index), false).size());
        for (boolean discover : new boolean[]{false, true}) {
            EsMetadataState state = new EsMetadataState(index, null, Arrays.asList("id", "name"),
                    discover, new String[]{endpoint});
            EsMetadataState fetched = new EsMetadataFetcher(client, state, null).fetch();
            Assertions.assertEquals(2, fetched.getShardPartitions().getShardRoutings().size());
            Assertions.assertEquals(Map.of("id", "id", "name", "name"),
                    fetched.getFieldContext().getDocValueFieldsContext());
            fetched.getShardPartitions().getShardRoutings().values().forEach(routes -> routes.forEach(route -> {
                Assertions.assertNotNull(route.getHttpHost());
                Assertions.assertTrue(route.getHttpPort() > 0);
            }));
        }
    }

    @Test
    void sourceAndDocvalueScrollsReturnAllRowsAndCanBeCleared() throws Exception {
        for (boolean docvalues : new boolean[]{false, true}) {
            String projection = docvalues ? "\"_source\":false,\"docvalue_fields\":[\"id\",\"name\"]"
                    : "\"_source\":[\"id\",\"name\"]";
            JsonNode response = jsonRequest("POST", "/" + index + "/_search?scroll=1m",
                    "{\"size\":1,\"sort\":[\"_doc\"],\"query\":{\"match_all\":{}}," + projection + "}");
            String scrollId = response.path("_scroll_id").asText();
            Set<Integer> ids = new HashSet<>();
            int pages = 0;
            try {
                while (!response.path("hits").path("hits").isEmpty()) {
                    for (JsonNode hit : response.path("hits").path("hits")) {
                        JsonNode row = hit.path(docvalues ? "fields" : "_source");
                        int id = (docvalues ? row.path("id").path(0) : row.path("id")).asInt();
                        Assertions.assertTrue(ids.add(id));
                        JsonNode name = docvalues ? row.path("name").path(0) : row.path("name");
                        if (id == 3) {
                            Assertions.assertTrue(name.isNull() || name.isMissingNode());
                        } else {
                            Assertions.assertEquals(id == 1 ? "alpha" : "beta", name.asText());
                        }
                    }
                    pages++;
                    response = jsonRequest("POST", "/_search/scroll", JSON.writeValueAsString(
                            Map.of("scroll", "1m", "scroll_id", scrollId)));
                    scrollId = response.path("_scroll_id").asText();
                }
                Assertions.assertEquals(Set.of(1, 2, 3), ids);
                Assertions.assertEquals(3, pages);
            } finally {
                Assertions.assertTrue(jsonRequest("DELETE", "/_search/scroll",
                        JSON.writeValueAsString(Map.of("scroll_id", scrollId))).path("succeeded").asBoolean());
            }
        }
    }

    @Test
    void invalidCredentialsFailInsteadOfHidingIndex() {
        Assertions.assertThrows(DorisConnectorException.class, () -> client(password + "-invalid").existIndex(index));
    }

    private EsConnectorRestClient client(String secret) {
        return new EsConnectorRestClient(new String[]{endpoint}, user, secret, false, null);
    }

    private JsonNode jsonRequest(String method, String path, String body) throws Exception {
        HttpResponse<String> response = request(method, path, body);
        Assertions.assertTrue(response.statusCode() >= 200 && response.statusCode() < 300,
                () -> method + " " + path + " returned HTTP " + response.statusCode());
        return JSON.readTree(response.body());
    }

    private HttpResponse<String> request(String method, String path, String body) throws Exception {
        String credentials = Base64.getEncoder().encodeToString((user + ":" + password)
                .getBytes(StandardCharsets.UTF_8));
        HttpRequest request = HttpRequest.newBuilder(URI.create(endpoint + path)).timeout(Duration.ofSeconds(30))
                .header("Authorization", "Basic " + credentials).header("Content-Type", "application/json")
                .method(method, HttpRequest.BodyPublishers.ofString(body)).build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
