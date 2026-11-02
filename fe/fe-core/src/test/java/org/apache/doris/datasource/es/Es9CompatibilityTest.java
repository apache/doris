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

package org.apache.doris.datasource.es;

import org.apache.doris.catalog.EsResource;
import org.apache.doris.catalog.EsTable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Opt-in live compatibility checks. Set DORIS_ES9_TEST_URL, DORIS_ES9_TEST_USER and
 * DORIS_ES9_TEST_PASSWORD to an ES 9 instance with permission to create a temporary test index.
 */
@EnabledIfEnvironmentVariable(named = "DORIS_ES9_TEST_URL", matches = ".+")
class Es9CompatibilityTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final OkHttpClient http = new OkHttpClient.Builder().connectTimeout(10, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS).build();
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
        HttpResult response = request("DELETE", "/" + index, "");
        Assertions.assertTrue(response.statusCode() == 200 || response.statusCode() == 404);
    }

    @Test
    void metadataDiscoveryAndSeedRoutingAcceptNine() throws Exception {
        EsRestClient client = client(password);
        Assertions.assertTrue(client.existIndex(http, index));
        Assertions.assertFalse(client.getHttpNodes().isEmpty());
        Assertions.assertTrue(client.listTable(false).contains(index));
        Assertions.assertEquals(2, client.searchShards(index).getShardRoutings().size());
        Assertions.assertEquals(2, EsUtil.genColumnsFromEs(client, index, null, false, new HashMap<>()).size());
        for (boolean discover : new boolean[]{false, true}) {
            EsTable table = new EsTable(1, index,
                    EsUtil.genColumnsFromEs(client, index, null, false, new HashMap<>()),
                    ImmutableMap.of(EsResource.HOSTS, endpoint, EsResource.INDEX, index,
                            EsResource.NODES_DISCOVERY, Boolean.toString(discover)), null);
            EsMetaStateTracker tracker = new EsMetaStateTracker(client, table);
            tracker.run();
            SearchContext fetched = tracker.searchContext();
            Assertions.assertEquals(2, fetched.partitions().getShardRoutings().size());
            Assertions.assertEquals(ImmutableMap.of("id", "id", "name", "name"), fetched.docValueFieldsContext());
            fetched.partitions().getShardRoutings().values().forEach(routes -> routes.forEach(route -> {
                Assertions.assertNotNull(route.getHttpAddress().getHostname());
                Assertions.assertTrue(route.getHttpAddress().getPort() > 0);
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
                            ImmutableMap.of("scroll", "1m", "scroll_id", scrollId)));
                    scrollId = response.path("_scroll_id").asText();
                }
                Assertions.assertEquals(ImmutableSet.of(1, 2, 3), ids);
                Assertions.assertEquals(3, pages);
            } finally {
                Assertions.assertTrue(jsonRequest("DELETE", "/_search/scroll",
                        JSON.writeValueAsString(ImmutableMap.of("scroll_id", scrollId))).path("succeeded").asBoolean());
            }
        }
    }

    @Test
    void invalidCredentialsFailMetadataLoading() {
        Assertions.assertThrows(DorisEsException.class, () -> client(password + "-invalid").getMapping(index));
    }

    private EsRestClient client(String secret) {
        return new EsRestClient(new String[]{endpoint}, user, secret, false);
    }

    private JsonNode jsonRequest(String method, String path, String body) throws Exception {
        HttpResult response = request(method, path, body);
        Assertions.assertTrue(response.statusCode() >= 200 && response.statusCode() < 300,
                () -> method + " " + path + " returned HTTP " + response.statusCode());
        return JSON.readTree(response.body());
    }

    private HttpResult request(String method, String path, String body) throws Exception {
        String credentials = Base64.getEncoder().encodeToString((user + ":" + password)
                .getBytes(StandardCharsets.UTF_8));
        RequestBody payload = "GET".equals(method) ? null
                : RequestBody.create(MediaType.parse("application/json"), body);
        Request request = new Request.Builder().url(endpoint + path)
                .header("Authorization", "Basic " + credentials).method(method, payload).build();
        try (Response response = http.newCall(request).execute()) {
            return new HttpResult(response.code(), response.body() == null ? "" : response.body().string());
        }
    }

    private static final class HttpResult {
        private final int code;
        private final String body;

        private HttpResult(int code, String body) {
            this.code = code;
            this.body = body;
        }

        int statusCode() {
            return code;
        }

        String body() {
            return body;
        }
    }
}
