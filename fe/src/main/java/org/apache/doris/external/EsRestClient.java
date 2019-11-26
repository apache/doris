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

package org.apache.doris.external;

import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class EsRestClient {
    private static final Logger LOG = LogManager.getLogger(EsRestClient.class);
    private ObjectMapper mapper;

    {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationConfig.Feature.USE_ANNOTATIONS, false);
        mapper.configure(SerializationConfig.Feature.USE_ANNOTATIONS, false);
    }

    private static OkHttpClient networkClient = new OkHttpClient.Builder()
            .readTimeout(10, TimeUnit.SECONDS)
            .build();

    private String basicAuth;

    private int nextClient = 0;
    private String[] nodes;
    private String currentNode;

    public EsRestClient(String[] nodes, String authUser, String authPassword) {
        this.nodes = nodes;
        if (!Strings.isEmpty(authUser) && !Strings.isEmpty(authPassword)) {
            basicAuth = Credentials.basic(authUser, authPassword);
        }
        selectNextNode();
    }

    private boolean selectNextNode() {
        if (nextClient >= nodes.length) {
            return false;
        }
        currentNode = nodes[nextClient++];
        return true;
    }

    public Map<String, EsNodeInfo> getHttpNodes() throws Exception {
        Map<String, Map<String, Object>> nodesData = get("_nodes/http", "nodes");
        if (nodesData == null) {
            return Collections.emptyMap();
        }
        Map<String, EsNodeInfo> nodes = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : nodesData.entrySet()) {
            EsNodeInfo node = new EsNodeInfo(entry.getKey(), entry.getValue());
            if (node.hasHttp()) {
                nodes.put(node.getId(), node);
            }
        }
        return nodes;
    }

    public String getIndexMetaData(String indexName) {
        String path = "_cluster/state?indices=" + indexName
                + "&metric=routing_table,nodes,metadata&expand_wildcards=open";
        return execute(path);

    }

    /**
     * execute request for specific path
     *
     * @param path the path must not leading with '/'
     * @return
     */
    private String execute(String path) {
        selectNextNode();
        boolean nextNode;
        do {
            Request.Builder builder = new Request.Builder();
            if (!Strings.isEmpty(basicAuth)) {
                builder.addHeader("Authorization", basicAuth);
            }

            // maybe should add HTTP schema to the address
            // actually, at this time we can only process http protocol
            if (!(currentNode.startsWith("http://") || currentNode.startsWith("https://"))) {
                currentNode = "http://" + currentNode;
            }

            Request request = builder.get()
                    .url(currentNode + "/" + path)
                    .build();
            LOG.trace("es rest client request URL: {}", currentNode + "/" + path);
            try {
                Response response = networkClient.newCall(request).execute();
                if (response.isSuccessful()) {
                    return response.body().string();
                }
            } catch (IOException e) {
                LOG.warn("request node [{}] [{}] failures {}, try next nodes", currentNode, path, e);
            }
            nextNode = selectNextNode();
            if (!nextNode) {
                LOG.warn("try all nodes [{}],no other nodes left", nodes);
            }
        } while (nextNode);
        return null;
    }

    public <T> T get(String q, String key) {
        return parseContent(execute(q), key);
    }

    private <T> T parseContent(String response, String key) {
        Map<String, Object> map = Collections.emptyMap();
        try {
            JsonParser jsonParser = mapper.getJsonFactory().createJsonParser(response);
            map = mapper.readValue(jsonParser, Map.class);
        } catch (IOException ex) {
            LOG.error("parse es response failure: [{}]", response);
        }
        return (T) (key != null ? map.get(key) : map);
    }

}
