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

package org.apache.doris.external.elasticsearch;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.catalog.Column;
import org.apache.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.json.JSONArray;
import org.json.JSONObject;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

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
    
    private Request.Builder builder;
    private String[] nodes;
    private String currentNode;
    private int currentNodeIndex = 0;
    
    public EsRestClient(String[] nodes, String authUser, String authPassword) {
        this.nodes = nodes;
        this.builder = new Request.Builder();
        if (!Strings.isEmpty(authUser) && !Strings.isEmpty(authPassword)) {
            this.builder.addHeader(HttpHeaders.AUTHORIZATION,
                    Credentials.basic(authUser, authPassword));
        }
        this.currentNode = nodes[currentNodeIndex];
    }
    
    private void selectNextNode() {
        currentNodeIndex++;
        // reroute, because the previously failed node may have already been restored
        if (currentNodeIndex >= nodes.length) {
            currentNodeIndex = 0;
        }
        currentNode = nodes[currentNodeIndex];
    }
    
    public Map<String, EsNodeInfo> getHttpNodes() throws Exception {
        Map<String, Map<String, Object>> nodesData = get("_nodes/http", "nodes");
        if (nodesData == null) {
            return Collections.emptyMap();
        }
        Map<String, EsNodeInfo> nodesMap = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : nodesData.entrySet()) {
            EsNodeInfo node = new EsNodeInfo(entry.getKey(), entry.getValue());
            if (node.hasHttp()) {
                nodesMap.put(node.getId(), node);
            }
        }
        return nodesMap;
    }
    
    public EsFieldInfo getFieldInfo(String indexName, String mappingType, List<Column> colList) throws Exception {
        String path = indexName + "/_mapping";
        String indexMapping = execute(path);
        if (indexMapping == null) {
            throw new Exception( "index[" + indexName + "] _mapping not found for the Elasticsearch Cluster");
        }
        return getFieldInfo(colList, parseProperties(indexMapping, mappingType));
    }
    
    @VisibleForTesting
    public EsFieldInfo getFieldInfo(List<Column> colList, JSONObject properties) {
        if (properties == null) {
            return null;
        }
        Map<String, String> fetchFieldMap = new HashMap<>();
        Map<String, String> docValueFieldMap = new HashMap<>();
        for (Column col : colList) {
            String colName = col.getName();
            if (!properties.has(colName)) {
                continue;
            }
            JSONObject fieldObject = properties.optJSONObject(colName);
            String fetchField = EsUtil.getFetchField(fieldObject, colName);
            if (StringUtils.isNotEmpty(fetchField)) {
                fetchFieldMap.put(colName, fetchField);
            }
            String docValueField = EsUtil.getDocValueField(fieldObject, colName);
            if (StringUtils.isNotEmpty(docValueField)) {
                docValueFieldMap.put(colName, docValueField);
            }
        }
        return new EsFieldInfo(fetchFieldMap, docValueFieldMap);
    }
    
    @VisibleForTesting
    public JSONObject parseProperties(String indexMapping, String mappingType) {
        JSONObject jsonObject = new JSONObject(indexMapping);
        // the indexName use alias takes the first mapping
        Iterator<String> keys = jsonObject.keys();
        String docKey = keys.next();
        JSONObject docData = jsonObject.optJSONObject(docKey);
        JSONObject mappings = docData.optJSONObject("mappings");
        JSONObject rootSchema = mappings.optJSONObject(mappingType);
        return rootSchema.optJSONObject("properties");
    }
    
    public EsIndexState getIndexState(String indexName) throws Exception {
        String path = indexName + "/_search_shards";
        String shardLocation = execute(path);
        if (shardLocation == null) {
            throw new Exception( "index[" + indexName + "] _search_shards not found for the Elasticsearch Cluster");
        }
        return parseIndexState(indexName, shardLocation);
    }
    
    @VisibleForTesting
    public EsIndexState parseIndexState(String indexName, String shardLocation) {
        EsIndexState indexState = new EsIndexState(indexName);
        JSONObject jsonObject = new JSONObject(shardLocation);
        JSONObject nodesMap = jsonObject.getJSONObject("nodes");
        JSONArray shards = jsonObject.getJSONArray("shards");
        int length = shards.length();
        for (int i = 0; i < length; i++) {
            List<EsShardRouting> singleShardRouting = Lists.newArrayList();
            JSONArray shardsArray = shards.getJSONArray(i);
            int arrayLength = shardsArray.length();
            for (int j = 0; j < arrayLength; j++) {
                JSONObject shard = shardsArray.getJSONObject(j);
                String shardState = shard.getString("state");
                if ("STARTED".equalsIgnoreCase(shardState) ||
                        "RELOCATING".equalsIgnoreCase(shardState)) {
                    try {
                        singleShardRouting.add(EsShardRouting.parseShardRoutingV55(shardState,
                                String.valueOf(i), shard, nodesMap));
                    } catch (Exception e) {
                        LOG.info(
                                "errors while parse shard routing from json [{}], ignore this shard",
                                shard, e);
                    }
                }
            }
            if (singleShardRouting.isEmpty()) {
                LOG.warn("could not find a healthy allocation for [{}][{}]", indexName, i);
            }
            indexState.addShardRouting(i, singleShardRouting);
        }
        return indexState;
    }
    
    /**
     * Get the Elasticsearch cluster version
     *
     * @return
     */
    public EsMajorVersion version () throws Exception {
        Map<String, String> versionMap = get("/", "version");
        
        EsMajorVersion majorVersion;
        try {
            majorVersion = EsMajorVersion.parse(versionMap.get("version"));
        } catch (Exception e) {
            LOG.warn("detect es version failure on node [{}]", currentNode);
            return EsMajorVersion.V_5_X;
        }
        return majorVersion;
    }
    
    /**
     * execute request for specific path，it will try again nodes.length times if it fails
     *
     * @param path the path must not leading with '/'
     * @return response
     */
    private String execute (String path) throws Exception {
        int retrySize = nodes.length;
        Exception scratchExceptionForThrow = null;
        for (int i = 0; i < retrySize; i++) {
            // maybe should add HTTP schema to the address
            // actually, at this time we can only process http protocol
            // NOTE. currentNode may have some spaces.
            // User may set a config like described below:
            // hosts: "http://192.168.0.1:8200, http://192.168.0.2:8200"
            // then currentNode will be "http://192.168.0.1:8200", " http://192.168.0.2:8200"
            currentNode = currentNode.trim();
            if (!(currentNode.startsWith("http://") || currentNode.startsWith("https://"))) {
                currentNode = "http://" + currentNode;
            }
            Request request = builder.get()
                    .url(currentNode + "/" + path)
                    .build();
            Response response = null;
            if (LOG.isTraceEnabled()) {
                LOG.trace("es rest client request URL: {}", currentNode + "/" + path);
            }
            try {
                response = networkClient.newCall(request).execute();
                if (response.isSuccessful()) {
                    return response.body().string();
                }
            } catch (IOException e) {
                LOG.warn("request node [{}] [{}] failures {}, try next nodes", currentNode, path, e);
                scratchExceptionForThrow = e;
            } finally {
                if (response != null) {
                    response.close();
                }
            }
            selectNextNode();
        }
        LOG.warn("try all nodes [{}],no other nodes left", nodes);
        if (scratchExceptionForThrow != null) {
            throw scratchExceptionForThrow;
        }
        return null;
    }
    
    public <T > T get(String q, String key) throws Exception {
        return parseContent(execute(q), key);
    }
    
    @SuppressWarnings("unchecked")
    private <T > T parseContent(String response, String key) {
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