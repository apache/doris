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

public class EsRestClient {
    private static final Logger LOG = LogManager.getLogger(EsRestClient.class);
    private ObjectMapper mapper;

    {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationConfig.Feature.USE_ANNOTATIONS, false);
        mapper.configure(SerializationConfig.Feature.USE_ANNOTATIONS, false);
    }

    private int nextClient = 0;
    private OkHttpClient networkClient;
    private String[] nodes;
    private String currentNode;

    public EsRestClient(String[] nodes, String authUser, String authPassword) {
        this.nodes = nodes;
        if (!Strings.isEmpty(authUser) && !Strings.isEmpty(authPassword)) {
            networkClient = new OkHttpClient.Builder().authenticator((route, response) -> {
                        String credential = Credentials.basic(authUser, authPassword);
                        return response.request().newBuilder().header("Authorization", credential).build();
                    }
            ).build();
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

    private String execute(String path) {
        selectNextNode();
        boolean nextNode;
        do {
            Request request = new Request.Builder()
                    .get()
                    .url(currentNode + "/" + path)
                    .build();
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
                LOG.error("try all nodes [{}],no other nodes left", nodes);
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
