/*********************************************************************************
 * Copyright (c)2020 CEC Health
 * FILE: EsNodeInfoTest
 * 版本      DATE             BY               REMARKS
 * ----  -----------  ---------------  ------------------------------------------
 * 1.0   2021-06-08        chenjie
 ********************************************************************************/
package org.apache.doris.external.elasticsearch;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @Description:
 * @Author: chenjie
 * @Date: 2021/6/8 8:39 下午
 * @Version V1.0
 **/
public class EsNodeInfoTest  extends EsTestCase{

    @Test
    public void parsePublishAddressTest() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonParser jsonParser = mapper.getJsonFactory().createJsonParser(loadJsonFromFile("data/es/test_nodes_http.json"));
        Map<String, Map<String, Object>> nodesData = (Map<String, Map<String, Object>>) mapper.readValue(jsonParser, Map.class).get("nodes");
        Map<String, EsNodeInfo> nodesMap = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : nodesData.entrySet()) {
            EsNodeInfo node = new EsNodeInfo(entry.getKey(), entry.getValue(), false);
            if ("node-A".equals(node.getName())) {
                assertEquals("10.0.0.1", node.getPublishAddress().hostname);
                assertEquals(8200, node.getPublishAddress().port);
            }
            if ("node-B".equals(node.getName())) {
                assertEquals("10.0.0.2", node.getPublishAddress().hostname);
                assertEquals(8200, node.getPublishAddress().port);
            }
        }
    }
}
