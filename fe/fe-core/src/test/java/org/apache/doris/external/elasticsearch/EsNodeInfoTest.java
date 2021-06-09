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

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

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
