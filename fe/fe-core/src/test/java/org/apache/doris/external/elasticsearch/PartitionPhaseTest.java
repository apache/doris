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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.EsTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.datasource.es.EsNodeInfo;
import org.apache.doris.datasource.es.EsRestClient;
import org.apache.doris.datasource.es.EsShardPartitions;
import org.apache.doris.datasource.es.PartitionPhase;
import org.apache.doris.datasource.es.SearchContext;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import mockit.Expectations;
import mockit.Injectable;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartitionPhaseTest extends EsTestCase {

    @Test
    public void testWorkFlow(@Injectable EsRestClient client) throws Exception {
        final EsShardPartitions[] esShardPartitions = {null};
        ExceptionChecker.expectThrowsNoException(() ->
                esShardPartitions[0] = EsShardPartitions.findShardPartitions("doe",
                        loadJsonFromFile("data/es/test_search_shards.json")));
        Assert.assertNotNull(esShardPartitions[0]);
        ObjectMapper mapper = new ObjectMapper();
        JsonParser jsonParser = mapper.getJsonFactory().createJsonParser(loadJsonFromFile("data/es/test_nodes_http.json"));
        Map<String, Map<String, Object>> nodesData = (Map<String, Map<String, Object>>) mapper.readValue(jsonParser, Map.class).get("nodes");
        Map<String, EsNodeInfo> nodesMap = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : nodesData.entrySet()) {
            EsNodeInfo node = new EsNodeInfo(entry.getKey(), entry.getValue(), false);
            if (node.hasHttp()) {
                nodesMap.put(node.getId(), node);
            }
        }

        new Expectations(client) {
            {
                client.getHttpNodes();
                minTimes = 0;
                result = nodesMap;

                client.searchShards("doe");
                minTimes = 0;
                result = esShardPartitions[0];
            }
        };
        List<Column> columns = new ArrayList<>();
        Column k1 = new Column("k1", PrimitiveType.BIGINT);
        columns.add(k1);
        EsTable esTableBefore7X = fakeEsTable("doe", "doe", "doc", columns);
        SearchContext context = new SearchContext(esTableBefore7X);
        PartitionPhase partitionPhase = new PartitionPhase(client);
        ExceptionChecker.expectThrowsNoException(() -> partitionPhase.execute(context));
        ExceptionChecker.expectThrowsNoException(() -> partitionPhase.postProcess(context));
        Assert.assertNotNull(context.tablePartitions());
    }
}
