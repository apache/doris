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

import java.util.HashMap;
import java.util.Map;

/**
 * Fetch resolved indices's search shards from remote ES Cluster
 */
public class PartitionPhase implements SearchPhase {

    private EsRestClient client;
    private EsShardPartitions shardPartitions;
    private Map<String, EsNodeInfo> nodesInfo;

    public PartitionPhase(EsRestClient client) {
        this.client = client;
    }

    @Override
    public void execute(SearchContext context) throws DorisEsException {
        shardPartitions = client.searchShards(context.sourceIndex());
        if (context.nodesDiscovery()) {
            nodesInfo = client.getHttpNodes();
        } else {
            nodesInfo = new HashMap<>();
            String[] seeds = context.esTable().getSeeds();
            for (int i = 0; i < seeds.length; i++) {
                nodesInfo.put(String.valueOf(i), new EsNodeInfo(String.valueOf(i), seeds[i]));
            }
        }
    }


    @Override
    public void postProcess(SearchContext context) throws DorisEsException {
        context.partitions(shardPartitions);
        context.partitions().addHttpAddress(nodesInfo);
    }
}
