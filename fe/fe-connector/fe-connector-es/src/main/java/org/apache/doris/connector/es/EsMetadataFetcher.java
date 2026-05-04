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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Orchestrates ES metadata fetching — mapping resolution and shard routing discovery.
 * Adapted from fe-core's EsMetaStateTracker — no EsExternalTable/UserException dependencies.
 *
 * <p>Execution sequence:
 * <ol>
 *   <li>Fetch index mapping → resolve field contexts (keyword, doc_value, date compat)</li>
 *   <li>Fetch search shards → resolve shard routing with node HTTP addresses</li>
 * </ol>
 */
public class EsMetadataFetcher {

    private static final Logger LOG = LogManager.getLogger(EsMetadataFetcher.class);

    private final EsConnectorRestClient restClient;
    private final EsMetadataState state;

    public EsMetadataFetcher(EsConnectorRestClient restClient, EsMetadataState state) {
        this.restClient = restClient;
        this.state = state;
    }

    /**
     * Fetch full metadata: mapping fields + shard routing.
     */
    public EsMetadataState fetch() {
        fetchMapping();
        fetchShards();
        return state;
    }

    private void fetchMapping() {
        String indexMapping = restClient.getMapping(state.getSourceIndex());
        EsFieldContext fieldContext = EsMappingUtils.resolveFieldContext(
                state.getColumnNames(),
                state.getSourceIndex(),
                indexMapping,
                state.getMappingType());
        state.setFieldContext(fieldContext);
        if (LOG.isDebugEnabled()) {
            LOG.debug("resolved field context for [{}]: {}", state.getSourceIndex(), fieldContext);
        }
    }

    private void fetchShards() {
        EsShardPartitions shardPartitions = restClient.searchShards(state.getSourceIndex());

        Map<String, EsNodeInfo> nodesInfo;
        if (state.isNodesDiscovery()) {
            nodesInfo = restClient.getHttpNodes();
        } else {
            nodesInfo = new HashMap<>();
            String[] seeds = state.getSeeds();
            for (int i = 0; i < seeds.length; i++) {
                nodesInfo.put(String.valueOf(i), new EsNodeInfo(String.valueOf(i), seeds[i]));
            }
        }

        shardPartitions.addHttpAddress(nodesInfo);
        state.setShardPartitions(shardPartitions);
        if (LOG.isDebugEnabled()) {
            LOG.debug("resolved shard partitions for [{}]: {}", state.getSourceIndex(), shardPartitions);
        }
    }
}
