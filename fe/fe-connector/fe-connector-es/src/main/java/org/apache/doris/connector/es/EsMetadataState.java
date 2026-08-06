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

import java.util.Collections;
import java.util.List;

/**
 * Full metadata state for an ES index, including field contexts, shard routing, and version.
 * Adapted from fe-core's SearchContext — no EsExternalTable/Column dependencies.
 */
public class EsMetadataState {

    private final String sourceIndex;
    private final String mappingType;
    private final List<String> columnNames;
    private final boolean nodesDiscovery;
    private final String[] seeds;

    private EsFieldContext fieldContext;
    private EsShardPartitions shardPartitions;
    private EsMajorVersion version;

    public EsMetadataState(String sourceIndex, String mappingType,
            List<String> columnNames, boolean nodesDiscovery, String[] seeds) {
        this.sourceIndex = sourceIndex;
        this.mappingType = mappingType;
        this.columnNames = columnNames != null ? columnNames : Collections.emptyList();
        this.nodesDiscovery = nodesDiscovery;
        this.seeds = seeds;
    }

    public String getSourceIndex() {
        return sourceIndex;
    }

    public String getMappingType() {
        return mappingType;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public boolean isNodesDiscovery() {
        return nodesDiscovery;
    }

    public String[] getSeeds() {
        return seeds;
    }

    public EsFieldContext getFieldContext() {
        return fieldContext;
    }

    public void setFieldContext(EsFieldContext fieldContext) {
        this.fieldContext = fieldContext;
    }

    public EsShardPartitions getShardPartitions() {
        return shardPartitions;
    }

    public void setShardPartitions(EsShardPartitions shardPartitions) {
        this.shardPartitions = shardPartitions;
    }

    public EsMajorVersion getVersion() {
        return version;
    }

    public void setVersion(EsMajorVersion version) {
        this.version = version;
    }
}
