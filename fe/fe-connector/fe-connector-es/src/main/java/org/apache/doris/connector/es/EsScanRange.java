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

import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Scan range for Elasticsearch — represents one shard to scan.
 *
 * <p>Each EsScanRange maps to one TEsScanRange in Thrift. It carries
 * the ES shard routing info needed by BE's ES HTTP scanner.</p>
 *
 * <p>Properties include: es.index, es.type, es.shard_id, and
 * es.hosts (comma-separated host:port list).</p>
 *
 * <p>{@link #getHosts()} returns plain hostnames (no port, no scheme) for
 * backend locality scheduling. {@link #getEsHosts()} returns the full
 * host:port strings that BE needs to connect to Elasticsearch.</p>
 */
public class EsScanRange implements ConnectorScanRange {

    private static final long serialVersionUID = 1L;

    public static final String PROP_INDEX = "es.index";
    public static final String PROP_TYPE = "es.type";
    public static final String PROP_SHARD_ID = "es.shard_id";
    public static final String PROP_HOSTS = "es.hosts";

    private final String indexName;
    private final String mappingType;
    private final int shardId;
    private final List<String> esHosts;
    private final List<String> plainHostnames;

    public EsScanRange(String indexName, String mappingType,
            int shardId, List<String> esHosts) {
        this.indexName = indexName;
        this.mappingType = mappingType;
        this.shardId = shardId;
        this.esHosts = esHosts != null
                ? Collections.unmodifiableList(new ArrayList<>(esHosts))
                : Collections.emptyList();
        this.plainHostnames = extractHostnames(this.esHosts);
    }

    @Override
    public ConnectorScanRangeType getRangeType() {
        return ConnectorScanRangeType.ES_SCAN;
    }

    /**
     * Returns plain hostnames for backend locality scheduling.
     * Strips port and scheme from host:port or https://host:port strings.
     */
    @Override
    public List<String> getHosts() {
        return plainHostnames;
    }

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> props = new HashMap<>();
        props.put(PROP_INDEX, indexName);
        if (mappingType != null) {
            props.put(PROP_TYPE, mappingType);
        }
        props.put(PROP_SHARD_ID, String.valueOf(shardId));
        props.put(PROP_HOSTS, String.join(",", esHosts));
        return props;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getMappingType() {
        return mappingType;
    }

    public int getShardId() {
        return shardId;
    }

    public List<String> getEsHosts() {
        return esHosts;
    }

    /**
     * Extracts plain hostnames from host:port or scheme://host:port strings,
     * preserving order and deduplicating.
     */
    static List<String> extractHostnames(List<String> hostPorts) {
        Set<String> seen = new LinkedHashSet<>();
        for (String hp : hostPorts) {
            String s = hp;
            // Strip scheme (http:// or https://)
            int schemeEnd = s.indexOf("://");
            if (schemeEnd >= 0) {
                s = s.substring(schemeEnd + 3);
            }
            // Strip port
            int colonIdx = s.lastIndexOf(':');
            if (colonIdx > 0) {
                s = s.substring(0, colonIdx);
            }
            if (!s.isEmpty()) {
                seen.add(s);
            }
        }
        return Collections.unmodifiableList(new ArrayList<>(seen));
    }

    @Override
    public String toString() {
        return "EsScanRange{index='" + indexName
                + "', shard=" + shardId
                + ", hosts=" + esHosts + "}";
    }
}
