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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.EsTable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * This class encapsulates the state needed to execute a query on ES table such as fields、doc_values、resolved index、
 * search shards etc.
 * Since then, we would add more state or runtime information to this class such as
 * query builder、slice scroll context、aggregation info etc.
 **/
public class SearchContext {

    private static final Logger LOG = LogManager.getLogger(SearchContext.class);

    // fetch string field value from not analyzed fields : userId => userId.keyword
    // this is activated when `enable_keyword_sniff = true`
    private Map<String, String> fetchFieldsContext = Maps.newHashMap();
    // used to indicate which fields can get from ES docavalue
    // because elasticsearch can have "fields" feature, field can have
    // two or more types, the first type maybe have not docvalue but other
    // can have, such as (text field not have docvalue, but keyword can have):
    // "properties": {
    //      "city": {
    //        "type": "text",
    //        "fields": {
    //          "raw": {
    //            "type":  "keyword"
    //          }
    //        }
    //      }
    //    }
    // then the docvalue context provided the mapping between the select field and real request field :
    // {"city": "city.raw"}
    // use select city from table, if enable the docvalue, we will fetch the `city` field value from `city.raw`
    // fetch field value from doc_values, this is activated when `enable_docvalue_scan= true`
    private Map<String, String> docValueFieldsContext = Maps.newHashMap();

    private List<String> needCompatDateFields = Lists.newArrayList();

    // sourceIndex is the name of index when creating ES external table
    private final String sourceIndex;

    // when the `sourceIndex` is `alias` or `wildcard` matched index, this maybe involved two or more indices
    // `resolvedIndices` would return the matched underlying indices
    private List<String> resolvedIndices = Collections.emptyList();

    // `type` of the `sourceIndex`
    private final String type;


    private EsTable table;

    // all columns which user created for ES external table
    private final List<Column> fullSchema;

    // represent `resolvedIndices`'s searchable shards
    private EsShardPartitions shardPartitions;

    // the ES cluster version
    private EsMajorVersion version;

    // whether the nodes needs to be discovered
    private boolean nodesDiscovery;


    public SearchContext(EsTable table) {
        this.table = table;
        fullSchema = table.getFullSchema();
        sourceIndex = table.getIndexName();
        type = table.getMappingType();
        nodesDiscovery = table.isNodesDiscovery();
    }


    public String sourceIndex() {
        return sourceIndex;
    }

    public List<String> resolvedIndices() {
        return resolvedIndices;
    }


    public String type() {
        return type;
    }

    public List<Column> columns() {
        return fullSchema;
    }

    public EsTable esTable() {
        return table;
    }

    public Map<String, String> fetchFieldsContext() {
        return fetchFieldsContext;
    }

    public Map<String, String> docValueFieldsContext() {
        return docValueFieldsContext;
    }

    public List<String> needCompatDateFields() {
        return needCompatDateFields;
    }

    public void version(EsMajorVersion version) {
        this.version = version;
    }

    public EsMajorVersion version() {
        return version;
    }

    public void partitions(EsShardPartitions shardPartitions) {
        this.shardPartitions = shardPartitions;
    }

    public EsShardPartitions partitions() {
        return shardPartitions;
    }

    // this will be refactor soon
    public EsTablePartitions tablePartitions() throws Exception {
        return EsTablePartitions.fromShardPartitions(table, shardPartitions);
    }

    public boolean nodesDiscovery() {
        return nodesDiscovery;
    }
}
