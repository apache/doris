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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.datasource.es.EsMetaStateTracker;
import org.apache.doris.datasource.es.EsRestClient;
import org.apache.doris.datasource.es.EsTablePartitions;
import org.apache.doris.datasource.es.EsUtil;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.thrift.TEsTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Elasticsearch table.
 **/
@Getter
@Setter
public class EsTable extends Table implements GsonPostProcessable {
    // reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/doc-values.html
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/text.html
    public static final Set<String> DEFAULT_DOCVALUE_DISABLED_FIELDS =
            new HashSet<>(Arrays.asList("text", "annotated_text", "match_only_text"));

    private static final Logger LOG = LogManager.getLogger(EsTable.class);
    // Solr doc_values vs stored_fields performance-smackdown indicate:
    // It is possible to notice that retrieving an high number of fields leads
    // to a sensible worsening of performance if DocValues are used.
    // Instead,  the (almost) surprising thing is that, by returning less than 20 fields,
    // DocValues performs better than stored fields and the difference
    // gets little as the number of fields returned increases.
    // Asking for 9 DocValues fields and 1 stored field takes an average query time is 6.86
    // (more than returning 10 stored fields)
    // Here we have a slightly conservative value of 20, but at the same time
    // we also provide configurable parameters for expert-using
    // @see `MAX_DOCVALUE_FIELDS`
    @Getter
    private static final int DEFAULT_MAX_DOCVALUE_FIELDS = 20;
    private String hosts;
    private String[] seeds;
    private String userName = "";
    private String passwd = "";
    // index name can be specific index、wildcard matched or alias.
    private String indexName;

    // which type used for `indexName`
    private String mappingType = null;
    // only save the partition definition, save the partition key,
    // partition list is got from es cluster dynamically and is saved in esTableState
    @SerializedName("pi")
    private PartitionInfo partitionInfo;
    private EsTablePartitions esTablePartitions;

    // Whether to enable docvalues scan optimization for fetching fields more fast, default to true
    private boolean enableDocValueScan = Boolean.parseBoolean(EsResource.DOC_VALUE_SCAN_DEFAULT_VALUE);
    // Whether to enable sniffing keyword for filtering more reasonable, default to true
    private boolean enableKeywordSniff = Boolean.parseBoolean(EsResource.KEYWORD_SNIFF_DEFAULT_VALUE);
    // if the number of fields which value extracted from `doc_value` exceeding this max limitation
    // would downgrade to extract value from `stored_fields`
    private int maxDocValueFields = DEFAULT_MAX_DOCVALUE_FIELDS;

    // Whether to enable the discovery of es nodes, You can disable it if you are in network isolation
    private boolean nodesDiscovery = Boolean.parseBoolean(EsResource.NODES_DISCOVERY_DEFAULT_VALUE);

    // Whether to use ssl call es, be and fe access through trust
    private boolean httpSslEnabled = Boolean.parseBoolean(EsResource.HTTP_SSL_ENABLED_DEFAULT_VALUE);

    // Whether pushdown like expr, like will trans to wildcard query, consumes too many es cpu resources
    private boolean likePushDown = Boolean.parseBoolean(EsResource.LIKE_PUSH_DOWN_DEFAULT_VALUE);

    // Whether to include hidden index, default to false
    private boolean includeHiddenIndex = Boolean.parseBoolean(EsResource.INCLUDE_HIDDEN_INDEX_DEFAULT_VALUE);

    // tableContext is used for being convenient to persist some configuration parameters uniformly
    @SerializedName("tc")
    private Map<String, String> tableContext = new HashMap<>();

    // record the latest and recently exception when sync ES table metadata (mapping, shard location)
    private Throwable lastMetaDataSyncException = null;

    // connect es.
    private EsRestClient client = null;

    // Periodically pull es metadata
    private EsMetaStateTracker esMetaStateTracker;

    public EsTable() {
        super(TableType.ELASTICSEARCH);
    }

    /**
     * Create table for user.
     **/
    public EsTable(String name, Map<String, String> properties) throws DdlException {
        super(TableType.ELASTICSEARCH);
        this.name = name;
        validate(properties);
        this.client = new EsRestClient(seeds, userName, passwd, httpSslEnabled);
    }

    /**
     * Create table for test.
     **/
    public EsTable(long id, String name, List<Column> schema, Map<String, String> properties,
            PartitionInfo partitionInfo) throws DdlException {
        super(id, name, TableType.ELASTICSEARCH, schema);
        this.partitionInfo = partitionInfo;
        validate(properties);
        this.client = new EsRestClient(seeds, userName, passwd, httpSslEnabled);
    }

    public EsTable(long id, String name, List<Column> schema, TableType tableType) {
        super(id, name, tableType, schema);
    }

    public Map<String, String> fieldsContext() throws UserException {
        return esMetaStateTracker.searchContext().fetchFieldsContext();
    }

    public Map<String, String> docValueContext() throws UserException {
        return esMetaStateTracker.searchContext().docValueFieldsContext();
    }

    public List<String> needCompatDateFields() throws UserException {
        return esMetaStateTracker.searchContext().needCompatDateFields();
    }

    private void validate(Map<String, String> properties) throws DdlException {
        EsResource.valid(properties, false);
        if (properties.containsKey(EsResource.USER)) {
            userName = properties.get(EsResource.USER).trim();
        }

        if (properties.containsKey(EsResource.PASSWORD)) {
            passwd = properties.get(EsResource.PASSWORD).trim();
        }

        indexName = properties.get(EsResource.INDEX).trim();

        // enable doc value scan for Elasticsearch
        if (properties.containsKey(EsResource.DOC_VALUE_SCAN)) {
            enableDocValueScan = EsUtil.getBoolean(properties, EsResource.DOC_VALUE_SCAN);
        }

        if (properties.containsKey(EsResource.KEYWORD_SNIFF)) {
            enableKeywordSniff = EsUtil.getBoolean(properties, EsResource.KEYWORD_SNIFF);
        }

        if (properties.containsKey(EsResource.NODES_DISCOVERY)) {
            nodesDiscovery = EsUtil.getBoolean(properties, EsResource.NODES_DISCOVERY);
        }

        if (properties.containsKey(EsResource.HTTP_SSL_ENABLED)) {
            httpSslEnabled = EsUtil.getBoolean(properties, EsResource.HTTP_SSL_ENABLED);
        }

        if (properties.containsKey(EsResource.LIKE_PUSH_DOWN)) {
            likePushDown = EsUtil.getBoolean(properties, EsResource.LIKE_PUSH_DOWN);
        }

        if (StringUtils.isNotBlank(properties.get(EsResource.TYPE))) {
            mappingType = properties.get(EsResource.TYPE).trim();
        }

        if (properties.containsKey(EsResource.MAX_DOCVALUE_FIELDS)) {
            try {
                maxDocValueFields = Integer.parseInt(properties.get(EsResource.MAX_DOCVALUE_FIELDS).trim());
                if (maxDocValueFields < 0) {
                    maxDocValueFields = 0;
                }
            } catch (Exception e) {
                maxDocValueFields = DEFAULT_MAX_DOCVALUE_FIELDS;
            }
        }

        hosts = properties.get(EsResource.HOSTS).trim();
        seeds = hosts.split(",");
        // parse httpSslEnabled before use it here.
        EsResource.fillUrlsWithSchema(seeds, httpSslEnabled);

        if (properties.containsKey(EsResource.INCLUDE_HIDDEN_INDEX_DEFAULT_VALUE)) {
            includeHiddenIndex = EsUtil.getBoolean(properties, EsResource.INCLUDE_HIDDEN_INDEX_DEFAULT_VALUE);
        }

        tableContext.put("hosts", hosts);
        tableContext.put("userName", userName);
        tableContext.put("passwd", passwd);
        tableContext.put("indexName", indexName);
        if (mappingType != null) {
            tableContext.put("mappingType", mappingType);
        }
        tableContext.put("enableDocValueScan", String.valueOf(enableDocValueScan));
        tableContext.put("enableKeywordSniff", String.valueOf(enableKeywordSniff));
        tableContext.put("maxDocValueFields", String.valueOf(maxDocValueFields));
        tableContext.put(EsResource.NODES_DISCOVERY, String.valueOf(nodesDiscovery));
        tableContext.put(EsResource.HTTP_SSL_ENABLED, String.valueOf(httpSslEnabled));
        tableContext.put(EsResource.LIKE_PUSH_DOWN, String.valueOf(likePushDown));
        tableContext.put(EsResource.INCLUDE_HIDDEN_INDEX, String.valueOf(includeHiddenIndex));
    }

    @Override
    public TTableDescriptor toThrift() {
        TEsTable tEsTable = new TEsTable();
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.ES_TABLE, fullSchema.size(), 0,
                getName(), "");
        tTableDescriptor.setEsTable(tEsTable);
        return tTableDescriptor;
    }

    @Override
    public String getSignature(int signatureVersion) {
        StringBuilder sb = new StringBuilder(signatureVersion);
        sb.append(name);
        sb.append(type.name());
        if (tableContext.isEmpty()) {
            sb.append(hosts);
            sb.append(userName);
            sb.append(passwd);
            sb.append(indexName);
            if (mappingType != null) {
                sb.append(mappingType);
            }
        } else {
            for (Map.Entry<String, String> entry : tableContext.entrySet()) {
                sb.append(entry.getKey());
                sb.append(entry.getValue());
            }
        }
        String md5 = DigestUtils.md5Hex(sb.toString());
        if (LOG.isDebugEnabled()) {
            LOG.debug("get signature of es table {}: {}. signature string: {}", name, md5, sb.toString());
        }
        return md5;
    }

    @Deprecated
    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        int size = in.readInt();
        for (int i = 0; i < size; ++i) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            tableContext.put(key, value);
        }
        hosts = tableContext.get("hosts");
        seeds = hosts.split(",");
        userName = tableContext.get("userName");
        passwd = tableContext.get("passwd");
        indexName = tableContext.get("indexName");
        mappingType = tableContext.get("mappingType");

        enableDocValueScan = Boolean.parseBoolean(
                tableContext.getOrDefault("enableDocValueScan", EsResource.DOC_VALUE_SCAN_DEFAULT_VALUE));
        enableKeywordSniff = Boolean.parseBoolean(
                tableContext.getOrDefault("enableKeywordSniff", EsResource.KEYWORD_SNIFF_DEFAULT_VALUE));
        if (tableContext.containsKey("maxDocValueFields")) {
            try {
                maxDocValueFields = Integer.parseInt(tableContext.get("maxDocValueFields"));
            } catch (Exception e) {
                maxDocValueFields = DEFAULT_MAX_DOCVALUE_FIELDS;
            }
        }
        nodesDiscovery = Boolean.parseBoolean(
                tableContext.getOrDefault(EsResource.NODES_DISCOVERY, EsResource.NODES_DISCOVERY_DEFAULT_VALUE));
        httpSslEnabled = Boolean.parseBoolean(
                tableContext.getOrDefault(EsResource.HTTP_SSL_ENABLED, EsResource.HTTP_SSL_ENABLED_DEFAULT_VALUE));
        likePushDown = Boolean.parseBoolean(
                tableContext.getOrDefault(EsResource.LIKE_PUSH_DOWN, EsResource.LIKE_PUSH_DOWN_DEFAULT_VALUE));
        includeHiddenIndex = Boolean.parseBoolean(tableContext.getOrDefault(EsResource.INCLUDE_HIDDEN_INDEX,
                EsResource.INCLUDE_HIDDEN_INDEX_DEFAULT_VALUE));
        PartitionType partType = PartitionType.valueOf(Text.readString(in));
        if (partType == PartitionType.UNPARTITIONED) {
            partitionInfo = SinglePartitionInfo.read(in);
        } else if (partType == PartitionType.RANGE) {
            partitionInfo = RangePartitionInfo.read(in);
        } else {
            throw new IOException("invalid partition type: " + partType);
        }
        client = new EsRestClient(seeds, userName, passwd, httpSslEnabled);
    }

    public void gsonPostProcess() throws IOException {
        hosts = tableContext.get("hosts");
        seeds = hosts.split(",");
        userName = tableContext.get("userName");
        passwd = tableContext.get("passwd");
        indexName = tableContext.get("indexName");
        mappingType = tableContext.get("mappingType");

        enableDocValueScan = Boolean.parseBoolean(
                tableContext.getOrDefault("enableDocValueScan", EsResource.DOC_VALUE_SCAN_DEFAULT_VALUE));
        enableKeywordSniff = Boolean.parseBoolean(
                tableContext.getOrDefault("enableKeywordSniff", EsResource.KEYWORD_SNIFF_DEFAULT_VALUE));
        if (tableContext.containsKey("maxDocValueFields")) {
            try {
                maxDocValueFields = Integer.parseInt(tableContext.get("maxDocValueFields"));
            } catch (Exception e) {
                maxDocValueFields = DEFAULT_MAX_DOCVALUE_FIELDS;
            }
        }
        nodesDiscovery = Boolean.parseBoolean(
                tableContext.getOrDefault(EsResource.NODES_DISCOVERY, EsResource.NODES_DISCOVERY_DEFAULT_VALUE));
        httpSslEnabled = Boolean.parseBoolean(
                tableContext.getOrDefault(EsResource.HTTP_SSL_ENABLED, EsResource.HTTP_SSL_ENABLED_DEFAULT_VALUE));
        likePushDown = Boolean.parseBoolean(
                tableContext.getOrDefault(EsResource.LIKE_PUSH_DOWN, EsResource.LIKE_PUSH_DOWN_DEFAULT_VALUE));
        includeHiddenIndex = Boolean.parseBoolean(tableContext.getOrDefault(EsResource.INCLUDE_HIDDEN_INDEX,
                EsResource.INCLUDE_HIDDEN_INDEX_DEFAULT_VALUE));

        client = new EsRestClient(seeds, userName, passwd, httpSslEnabled);
    }

    /**
     * Sync es index meta from remote ES Cluster.
     */
    public void syncTableMetaData() {
        if (esMetaStateTracker == null) {
            esMetaStateTracker = new EsMetaStateTracker(client, this);
        }
        try {
            esMetaStateTracker.run();
            this.esTablePartitions = esMetaStateTracker.searchContext().tablePartitions();
        } catch (Throwable e) {
            LOG.warn(
                    "Exception happens when fetch index [{}] meta data from remote es cluster." + "table id: {}, err: ",
                    this.name, this.id, e);
            this.esTablePartitions = null;
            this.lastMetaDataSyncException = e;
        }
    }

    public List<Column> genColumnsFromEs() {
        return EsUtil.genColumnsFromEs(client, indexName, mappingType, false);
    }
}
