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
import org.apache.doris.common.io.Text;
import org.apache.doris.external.elasticsearch.EsMetaStateTracker;
import org.apache.doris.external.elasticsearch.EsRestClient;
import org.apache.doris.external.elasticsearch.EsTablePartitions;
import org.apache.doris.external.elasticsearch.EsUtil;
import org.apache.doris.thrift.TEsTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
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
public class EsTable extends Table {

    public static final Set<String> DEFAULT_DOCVALUE_DISABLED_FIELDS = new HashSet<>(Arrays.asList("text"));

    public static final String HOSTS = "hosts";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String INDEX = "index";
    public static final String TYPE = "type";
    public static final String VERSION = "version";
    public static final String DOC_VALUES_MODE = "doc_values_mode";

    public static final String DOC_VALUE_SCAN = "enable_docvalue_scan";
    public static final String KEYWORD_SNIFF = "enable_keyword_sniff";
    public static final String MAX_DOCVALUE_FIELDS = "max_docvalue_fields";
    public static final String NODES_DISCOVERY = "nodes_discovery";
    public static final String HTTP_SSL_ENABLED = "http_ssl_enabled";
    public static final String QUERY_DSL = "query_dsl";
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
    private PartitionInfo partitionInfo;
    private EsTablePartitions esTablePartitions;

    // Whether to enable docvalues scan optimization for fetching fields more fast, default to true
    private boolean enableDocValueScan = true;
    // Whether to enable sniffing keyword for filtering more reasonable, default to true
    private boolean enableKeywordSniff = true;
    // if the number of fields which value extracted from `doc_value` exceeding this max limitation
    // would downgrade to extract value from `stored_fields`
    private int maxDocValueFields = DEFAULT_MAX_DOCVALUE_FIELDS;

    private boolean nodesDiscovery = true;

    private boolean httpSslEnabled = false;

    // tableContext is used for being convenient to persist some configuration parameters uniformly
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

    public Map<String, String> fieldsContext() {
        return esMetaStateTracker.searchContext().fetchFieldsContext();
    }

    public Map<String, String> docValueContext() {
        return esMetaStateTracker.searchContext().docValueFieldsContext();
    }

    private void validate(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            throw new DdlException(
                    "Please set properties of elasticsearch table, " + "they are: hosts, user, password, index");
        }
        if (StringUtils.isBlank(properties.get(HOSTS))) {
            throw new DdlException("Hosts of ES table is null. "
                    + "Please add properties('hosts'='xxx.xxx.xxx.xxx,xxx.xxx.xxx.xxx') when create table");
        }
        hosts = properties.get(HOSTS).trim();
        seeds = hosts.split(",");
        if (properties.containsKey(USER)) {
            userName = properties.get(USER).trim();
        }

        if (properties.containsKey(PASSWORD)) {
            passwd = properties.get(PASSWORD).trim();
        }

        if (StringUtils.isBlank(properties.get(INDEX))) {
            throw new DdlException(
                    "Index of ES table is null. " + "Please add properties('index'='xxxx') when create table");
        }
        indexName = properties.get(INDEX).trim();

        // enable doc value scan for Elasticsearch
        if (properties.containsKey(DOC_VALUE_SCAN)) {
            enableDocValueScan = EsUtil.getBoolean(properties, DOC_VALUE_SCAN);
        }

        if (properties.containsKey(KEYWORD_SNIFF)) {
            enableKeywordSniff = EsUtil.getBoolean(properties, KEYWORD_SNIFF);
        }

        if (properties.containsKey(NODES_DISCOVERY)) {
            nodesDiscovery = EsUtil.getBoolean(properties, NODES_DISCOVERY);
        }

        if (properties.containsKey(HTTP_SSL_ENABLED)) {
            httpSslEnabled = EsUtil.getBoolean(properties, HTTP_SSL_ENABLED);
            // check protocol
            for (String seed : seeds) {
                if (httpSslEnabled && seed.startsWith("http://")) {
                    throw new DdlException("if http_ssl_enabled is true, the https protocol must be used");
                }
                if (!httpSslEnabled && seed.startsWith("https://")) {
                    throw new DdlException("if http_ssl_enabled is false, the http protocol must be used");
                }
            }
        }

        if (StringUtils.isNotBlank(properties.get(TYPE))) {
            mappingType = properties.get(TYPE).trim();
        }

        if (properties.containsKey(MAX_DOCVALUE_FIELDS)) {
            try {
                maxDocValueFields = Integer.parseInt(properties.get(MAX_DOCVALUE_FIELDS).trim());
                if (maxDocValueFields < 0) {
                    maxDocValueFields = 0;
                }
            } catch (Exception e) {
                maxDocValueFields = DEFAULT_MAX_DOCVALUE_FIELDS;
            }
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
        tableContext.put(NODES_DISCOVERY, String.valueOf(nodesDiscovery));
        tableContext.put(HTTP_SSL_ENABLED, String.valueOf(httpSslEnabled));
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
        LOG.debug("get signature of es table {}: {}. signature string: {}", name, md5, sb.toString());
        return md5;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(tableContext.size());
        for (Map.Entry<String, String> entry : tableContext.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }
        Text.writeString(out, partitionInfo.getType().name());
        partitionInfo.write(out);
    }

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

        enableDocValueScan = Boolean.parseBoolean(tableContext.get("enableDocValueScan"));
        if (tableContext.containsKey("enableKeywordSniff")) {
            enableKeywordSniff = Boolean.parseBoolean(tableContext.get("enableKeywordSniff"));
        } else {
            enableKeywordSniff = true;
        }
        if (tableContext.containsKey("maxDocValueFields")) {
            try {
                maxDocValueFields = Integer.parseInt(tableContext.get("maxDocValueFields"));
            } catch (Exception e) {
                maxDocValueFields = DEFAULT_MAX_DOCVALUE_FIELDS;
            }
        }
        if (tableContext.containsKey(NODES_DISCOVERY)) {
            nodesDiscovery = Boolean.parseBoolean(tableContext.get(NODES_DISCOVERY));
        } else {
            nodesDiscovery = true;
        }
        if (tableContext.containsKey(HTTP_SSL_ENABLED)) {
            httpSslEnabled = Boolean.parseBoolean(tableContext.get(HTTP_SSL_ENABLED));
        } else {
            httpSslEnabled = false;
        }
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
        return EsUtil.genColumnsFromEs(client, indexName, mappingType);
    }
}
