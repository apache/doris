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
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.external.elasticsearch.EsFieldInfos;
import org.apache.doris.external.elasticsearch.EsMajorVersion;
import org.apache.doris.external.elasticsearch.EsNodeInfo;
import org.apache.doris.external.elasticsearch.EsRestClient;
import org.apache.doris.external.elasticsearch.EsShardPartitions;
import org.apache.doris.external.elasticsearch.EsTablePartitions;
import org.apache.doris.thrift.TEsTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Strings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.Adler32;

public class EsTable extends Table {
    private static final Logger LOG = LogManager.getLogger(EsTable.class);

    public static final Set<String> DEFAULT_DOCVALUE_DISABLED_FIELDS = new HashSet<>(Arrays.asList("text"));

    public static final String HOSTS = "hosts";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String INDEX = "index";
    public static final String TYPE = "type";
    public static final String TRANSPORT = "transport";
    public static final String VERSION = "version";

    public static final String TRANSPORT_HTTP = "http";
    public static final String TRANSPORT_THRIFT = "thrift";
    public static final String DOC_VALUE_SCAN = "enable_docvalue_scan";
    public static final String KEYWORD_SNIFF = "enable_keyword_sniff";

    private String hosts;
    private String[] seeds;
    private String userName = "";
    private String passwd = "";
    private String indexName;
    private String mappingType = "_doc";
    private String transport = "http";
    // only save the partition definition, save the partition key,
    // partition list is got from es cluster dynamically and is saved in esTableState
    private PartitionInfo partitionInfo;
    private EsTablePartitions esTablePartitions;
    private boolean enableDocValueScan = false;
    private boolean enableKeywordSniff = true;

    public EsMajorVersion majorVersion = null;

    private Map<String, String> tableContext = new HashMap<>();

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
    private Map<String, String> docValueContext = new HashMap<>();

    private Map<String, String> fieldsContext = new HashMap<>();

    // record the latest and recently exception when sync ES table metadata (mapping, shard location)
    private Throwable lastMetaDataSyncException = null;

    public EsTable() {
        super(TableType.ELASTICSEARCH);
    }

    public EsTable(long id, String name, List<Column> schema,
                   Map<String, String> properties, PartitionInfo partitionInfo) throws DdlException {
        super(id, name, TableType.ELASTICSEARCH, schema);
        this.partitionInfo = partitionInfo;
        validate(properties);
    }

    public void addFieldInfos(EsFieldInfos esFieldInfos) {
        if (enableKeywordSniff && esFieldInfos.getFieldsContext() != null) {
            fieldsContext = esFieldInfos.getFieldsContext();
        }
        if (enableDocValueScan && esFieldInfos.getDocValueContext() != null) {
            docValueContext = esFieldInfos.getDocValueContext();
        }
    }

    public Map<String, String> fieldsContext() {
        return fieldsContext;
    }

    public Map<String, String> docValueContext() {
        return docValueContext;
    }

    public boolean isDocValueScanEnable() {
        return enableDocValueScan;
    }

    public boolean isKeywordSniffEnable() {
        return enableKeywordSniff;
    }


    private void validate(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            throw new DdlException("Please set properties of elasticsearch table, "
                    + "they are: hosts, user, password, index");
        }

        if (Strings.isNullOrEmpty(properties.get(HOSTS))
                || Strings.isNullOrEmpty(properties.get(HOSTS).trim())) {
            throw new DdlException("Hosts of ES table is null. "
                    + "Please add properties('hosts'='xxx.xxx.xxx.xxx,xxx.xxx.xxx.xxx') when create table");
        }
        hosts = properties.get(HOSTS).trim();
        seeds = hosts.split(",");

        if (!Strings.isNullOrEmpty(properties.get(USER))
                && !Strings.isNullOrEmpty(properties.get(USER).trim())) {
            userName = properties.get(USER).trim();
        }

        if (!Strings.isNullOrEmpty(properties.get(PASSWORD))
                && !Strings.isNullOrEmpty(properties.get(PASSWORD).trim())) {
            passwd = properties.get(PASSWORD).trim();
        }

        if (Strings.isNullOrEmpty(properties.get(INDEX))
                || Strings.isNullOrEmpty(properties.get(INDEX).trim())) {
            throw new DdlException("Index of ES table is null. "
                    + "Please add properties('index'='xxxx') when create table");
        }
        indexName = properties.get(INDEX).trim();

        // Explicit setting for cluster version to avoid detecting version failure
        if (properties.containsKey(VERSION)) {
            try {
                majorVersion = EsMajorVersion.parse(properties.get(VERSION).trim());
            } catch (Exception e) {
                throw new DdlException("fail to parse ES major version, version= "
                        + properties.get(VERSION).trim() + ", shoud be like '6.5.3' ");
            }
        }

        // enable doc value scan for Elasticsearch
        if (properties.containsKey(DOC_VALUE_SCAN)) {
            try {
                enableDocValueScan = Boolean.parseBoolean(properties.get(DOC_VALUE_SCAN).trim());
            } catch (Exception e) {
                throw new DdlException("fail to parse enable_docvalue_scan, enable_docvalue_scan= "
                        + properties.get(VERSION).trim() + " ,`enable_docvalue_scan`"
                        + " shoud be like 'true' or 'false'， value should be double quotation marks");
            }
        } else {
            enableDocValueScan = false;
        }

        if (properties.containsKey(KEYWORD_SNIFF)) {
            try {
                enableKeywordSniff = Boolean.parseBoolean(properties.get(KEYWORD_SNIFF).trim());
            } catch (Exception e) {
                throw new DdlException("fail to parse enable_keyword_sniff, enable_keyword_sniff= "
                        + properties.get(VERSION).trim() + " ,`enable_keyword_sniff`"
                        + " shoud be like 'true' or 'false'， value should be double quotation marks");
            }
        } else {
            enableKeywordSniff = true;
        }

        if (!Strings.isNullOrEmpty(properties.get(TYPE))
                && !Strings.isNullOrEmpty(properties.get(TYPE).trim())) {
            mappingType = properties.get(TYPE).trim();
        }
        if (!Strings.isNullOrEmpty(properties.get(TRANSPORT))
                && !Strings.isNullOrEmpty(properties.get(TRANSPORT).trim())) {
            transport = properties.get(TRANSPORT).trim();
            if (!(TRANSPORT_HTTP.equals(transport) || TRANSPORT_THRIFT.equals(transport))) {
                throw new DdlException("transport of ES table must be http(recommend) or thrift(reserved inner usage),"
                        + " but value is " + transport);
            }
        }
        tableContext.put("hosts", hosts);
        tableContext.put("userName", userName);
        tableContext.put("passwd", passwd);
        tableContext.put("indexName", indexName);
        tableContext.put("mappingType", mappingType);
        tableContext.put("transport", transport);
        if (majorVersion != null) {
            tableContext.put("majorVersion", majorVersion.toString());
        }
        tableContext.put("enableDocValueScan", String.valueOf(enableDocValueScan));
        tableContext.put("enableKeywordSniff", String.valueOf(enableKeywordSniff));
    }

    public TTableDescriptor toThrift() {
        TEsTable tEsTable = new TEsTable();
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.ES_TABLE,
                fullSchema.size(), 0, getName(), "");
        tTableDescriptor.setEsTable(tEsTable);
        return tTableDescriptor;
    }

    @Override
    public int getSignature(int signatureVersion) {
        Adler32 adler32 = new Adler32();
        adler32.update(signatureVersion);
        String charsetName = "UTF-8";

        try {
            // name
            adler32.update(name.getBytes(charsetName));
            // type
            adler32.update(type.name().getBytes(charsetName));
            if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_68) {
                for (Map.Entry<String, String> entry : tableContext.entrySet()) {
                    adler32.update(entry.getValue().getBytes(charsetName));
                }
            } else {
                // host
                adler32.update(hosts.getBytes(charsetName));
                // username
                adler32.update(userName.getBytes(charsetName));
                // passwd
                adler32.update(passwd.getBytes(charsetName));
                // index name
                adler32.update(indexName.getBytes(charsetName));
                // mappingType
                adler32.update(mappingType.getBytes(charsetName));
                // transport
                adler32.update(transport.getBytes(charsetName));
            }
        } catch (UnsupportedEncodingException e) {
            LOG.error("encoding error", e);
            return -1;
        }

        return Math.abs((int) adler32.getValue());
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

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_68) {
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
            transport = tableContext.get("transport");
            if (tableContext.containsKey("majorVersion")) {
                try {
                    majorVersion = EsMajorVersion.parse(tableContext.get("majorVersion"));
                } catch (Exception e) {
                    majorVersion = EsMajorVersion.V_5_X;
                }
            }

            enableDocValueScan = Boolean.parseBoolean(tableContext.get("enableDocValueScan"));
            if (tableContext.containsKey("enableKeywordSniff")) {
                enableKeywordSniff = Boolean.parseBoolean(tableContext.get("enableKeywordSniff"));
            } else {
                enableKeywordSniff = true;
            }

            PartitionType partType = PartitionType.valueOf(Text.readString(in));
            if (partType == PartitionType.UNPARTITIONED) {
                partitionInfo = SinglePartitionInfo.read(in);
            } else if (partType == PartitionType.RANGE) {
                partitionInfo = RangePartitionInfo.read(in);
            } else {
                throw new IOException("invalid partition type: " + partType);
            }
        } else {
            hosts = Text.readString(in);
            seeds = hosts.split(",");
            userName = Text.readString(in);
            passwd = Text.readString(in);
            indexName = Text.readString(in);
            mappingType = Text.readString(in);
            PartitionType partType = PartitionType.valueOf(Text.readString(in));
            if (partType == PartitionType.UNPARTITIONED) {
                partitionInfo = SinglePartitionInfo.read(in);
            } else if (partType == PartitionType.RANGE) {
                partitionInfo = RangePartitionInfo.read(in);
            } else {
                throw new IOException("invalid partition type: " + partType);
            }
            transport = Text.readString(in);
            // for upgrading write
            tableContext.put("hosts", hosts);
            tableContext.put("userName", userName);
            tableContext.put("passwd", passwd);
            tableContext.put("indexName", indexName);
            tableContext.put("mappingType", mappingType);
            tableContext.put("transport", transport);
            tableContext.put("enableDocValueScan", "false");
            tableContext.put(KEYWORD_SNIFF, "true");
        }
    }

    public String getHosts() {
        return hosts;
    }

    public String[] getSeeds() {
        return seeds;
    }

    public String getUserName() {
        return userName;
    }

    public String getPasswd() {
        return passwd;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getMappingType() {
        return mappingType;
    }

    public String getTransport() {
        return transport;
    }

    public PartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    public EsTablePartitions getEsTablePartitions() {
        return esTablePartitions;
    }

    public void setEsTablePartitions(EsTablePartitions esTablePartitions) {
        this.esTablePartitions = esTablePartitions;
    }

    public Throwable getLastMetaDataSyncException() {
        return lastMetaDataSyncException;
    }

    public void setLastMetaDataSyncException(Throwable lastMetaDataSyncException) {
        this.lastMetaDataSyncException = lastMetaDataSyncException;
    }

    /**
     * sync es index meta from remote
     * @param client esRestClient
     */
    public void syncESIndexMeta(EsRestClient client) {
        try {
            EsFieldInfos fieldInfos = client.getFieldInfos(this.indexName, this.mappingType, this.fullSchema);
            EsShardPartitions esShardPartitions = client.getShardPartitions(this.indexName);
            Map<String, EsNodeInfo> nodesInfo = client.getHttpNodes();
            if (this.enableKeywordSniff || this.enableDocValueScan) {
                addFieldInfos(fieldInfos);
            }

            this.esTablePartitions = EsTablePartitions.fromShardPartitions(this, esShardPartitions);

            if (EsTable.TRANSPORT_HTTP.equals(getTransport())) {
                this.esTablePartitions.addHttpAddress(nodesInfo);
            }
        } catch (Throwable e) {
            LOG.warn("Exception happens when fetch index [{}] meta data from remote es cluster", this.name, e);
            this.esTablePartitions = null;
            this.lastMetaDataSyncException = e;
        }
    }
}
