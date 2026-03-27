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
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.thrift.TEsTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Elasticsearch external table.
 */
public class EsExternalTable extends ExternalTable {

    private static final Logger LOG = LogManager.getLogger(EsExternalTable.class);

    private static final int DEFAULT_MAX_DOCVALUE_FIELDS = 20;

    // Runtime ES state — populated by initEsState()
    @Getter
    private String indexName;
    @Getter
    private String mappingType = null;
    @Getter
    private String userName = "";
    @Getter
    private String passwd = "";
    @Getter
    private String hosts;
    @Getter
    private String[] seeds;
    @Getter
    private boolean enableDocValueScan;
    @Getter
    private boolean enableKeywordSniff;
    @Getter
    private boolean nodesDiscovery;
    @Getter
    private boolean httpSslEnabled;
    @Getter
    private boolean likePushDown;
    @Getter
    private boolean includeHiddenIndex;
    @Getter
    private int maxDocValueFields = DEFAULT_MAX_DOCVALUE_FIELDS;
    @Getter
    private Map<String, String> column2typeMap = new HashMap<>();

    private EsRestClient client;
    private EsMetaStateTracker esMetaStateTracker;
    @Getter
    private EsTablePartitions esTablePartitions;
    @Getter
    private Throwable lastMetaDataSyncException = null;

    /**
     * Create elasticsearch external table.
     *
     * @param id Table id.
     * @param name Table name.
     * @param remoteName Remote table name.
     * @param catalog EsExternalDataSource.
     * @param db Database.
     */
    public EsExternalTable(long id, String name, String remoteName, EsExternalCatalog catalog, EsExternalDatabase db) {
        super(id, name, remoteName, catalog, db, TableType.ES_EXTERNAL_TABLE);
    }

    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            initEsState();
            objectCreated = true;
        }
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        TEsTable tEsTable = new TEsTable();
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.ES_TABLE, schema.size(), 0,
                getName(), "");
        tTableDescriptor.setEsTable(tEsTable);
        return tTableDescriptor;
    }

    @Override
    public Optional<SchemaCacheValue> initSchema() {
        EsRestClient restClient = ((EsExternalCatalog) catalog).getEsRestClient();
        Map<String, String> column2typeMap = new HashMap<>();
        List<Column> columns = EsUtil.genColumnsFromEs(restClient, name, null,
                ((EsExternalCatalog) catalog).enableMappingEsId(), column2typeMap);
        return Optional.of(new EsSchemaCacheValue(columns, column2typeMap));
    }

    public Map<String, String> getColumn2type() {
        Optional<SchemaCacheValue> schemaCacheValue = getSchemaCacheValue();
        return schemaCacheValue.map(value -> ((EsSchemaCacheValue) value).getColumn2typeMap()).orElse(new HashMap<>());
    }

    /**
     * Get fetch fields context (col -> col.keyword mapping for keyword sniff).
     */
    public Map<String, String> fieldsContext() throws UserException {
        makeSureInitialized();
        return esMetaStateTracker.searchContext().fetchFieldsContext();
    }

    /**
     * Get doc value fields context for doc_value scan optimization.
     */
    public Map<String, String> docValueContext() throws UserException {
        makeSureInitialized();
        return esMetaStateTracker.searchContext().docValueFieldsContext();
    }

    /**
     * Get fields that need date compatibility handling.
     */
    public List<String> needCompatDateFields() throws UserException {
        makeSureInitialized();
        return esMetaStateTracker.searchContext().needCompatDateFields();
    }

    /**
     * Initialize all ES runtime state from catalog configuration.
     * Replaces the old toEsTable() bridge pattern.
     */
    private void initEsState() {
        EsExternalCatalog esCatalog = (EsExternalCatalog) catalog;

        this.indexName = name;
        this.userName = esCatalog.getUsername();
        this.passwd = esCatalog.getPassword();
        this.enableDocValueScan = esCatalog.enableDocValueScan();
        this.enableKeywordSniff = esCatalog.enableKeywordSniff();
        this.nodesDiscovery = esCatalog.enableNodesDiscovery();
        this.httpSslEnabled = esCatalog.enableSsl();
        this.likePushDown = esCatalog.enableLikePushDown();
        this.includeHiddenIndex = esCatalog.enableIncludeHiddenIndex();
        this.seeds = esCatalog.getNodes();
        this.hosts = String.join(",", esCatalog.getNodes());
        this.client = esCatalog.getEsRestClient();
        this.column2typeMap = getColumn2type();

        // Initialize metadata tracker and sync
        this.esMetaStateTracker = new EsMetaStateTracker(client, this);
        syncTableMetaData();
    }

    /**
     * Sync es index meta from remote ES Cluster.
     */
    public void syncTableMetaData() {
        try {
            esMetaStateTracker.run();
            this.esTablePartitions = esMetaStateTracker.searchContext().tablePartitions();
        } catch (Throwable e) {
            LOG.warn("Exception happens when fetch index [{}] meta data from remote es cluster. err: ",
                    this.name, e);
            this.esTablePartitions = null;
            this.lastMetaDataSyncException = e;
        }
    }
}
