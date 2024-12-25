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

package org.apache.doris.datasource.paimon;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.ExternalSchemaCache;
import org.apache.doris.datasource.ExternalSchemaCache.SchemaCacheKey;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ExternalAnalysisTask;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.system.SchemasTable;
import org.apache.paimon.types.DataField;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class PaimonExternalTable extends ExternalTable implements MvccTable {

    private static final Logger LOG = LogManager.getLogger(PaimonExternalTable.class);

    private final Table paimonTable;

    public PaimonExternalTable(long id, String name, String remoteName, PaimonExternalCatalog catalog,
            PaimonExternalDatabase db) {
        super(id, name, remoteName, catalog, db, TableType.PAIMON_EXTERNAL_TABLE);
        this.paimonTable = catalog.getPaimonTable(dbName, name);
    }

    public String getPaimonCatalogType() {
        return ((PaimonExternalCatalog) catalog).getCatalogType();
    }

    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            objectCreated = true;
        }
    }

    public Table getPaimonTable(Optional<MvccSnapshot> snapshot) {
        return paimonTable.copy(
                Collections.singletonMap(CoreOptions.SCAN_VERSION.key(),
                        String.valueOf(getOrFetchSnapshotCacheValue(snapshot).getSnapshot().getSnapshotId())));
    }

    public PaimonSchemaCacheValue getPaimonSchemaCacheValue(long schemaId) {
        ExternalSchemaCache cache = Env.getCurrentEnv().getExtMetaCacheMgr().getSchemaCache(catalog);
        Optional<SchemaCacheValue> schemaCacheValue = cache.getSchemaValue(
                new PaimonSchemaCacheKey(dbName, name, schemaId));
        if (!schemaCacheValue.isPresent()) {
            throw new CacheException("failed to getSchema for: %s.%s.%s.%s",
                    null, catalog.getName(), dbName, name, schemaId);
        }
        return (PaimonSchemaCacheValue) schemaCacheValue.get();
    }

    private PaimonSnapshotCacheValue getPaimonSnapshotCacheValue() {
        makeSureInitialized();
        return Env.getCurrentEnv().getExtMetaCacheMgr().getPaimonMetadataCache()
                .getPaimonSnapshot(catalog, dbName, name);
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        if (PaimonExternalCatalog.PAIMON_HMS.equals(getPaimonCatalogType())
                || PaimonExternalCatalog.PAIMON_FILESYSTEM.equals(getPaimonCatalogType())
                || PaimonExternalCatalog.PAIMON_DLF.equals(getPaimonCatalogType())) {
            THiveTable tHiveTable = new THiveTable(dbName, name, new HashMap<>());
            TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.HIVE_TABLE, schema.size(), 0,
                    getName(), dbName);
            tTableDescriptor.setHiveTable(tHiveTable);
            return tTableDescriptor;
        } else {
            throw new IllegalArgumentException("Currently only supports hms/filesystem catalog,not support :"
                    + getPaimonCatalogType());
        }
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        makeSureInitialized();
        return new ExternalAnalysisTask(info);
    }

    @Override
    public long fetchRowCount() {
        makeSureInitialized();
        try {
            long rowCount = 0;
            List<Split> splits = paimonTable.newReadBuilder().newScan().plan().splits();
            for (Split split : splits) {
                rowCount += split.rowCount();
            }
            return rowCount > 0 ? rowCount : UNKNOWN_ROW_COUNT;
        } catch (Exception e) {
            LOG.warn("Fail to collect row count for db {} table {}", dbName, name, e);
        }
        return UNKNOWN_ROW_COUNT;
    }

    @Override
    public List<Column> getPartitionColumns(Optional<MvccSnapshot> snapshot) {
        return getPaimonSchemaCacheValue(snapshot).getPartitionColumns();
    }

    @Override
    public MvccSnapshot loadSnapshot() {
        return new PaimonMvccSnapshot(getPaimonSnapshotCacheValue());
    }

    @Override
    public Map<String, PartitionItem> getNameToPartitionItems(Optional<MvccSnapshot> snapshot) {
        return getOrFetchSnapshotCacheValue(snapshot).getPartitionInfo().getNameToPartitionItem();
    }

    @Override
    public boolean supportInternalPartitionPruned() {
        return true;
    }

    @Override
    public List<Column> getFullSchema() {
        return getPaimonSchemaCacheValue(MvccUtil.getSnapshotFromContext(this)).getSchema();
    }

    @Override
    public Optional<SchemaCacheValue> initSchema(SchemaCacheKey key) {
        makeSureInitialized();
        PaimonSchemaCacheKey paimonSchemaCacheKey = (PaimonSchemaCacheKey) key;
        try {
            PaimonSchema schema = loadPaimonSchemaBySchemaId(paimonSchemaCacheKey);
            List<DataField> columns = schema.getFields();
            List<Column> dorisColumns = Lists.newArrayListWithCapacity(columns.size());
            Set<String> partitionColumnNames = Sets.newHashSet(schema.getPartitionKeys());
            List<Column> partitionColumns = Lists.newArrayList();
            for (DataField field : columns) {
                Column column = new Column(field.name().toLowerCase(),
                        PaimonUtil.paimonTypeToDorisType(field.type()), true, null, true, field.description(), true,
                        field.id());
                dorisColumns.add(column);
                if (partitionColumnNames.contains(field.name())) {
                    partitionColumns.add(column);
                }
            }
            return Optional.of(new PaimonSchemaCacheValue(dorisColumns, partitionColumns));
        } catch (Exception e) {
            throw new CacheException("failed to initSchema for: %s.%s.%s.%s",
                    null, getCatalog().getName(), key.getDbName(), key.getTblName(),
                    paimonSchemaCacheKey.getSchemaId());
        }
    }

    private PaimonSchema loadPaimonSchemaBySchemaId(PaimonSchemaCacheKey key) throws IOException {
        Table table = ((PaimonExternalCatalog) getCatalog()).getPaimonTable(key.getDbName(),
                name + Catalog.SYSTEM_TABLE_SPLITTER + SchemasTable.SCHEMAS);
        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        Predicate predicate = builder.equal(0, key.getSchemaId());
        // Adding predicates will also return excess data
        List<InternalRow> rows = PaimonUtil.read(table, new int[][] {{0}, {1}, {2}}, predicate);
        for (InternalRow row : rows) {
            PaimonSchema schema = PaimonUtil.rowToSchema(row);
            if (schema.getSchemaId() == key.getSchemaId()) {
                return schema;
            }
        }
        throw new CacheException("failed to initSchema for: %s.%s.%s.%s",
                null, getCatalog().getName(), key.getDbName(), key.getTblName(), key.getSchemaId());
    }

    private PaimonSchemaCacheValue getPaimonSchemaCacheValue(Optional<MvccSnapshot> snapshot) {
        PaimonSnapshotCacheValue snapshotCacheValue = getOrFetchSnapshotCacheValue(snapshot);
        return getPaimonSchemaCacheValue(snapshotCacheValue.getSnapshot().getSchemaId());
    }

    private PaimonSnapshotCacheValue getOrFetchSnapshotCacheValue(Optional<MvccSnapshot> snapshot) {
        if (snapshot.isPresent()) {
            return ((PaimonMvccSnapshot) snapshot.get()).getSnapshotCacheValue();
        } else {
            return getPaimonSnapshotCacheValue();
        }
    }

}
