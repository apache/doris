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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableAttributes;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.constraint.Constraint;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.Util;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.thrift.TTableDescriptor;

import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * External table represent tables that are not self-managed by Doris.
 * Such as tables from hive, iceberg, es, etc.
 */
@Getter
public class ExternalTable implements TableIf, Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(ExternalTable.class);

    @SerializedName(value = "id")
    protected long id;
    @SerializedName(value = "name")
    protected String name;
    @SerializedName(value = "type")
    protected TableType type = null;
    @SerializedName(value = "timestamp")
    protected long timestamp;
    @SerializedName(value = "dbName")
    protected String dbName;
    @SerializedName(value = "ta")
    private final TableAttributes tableAttributes = new TableAttributes();

    // this field will be refreshed after reloading schema
    protected volatile long schemaUpdateTime;

    protected long dbId;
    protected boolean objectCreated;
    protected ExternalCatalog catalog;

    /**
     * No args constructor for persist.
     */
    public ExternalTable() {
        this.objectCreated = false;
    }

    /**
     * Create external table.
     *
     * @param id Table id.
     * @param name Table name.
     * @param catalog ExternalCatalog this table belongs to.
     * @param dbName Name of the db the this table belongs to.
     * @param type Table type.
     */
    public ExternalTable(long id, String name, ExternalCatalog catalog, String dbName, TableType type) {
        this.id = id;
        this.name = name;
        this.catalog = catalog;
        this.dbName = dbName;
        this.type = type;
        this.objectCreated = false;
    }

    public void setCatalog(ExternalCatalog catalog) {
        this.catalog = catalog;
    }

    public boolean isView() {
        return false;
    }

    protected void makeSureInitialized() {
        try {
            // getDbOrAnalysisException will call makeSureInitialized in ExternalCatalog.
            ExternalDatabase db = catalog.getDbOrAnalysisException(dbName);
            dbId = db.getId();
            db.makeSureInitialized();
        } catch (AnalysisException e) {
            Util.logAndThrowRuntimeException(LOG, String.format("Exception to get db %s", dbName), e);
        }
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public TableType getType() {
        return type;
    }

    @Override
    public List<Column> getFullSchema() {
        ExternalSchemaCache cache = Env.getCurrentEnv().getExtMetaCacheMgr().getSchemaCache(catalog);
        Optional<SchemaCacheValue> schemaCacheValue = cache.getSchemaValue(dbName, name);
        return schemaCacheValue.map(SchemaCacheValue::getSchema).orElse(null);
    }

    @Override
    public List<Column> getBaseSchema() {
        return getFullSchema();
    }

    @Override
    public List<Column> getBaseSchema(boolean full) {
        return getFullSchema();
    }

    @Override
    public void setNewFullSchema(List<Column> newSchema) {
    }

    @Override
    public Column getColumn(String name) {
        List<Column> schema = getFullSchema();
        for (Column column : schema) {
            if (name.equalsIgnoreCase(column.getName())) {
                return column;
            }
        }
        return null;
    }

    @Override
    public Map<String, Constraint> getConstraintsMapUnsafe() {
        return tableAttributes.getConstraintsMap();
    }

    @Override
    public String getEngine() {
        return getType().toEngineName();
    }

    @Override
    public String getMysqlType() {
        return getType().toMysqlType();
    }

    @Override
    public long getRowCount() {
        // Return -1 if makeSureInitialized throw exception.
        // For example, init hive table may throw NotSupportedException.
        try {
            makeSureInitialized();
        } catch (Exception e) {
            LOG.warn("Failed to initialize table {}.{}.{}", catalog.getName(), dbName, name, e);
            return -1;
        }
        // All external table should get external row count from cache.
        return Env.getCurrentEnv().getExtMetaCacheMgr().getRowCountCache().getCachedRowCount(catalog.getId(), dbId, id);
    }

    @Override
    public long getCachedRowCount() {
        // Return -1 if makeSureInitialized throw exception.
        // For example, init hive table may throw NotSupportedException.
        try {
            makeSureInitialized();
        } catch (Exception e) {
            LOG.warn("Failed to initialize table {}.{}.{}", catalog.getName(), dbName, name, e);
            return -1;
        }
        return Env.getCurrentEnv().getExtMetaCacheMgr().getRowCountCache().getCachedRowCount(catalog.getId(), dbId, id);
    }

    @Override
    /**
     * Default return -1. Subclass need to implement this interface.
     * This is called by ExternalRowCountCache to load row count cache.
     */
    public long fetchRowCount() {
        return -1;
    }

    @Override
    public long getAvgRowLength() {
        return 0;
    }

    @Override
    public long getDataLength() {
        return 0;
    }

    @Override
    public long getCreateTime() {
        return 0;
    }

    // return schema update time as default
    // override this method if there is some other kinds of update time
    // use getSchemaUpdateTime if just need the schema update time
    @Override
    public long getUpdateTime() {
        return this.schemaUpdateTime;
    }

    public void setUpdateTime(long schemaUpdateTime) {
        this.schemaUpdateTime = schemaUpdateTime;
    }

    @Override
    public long getLastCheckTime() {
        return 0;
    }

    @Override
    public String getComment() {
        return "";
    }

    @Override
    public String getComment(boolean escapeQuota) {
        return "";
    }

    public TTableDescriptor toThrift() {
        return null;
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        throw new NotImplementedException("createAnalysisTask not implemented");
    }

    @Override
    public DatabaseIf getDatabase() {
        return catalog.getDbNullable(dbName);
    }

    @Override
    public List<Column> getColumns() {
        return getFullSchema();
    }

    @Override
    public boolean autoAnalyzeEnabled() {
        makeSureInitialized();
        String policy = catalog.getTableAutoAnalyzePolicy().get(Pair.of(dbName, name));
        if (policy == null) {
            return catalog.enableAutoAnalyze();
        }
        return policy.equalsIgnoreCase(PropertyAnalyzer.ENABLE_AUTO_ANALYZE_POLICY);
    }

    @Override
    public Optional<ColumnStatistic> getColumnStatistic(String colName) {
        return Optional.empty();
    }

    /**
     * Should only be called in ExternalCatalog's getSchema(),
     * which is called from schema cache.
     * If you want to get schema of this table, use getFullSchema()
     *
     * @return
     */
    public Optional<SchemaCacheValue> initSchemaAndUpdateTime() {
        schemaUpdateTime = System.currentTimeMillis();
        return initSchema();
    }

    public Optional<SchemaCacheValue> initSchema() {
        throw new NotImplementedException("implement in sub class");
    }

    public void unsetObjectCreated() {
        this.objectCreated = false;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static ExternalTable read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ExternalTable.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        objectCreated = false;
    }

    @Override
    public Set<Pair<String, String>> getColumnIndexPairs(Set<String> columns) {
        Set<Pair<String, String>> ret = Sets.newHashSet();
        for (String column : columns) {
            Column col = getColumn(column);
            if (col == null || StatisticsUtil.isUnsupportedType(col.getType())) {
                continue;
            }
            // External table put table name as index name.
            ret.add(Pair.of(String.valueOf(name), column));
        }
        return ret;
    }

    @Override
    public List<Long> getChunkSizes() {
        throw new NotImplementedException("getChunkSized not implemented");
    }

    protected Optional<SchemaCacheValue> getSchemaCacheValue() {
        ExternalSchemaCache cache = Env.getCurrentEnv().getExtMetaCacheMgr().getSchemaCache(catalog);
        return cache.getSchemaValue(dbName, name);
    }
}
