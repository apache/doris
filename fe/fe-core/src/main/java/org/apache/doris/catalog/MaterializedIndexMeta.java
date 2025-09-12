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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.MVColumnItem;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.mtmv.SyncMvMetrics;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateMaterializedViewCommand;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContextUtil;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.thrift.TStorageType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MaterializedIndexMeta implements GsonPostProcessable {
    @SerializedName(value = "id", alternate = {"indexId"})
    private long indexId;
    @SerializedName(value = "sc", alternate = {"schema"})
    private List<Column> schema = Lists.newArrayList();
    @SerializedName(value = "sv", alternate = {"schemaVersion"})
    private int schemaVersion;
    @SerializedName(value = "sh", alternate = {"schemaHash"})
    private int schemaHash;
    @SerializedName(value = "skcc", alternate = {"shortKeyColumnCount"})
    private short shortKeyColumnCount;
    @SerializedName(value = "st", alternate = {"storageType"})
    private TStorageType storageType;
    @SerializedName(value = "kt", alternate = {"keysType"})
    private KeysType keysType;
    @SerializedName(value = "dst", alternate = {"defineStmt"})
    private OriginStatement defineStmt;
    //for light schema change
    @SerializedName(value = "mcui", alternate = {"maxColUniqueId"})
    private int maxColUniqueId = Column.COLUMN_UNIQUE_ID_INIT_VALUE;
    @SerializedName(value = "idx", alternate = {"indexes"})
    private List<Index> indexes;

    private Expr whereClause;
    private Map<String, Column> nameToColumn;
    private Map<String, Column> definedNameToColumn;
    private SyncMvMetrics syncMvMetrics;

    @SerializedName(value = "dbName")
    private String dbName;

    private static final Logger LOG = LogManager.getLogger(MaterializedIndexMeta.class);

    public MaterializedIndexMeta() {
        this.syncMvMetrics = new SyncMvMetrics();
    }

    public MaterializedIndexMeta(long indexId, List<Column> schema, int schemaVersion, int schemaHash,
            short shortKeyColumnCount, TStorageType storageType, KeysType keysType, OriginStatement defineStmt) {
        this(indexId, schema, schemaVersion, schemaHash, shortKeyColumnCount, storageType, keysType,
                defineStmt, null, null); // indexes is null by default
    }

    public MaterializedIndexMeta(long indexId, List<Column> schema, int schemaVersion, int schemaHash,
            short shortKeyColumnCount, TStorageType storageType, KeysType keysType, OriginStatement defineStmt,
            List<Index> indexes, String dbName) {
        this.indexId = indexId;
        Preconditions.checkState(schema != null);
        Preconditions.checkState(schema.size() != 0);
        this.schema = schema;
        this.schemaVersion = schemaVersion;
        this.schemaHash = schemaHash;
        this.shortKeyColumnCount = shortKeyColumnCount;
        Preconditions.checkState(storageType != null);
        this.storageType = storageType;
        Preconditions.checkState(keysType != null);
        this.keysType = keysType;
        this.defineStmt = defineStmt;
        this.indexes = indexes != null ? indexes : Lists.newArrayList();
        initColumnNameMap();
        this.dbName = dbName;
        this.syncMvMetrics = new SyncMvMetrics();
    }

    public void setWhereClause(Expr whereClause) {
        this.whereClause = whereClause;
        if (this.whereClause != null) {
            this.whereClause.disableTableName();
        }
    }

    public Expr getWhereClause() {
        return whereClause;
    }

    public long getIndexId() {
        return indexId;
    }

    public void resetIndexIdForRestore(long id, String srcDbName, String dbName) {
        indexId = id;

        // the source db name is not setted in old BackupMeta, keep compatible with the old one.
        // See InitMaterializationContextHook.java:createSyncMvContexts for details.
        if (defineStmt != null && srcDbName != null) {
            String newStmt = defineStmt.originStmt.replaceAll(srcDbName, dbName);
            defineStmt = new OriginStatement(newStmt, defineStmt.idx);
        }
    }

    public KeysType getKeysType() {
        return keysType;
    }

    public void setKeysType(KeysType keysType) {
        this.keysType = keysType;
    }

    public TStorageType getStorageType() {
        return storageType;
    }

    public List<Index> getIndexes() {
        return indexes != null ? indexes : Lists.newArrayList();
    }

    public void setIndexes(List<Index> newIndexes) {
        this.indexes = newIndexes;
    }

    public String getDbName() {
        return dbName;
    }

    public List<Column> getSchema() {
        return getSchema(true);
    }

    public List<Column> getSchema(boolean full) {
        if (full) {
            return schema;
        } else {
            return schema.stream().filter(column -> column.isVisible()).collect(Collectors.toList());
        }
    }

    public void setSchema(List<Column> newSchema) throws IOException {
        this.schema = newSchema;
        parseStmt();
        initColumnNameMap();
    }

    public List<Column> getPrefixKeyColumns() {
        List<Column> keys = Lists.newArrayList();
        for (Column col : schema) {
            if (col.isKey()) {
                keys.add(col);
            } else {
                break;
            }
        }
        return keys;
    }

    public void setSchemaHash(int newSchemaHash) {
        this.schemaHash = newSchemaHash;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public short getShortKeyColumnCount() {
        return shortKeyColumnCount;
    }

    public void setSchemaVersion(int newSchemaVersion) {
        this.schemaVersion = newSchemaVersion;
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    private void setColumnsDefineExpr(List<Expr> columnDefineExprs) throws IOException {
        int size = columnDefineExprs.size();
        if (size <= schema.size()) {
            for (int i = 0; i < size; ++i) {
                schema.get(i).setDefineExpr(columnDefineExprs.get(i));
            }
        } else {
            LOG.warn(String.format("columns size %d in schema is smaller than column define expr size %d",
                    schema.size(), size));
        }
    }

    public static String normalizeName(String name) {
        return name.replace("`", "");
    }

    public static boolean matchColumnName(String lhs, String rhs) {
        return normalizeName(lhs).equalsIgnoreCase(normalizeName(rhs));
    }

    public Column getColumnByDefineName(String colDefineName) {
        String normalizedName = normalizeName(colDefineName);
        return definedNameToColumn.getOrDefault(normalizedName, null);
    }

    public Column getColumnByName(String columnName) {
        String normalizedName = normalizeName(columnName);
        return nameToColumn.getOrDefault(normalizedName, null);
    }

    public OriginStatement getDefineStmt() {
        return defineStmt;
    }

    public SyncMvMetrics getSyncMvMetrics() {
        return syncMvMetrics;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MaterializedIndexMeta)) {
            return false;
        }
        MaterializedIndexMeta other = (MaterializedIndexMeta) obj;

        return indexId == other.indexId
                && schema.size() == other.schema.size()
                && schema.equals(other.schema)
                && schemaVersion == other.schemaVersion
                && schemaHash == other.schemaHash
                && shortKeyColumnCount == other.shortKeyColumnCount
                && storageType == other.storageType
                && keysType == other.keysType
                && maxColUniqueId == other.maxColUniqueId;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        initColumnNameMap();
    }

    public void parseStmt() throws IOException {
        // analyze define stmt
        if (defineStmt == null) {
            return;
        }
        try {
            NereidsParser nereidsParser = new NereidsParser();
            CreateMaterializedViewCommand command = (CreateMaterializedViewCommand) nereidsParser.parseSingle(
                    defineStmt.originStmt);
            boolean tmpCreate = false;
            ConnectContext ctx = ConnectContext.get();
            try {
                if (ctx == null) {
                    tmpCreate = true;
                    ctx = ConnectContextUtil.getDummyCtx(dbName);
                } else {
                    // may cause by org.apache.doris.alter.AlterJobV2.run
                    if (ctx.getStatementContext() == null) {
                        StatementContext statementContext = new StatementContext();
                        statementContext.setConnectContext(ctx);
                        ctx.setStatementContext(statementContext);
                    }
                    if (StringUtils.isEmpty(ctx.getDatabase())) {
                        ctx.setDatabase(dbName);
                    }
                    if (ctx.getEnv() == null) {
                        ctx.setEnv(Env.getCurrentEnv());
                    }
                    if (ctx.getCurrentUserIdentity() == null) {
                        ctx.setCurrentUserIdentity(UserIdentity.ADMIN);
                    }
                }
                command.validate(ctx);
            } finally {
                if (tmpCreate) {
                    ctx.cleanup();
                }
            }

            if (command.getWhereClauseItem() != null) {
                setWhereClause(command.getWhereClauseItem().getDefineExpr());
            }
            try {
                List<MVColumnItem> mvColumnItemList = command.getMVColumnItemList();
                List<Expr> columnDefineExprs = new ArrayList<>(mvColumnItemList.size());
                for (MVColumnItem item : mvColumnItemList) {
                    Expr defineExpr = item.getDefineExpr();
                    defineExpr.disableTableName();
                    columnDefineExprs.add(defineExpr);
                }
                setColumnsDefineExpr(columnDefineExprs);
            } catch (Exception e) {
                LOG.warn("CreateMaterializedViewCommand parseDefineExpr failed, reason=", e);
            }
        } catch (Exception e) {
            throw new IOException("error happens when parsing create materialized view stmt: " + defineStmt, e);
        }
    }

    //take care: only use when creating MaterializedIndexMeta's schema.
    public int incAndGetMaxColUniqueId() {
        this.maxColUniqueId++;
        return this.maxColUniqueId;
    }

    public int getMaxColUniqueId() {
        return this.maxColUniqueId;
    }

    public void setMaxColUniqueId(int maxColUniqueId) {
        this.maxColUniqueId = maxColUniqueId;
    }

    public void initSchemaColumnUniqueId() {
        maxColUniqueId = Column.COLUMN_UNIQUE_ID_INIT_VALUE;
        this.schema.forEach(column -> {
            column.setUniqueId(incAndGetMaxColUniqueId());
            if (LOG.isDebugEnabled()) {
                LOG.debug("indexId: {},  column:{}, uniqueId:{}",
                        indexId, column, column.getUniqueId());
            }
        });
    }

    public void initColumnNameMap() {
        // case insensitive
        nameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        definedNameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (Column column : schema) {
            nameToColumn.put(normalizeName(column.getName()), column);
            definedNameToColumn.put(normalizeName(column.getDefineName()), column);
        }
    }
}
