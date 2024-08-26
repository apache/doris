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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.thrift.TStorageType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MaterializedIndexMeta implements Writable, GsonPostProcessable {
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

    @SerializedName(value = "dbName")
    private String dbName;

    private static final Logger LOG = LogManager.getLogger(MaterializedIndexMeta.class);


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
    }

    public void setWhereClause(Expr whereClause) {
        this.whereClause = whereClause;
        if (this.whereClause != null) {
            this.whereClause.setDisableTableName(true);
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
        parseStmt(null);
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

    private void setColumnsDefineExpr(Map<String, Expr> columnNameToDefineExpr) throws IOException {
        for (Map.Entry<String, Expr> entry : columnNameToDefineExpr.entrySet()) {
            boolean match = false;
            for (Column column : schema) {
                if (column.getName().equals(entry.getKey())) {
                    column.setDefineExpr(entry.getValue());
                    match = true;
                    break;
                }
            }

            if (!match) {
                // Compatibility code for older versions of mv
                // store_id -> mv_store_id
                // sale_amt -> mva_SUM__`sale_amt`
                // mv_count_sale_amt -> mva_SUM__CASE WHEN `sale_amt` IS NULL THEN 0 ELSE 1 END
                List<SlotRef> slots = new ArrayList<>();
                entry.getValue().collect(SlotRef.class, slots);

                String name = MaterializedIndexMeta.normalizeName(slots.get(0).toSqlWithoutTbl());
                Column matchedColumn = null;

                String columnList = "[";
                for (Column column : schema) {
                    if (columnList.length() != 1) {
                        columnList += ", ";
                    }
                    columnList += column.getName();
                }
                columnList += "]";

                for (Column column : schema) {
                    if (CreateMaterializedViewStmt.oldmvColumnBreaker(column.getName()).equalsIgnoreCase(name)) {
                        if (matchedColumn == null) {
                            matchedColumn = column;
                        } else {
                            LOG.warn("DefineExpr match multiple column in MaterializedIndex, ExprName=" + entry.getKey()
                                    + ", Expr=" + entry.getValue().toSqlWithoutTbl() + ", Slot=" + name
                                    + ", Columns=" + columnList);
                        }
                    }
                }

                if (matchedColumn != null) {
                    LOG.info("trans old MV: {},  DefineExpr:{}, DefineName:{}",
                            matchedColumn.getName(), entry.getValue().toSqlWithoutTbl(), entry.getKey());
                    matchedColumn.setDefineExpr(entry.getValue());
                    matchedColumn.setDefineName(entry.getKey());
                } else {
                    LOG.warn("DefineExpr does not match any column in MaterializedIndex, ExprName=" + entry.getKey()
                            + ", Expr=" + entry.getValue().toSqlWithoutTbl() + ", Slot=" + name
                            + ", Columns=" + columnList);
                }
            }
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
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static MaterializedIndexMeta read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, MaterializedIndexMeta.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        initColumnNameMap();
    }

    public void parseStmt(Analyzer analyzer) throws IOException {
        // analyze define stmt
        if (defineStmt == null) {
            return;
        }
        // parse the define stmt to schema
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(defineStmt.originStmt),
                SqlModeHelper.MODE_DEFAULT));
        CreateMaterializedViewStmt stmt;
        try {
            stmt = (CreateMaterializedViewStmt) SqlParserUtils.getStmt(parser, defineStmt.idx);
            stmt.setIsReplay(true);
            if (analyzer != null) {
                try {
                    stmt.analyze(analyzer);
                } catch (Exception e) {
                    LOG.warn("CreateMaterializedViewStmt analyze failed, mv=" + defineStmt.originStmt + ", reason=", e);
                    return;
                }
            }

            setWhereClause(stmt.getWhereClause());
            stmt.rewriteToBitmapWithCheck();
            try {
                Map<String, Expr> columnNameToDefineExpr = stmt.parseDefineExpr(analyzer);
                setColumnsDefineExpr(columnNameToDefineExpr);
            } catch (Exception e) {
                LOG.warn("CreateMaterializedViewStmt parseDefineExpr failed, reason=", e);
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
