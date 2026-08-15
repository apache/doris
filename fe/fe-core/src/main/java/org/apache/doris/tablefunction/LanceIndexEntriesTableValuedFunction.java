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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.lance.LanceExternalTable;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TLanceIndexMetadataParams;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Read-only inspection TVF listing the physical index entries of one Lance Directory catalog
 * table. Rows are produced on the FE master through the bounded catalog read path; this class
 * only validates arguments, authorization and the wire contract.
 */
public class LanceIndexEntriesTableValuedFunction extends MetadataTableValuedFunction {
    public static final String NAME = "lance_index_entries";
    static final String REST_CATALOG_REJECT_MESSAGE =
            "lance_index_entries is not supported for Lance REST catalogs";
    private static final String TABLE = "table";
    private static final Set<String> PROPERTIES = ImmutableSet.of(TABLE);
    private static final String FULLY_QUALIFIED_TABLE_NAME_ERROR =
            "'table' must be a fully qualified catalog.database.table name";
    private static final ImmutableList<Column> SCHEMA = ImmutableList.of(
            new Column("CatalogName", ScalarType.createStringType()),
            new Column("DatabaseName", ScalarType.createStringType()),
            new Column("TableName", ScalarType.createStringType()),
            new Column("IndexName", ScalarType.createStringType()),
            new Column("IndexUuid", ScalarType.createStringType()),
            new Column("DatasetVersion", PrimitiveType.BIGINT, true));
    private static final ImmutableMap<String, Integer> COLUMN_TO_INDEX = buildColumnIndex();

    private final TableName sourceTableName;

    public LanceIndexEntriesTableValuedFunction(Map<String, String> properties) throws AnalysisException {
        sourceTableName = parseTableName(normalizeProperties(properties).get(TABLE));

        // This check intentionally precedes catalog lookup/initialization and every provider call.
        checkShowPrivilege(ConnectContext.get(), sourceTableName);
        resolveLanceTable(sourceTableName);
    }

    public final String getCatalogName() {
        return sourceTableName.getCtl();
    }

    public final String getDatabaseName() {
        return sourceTableName.getDb();
    }

    public final String getSourceTableName() {
        return sourceTableName.getTbl();
    }

    @Override
    public final TMetadataType getMetadataType() {
        return TMetadataType.LANCE_INDEX_ENTRIES;
    }

    @Override
    public final TMetaScanRange getMetaScanRange(List<String> requiredFields) {
        TLanceIndexMetadataParams params = new TLanceIndexMetadataParams()
                .setCatalog(getCatalogName())
                .setDatabase(getDatabaseName())
                .setTable(getSourceTableName());
        return new TMetaScanRange()
                .setMetadataType(TMetadataType.LANCE_INDEX_ENTRIES)
                .setLanceIndexParams(params);
    }

    public static Integer getColumnIndexFromColumnName(String columnName) {
        return COLUMN_TO_INDEX.get(columnName.toLowerCase(Locale.ROOT));
    }

    static List<Column> getSchemaForTest() {
        return SCHEMA;
    }

    @Override
    public String getTableName() {
        return "LanceIndexEntriesTableValuedFunction";
    }

    @Override
    public List<Column> getTableColumns() {
        return SCHEMA;
    }

    @VisibleForTesting
    static TableName parseTableName(String value) throws AnalysisException {
        Expression expression;
        try {
            expression = new NereidsParser().parseExpression(value);
        } catch (ParseException e) {
            throw new AnalysisException(FULLY_QUALIFIED_TABLE_NAME_ERROR, e);
        }
        if (!(expression instanceof UnboundSlot)) {
            throw new AnalysisException(FULLY_QUALIFIED_TABLE_NAME_ERROR);
        }
        List<String> names = ((UnboundSlot) expression).getNameParts();
        if (names.size() != 3) {
            throw new AnalysisException(FULLY_QUALIFIED_TABLE_NAME_ERROR);
        }
        return new TableName(names.get(0), names.get(1), names.get(2));
    }

    @VisibleForTesting
    static Map<String, String> normalizeProperties(Map<String, String> properties)
            throws AnalysisException {
        Map<String, String> normalized = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey().toLowerCase(Locale.ROOT);
            if (!PROPERTIES.contains(key)) {
                throw new AnalysisException("'" + entry.getKey()
                        + "' is an invalid property for " + NAME);
            }
            if (normalized.containsKey(key)) {
                throw new AnalysisException("Duplicate " + NAME + " property '" + key + "'");
            }
            normalized.put(key, entry.getValue());
        }
        String table = normalized.get(TABLE);
        if (table == null || table.trim().isEmpty()) {
            throw new AnalysisException("Missing required " + NAME + " property '" + TABLE + "'");
        }
        normalized.put(TABLE, table.trim());
        return normalized;
    }

    static LanceExternalTable resolveLanceTable(TableName tableName) throws AnalysisException {
        CatalogIf<?> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(tableName.getCtl());
        if (!(catalog instanceof LanceExternalCatalog)) {
            throw new AnalysisException("Catalog '" + tableName.getCtl() + "' is not a Lance catalog");
        }
        // REST catalogs are rejected from configuration only, before any database or table
        // resolution that could trigger remote namespace requests or catalog initialization.
        if (((LanceExternalCatalog) catalog).isRestCatalogConfigured()) {
            throw new AnalysisException(REST_CATALOG_REJECT_MESSAGE);
        }
        TableIf table;
        try {
            table = catalog.getDbOrAnalysisException(tableName.getDb())
                    .getTableOrAnalysisException(tableName.getTbl());
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
        if (!(table instanceof LanceExternalTable)) {
            throw new AnalysisException("Table '" + tableName + "' is not a Lance table");
        }
        return (LanceExternalTable) table;
    }

    private static void checkShowPrivilege(ConnectContext context, TableName tableName)
            throws AnalysisException {
        if (context == null || !Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(context, tableName, PrivPredicate.SHOW)) {
            String user = context == null ? "unknown" : context.getQualifiedUser();
            String remoteIp = context == null ? "unknown" : context.getRemoteIP();
            throw new AnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR.formatErrorMsg(
                    "SHOW", user, remoteIp,
                    tableName.getDb() + ": " + tableName.getTbl()));
        }
    }

    private static ImmutableMap<String, Integer> buildColumnIndex() {
        ImmutableMap.Builder<String, Integer> builder = ImmutableMap.builder();
        for (int i = 0; i < SCHEMA.size(); i++) {
            builder.put(SCHEMA.get(i).getName().toLowerCase(Locale.ROOT), i);
        }
        return builder.build();
    }
}
