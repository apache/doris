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
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.lance.LanceExternalTable;
import org.apache.doris.datasource.lance.LanceTableMetadata;
import org.apache.doris.datasource.lance.LanceTypeConverter;
import org.apache.doris.datasource.lance.source.LanceScanNode;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TExternalSearchRequest;
import org.apache.doris.thrift.TSearchFilter;
import org.apache.doris.thrift.TSearchFilterFormat;

import org.apache.arrow.vector.types.pojo.Field;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/** Common immutable planning state and validation for Lance external-search relation TVFs. */
abstract class LanceExternalSearchTableValuedFunction extends TableValuedFunctionIf {
    protected static final String TABLE = "table";
    protected static final String COLUMN = "column";
    protected static final String TOP_K = "top_k";
    protected static final String OFFSET = "offset";
    protected static final String FILTER = "filter";

    private static final String FULLY_QUALIFIED_TABLE_NAME_ERROR =
            "'table' must be a fully qualified catalog.database.table name";
    private static final long UINT32_MAX = 0xFFFF_FFFFL;

    private final String displayName;
    private final TableName sourceTableName;
    private final LanceExternalTable sourceTable;
    private final LanceTableMetadata metadata;
    private final int fieldId;
    private final TExternalSearchRequest searchRequest;
    private final List<Column> columns;
    private final long topK;
    private final long offset;

    protected LanceExternalSearchTableValuedFunction(PreparedSearch prepared) {
        CommonSearch common = prepared.common;
        this.displayName = common.displayName;
        this.sourceTableName = common.sourceTableName;
        this.sourceTable = common.sourceTable;
        this.metadata = common.metadata;
        this.fieldId = prepared.fieldId;
        this.searchRequest = prepared.searchRequest.deepCopy();
        this.columns = Collections.unmodifiableList(new ArrayList<>(prepared.columns));
        this.topK = common.topK;
        this.offset = common.offset;
    }

    public final LanceExternalTable getSourceTable() {
        return sourceTable;
    }

    public final LanceTableMetadata getMetadata() {
        return metadata;
    }

    public final TExternalSearchRequest getSearchRequest() {
        return searchRequest.deepCopy();
    }

    public final long getTopK() {
        return topK;
    }

    public final long getOffset() {
        return offset;
    }

    @Override
    public final String getTableName() {
        return displayName + "<" + sourceTableName + ">";
    }

    @Override
    public final List<Column> getTableColumns() {
        return columns;
    }

    @Override
    public final ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc, SessionVariable sv) {
        return LanceScanNode.forExternalSearch(
                id, desc, sourceTable, metadata, fieldId, searchRequest, sv);
    }

    protected static Map<String, String> normalizeProperties(Map<String, String> properties,
            Set<String> allowedProperties, String functionName) throws AnalysisException {
        Map<String, String> normalized = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey().toLowerCase(Locale.ROOT);
            if (!allowedProperties.contains(key)) {
                throw new AnalysisException("'" + entry.getKey()
                        + "' is an invalid property for " + functionName + "()");
            }
            if (normalized.put(key, entry.getValue()) != null) {
                throw new AnalysisException(
                        "Duplicate " + functionName + "() property '" + key + "'");
            }
        }
        return normalized;
    }

    protected static String required(Map<String, String> params, String key, String functionName)
            throws AnalysisException {
        String value = params.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new AnalysisException(
                    "Missing required " + functionName + "() property '" + key + "'");
        }
        return value.trim();
    }

    protected static CommonSearch prepareCommon(Map<String, String> params, String functionName,
            String displayName, String searchDescription, boolean loadIndexMetadata)
            throws AnalysisException {
        TableName sourceTableName = parseTableName(required(params, TABLE, functionName));
        LanceExternalTable sourceTable = findLanceExternalTable(sourceTableName);
        LanceTableMetadata metadata;
        try {
            metadata = loadIndexMetadata
                    ? sourceTable.loadMetadataForSearch() : sourceTable.loadMetadata();
        } catch (RuntimeException e) {
            throw new AnalysisException("Failed to load Lance metadata for " + searchDescription
                    + " on " + sourceTableName + ": " + e.getMessage(), e);
        }
        if (metadata.getVersion() <= 0) {
            throw new AnalysisException("Lance " + searchDescription
                    + " requires a fixed positive dataset version");
        }

        long topK = parseLong(params.getOrDefault(TOP_K, "10"), TOP_K, 1, Long.MAX_VALUE);
        long offset = parseLong(params.getOrDefault(OFFSET, "0"), OFFSET, 0, Long.MAX_VALUE);
        if (offset > UINT32_MAX || topK > UINT32_MAX - offset) {
            throw new AnalysisException("'top_k + offset' must not exceed " + UINT32_MAX);
        }
        return new CommonSearch(params, displayName, sourceTableName, sourceTable, metadata,
                topK, offset);
    }

    protected static PreparedSearch prepareSearch(CommonSearch common, int fieldId,
            TExternalSearchRequest searchRequest, String resultColumn, String searchDescription)
            throws AnalysisException {
        if (common.params.containsKey(FILTER)) {
            searchRequest.setSearchFilter(new TSearchFilter()
                    .setFormat(TSearchFilterFormat.SQL)
                    .setPayload(validateAndEncodeSqlFilter(common.params.get(FILTER))));
        }
        List<Column> columns = buildOutputColumns(
                common.metadata, resultColumn, searchDescription);
        return new PreparedSearch(common, fieldId, searchRequest, columns);
    }

    protected static int requireLanceFieldId(LanceTableMetadata metadata, Field field,
            String searchDescription) throws AnalysisException {
        OptionalInt fieldId = metadata.getLanceFieldId(field.getName());
        if (!fieldId.isPresent()) {
            throw new AnalysisException("Lance " + searchDescription + " column '"
                    + field.getName() + "' has no field ID in the Lance schema");
        }
        return fieldId.getAsInt();
    }

    protected static List<Column> buildOutputColumns(LanceTableMetadata metadata,
            String resultColumn, String searchDescription) throws AnalysisException {
        List<Column> result = new ArrayList<>(metadata.getSchema().getFields().size() + 1);
        Set<String> fieldNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        int position = 0;
        for (Field field : metadata.getSchema().getFields()) {
            if (!fieldNames.add(field.getName())) {
                throw new AnalysisException("Duplicate Lance schema column under "
                        + "case-insensitive matching: '" + field.getName() + "'");
            }
            if (field.getName().startsWith(Column.GLOBAL_ROWID_COL)) {
                throw new AnalysisException("Lance table contains column '" + field.getName()
                        + "' using reserved Doris internal column prefix '"
                        + Column.GLOBAL_ROWID_COL + "'");
            }
            if (field.getName().equalsIgnoreCase(resultColumn)) {
                throw new AnalysisException("Lance table already contains reserved "
                        + searchDescription + " column '" + resultColumn + "'");
            }
            String comment = field.getMetadata() == null
                    ? null : field.getMetadata().get("comment");
            Type type;
            try {
                type = LanceTypeConverter.toDorisType(field);
            } catch (RuntimeException e) {
                throw new AnalysisException("Invalid Lance type for column '" + field.getName()
                        + "': " + e.getMessage(), e);
            }
            result.add(new Column(field.getName(), type, false, null,
                    field.isNullable(), comment, true, position++));
        }
        result.add(new Column(resultColumn, Type.FLOAT, false, null,
                true, null, true, position));
        return result;
    }

    protected static TableName parseTableName(String value) throws AnalysisException {
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

    protected static LanceExternalTable findLanceExternalTable(TableName tableName)
            throws AnalysisException {
        ConnectContext context = ConnectContext.get();
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(context, tableName, PrivPredicate.SELECT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SELECT",
                    context.getQualifiedUser(), context.getRemoteIP(),
                    tableName.getDb() + ": " + tableName.getTbl());
        }
        CatalogIf<?> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(tableName.getCtl());
        if (!(catalog instanceof LanceExternalCatalog)) {
            throw new AnalysisException("Catalog '" + tableName.getCtl()
                    + "' is not a Lance catalog");
        }
        TableIf table = catalog.getDbOrAnalysisException(tableName.getDb())
                .getTableOrAnalysisException(tableName.getTbl());
        if (!(table instanceof LanceExternalTable)) {
            throw new AnalysisException("Table '" + tableName + "' is not a Lance table");
        }
        return (LanceExternalTable) table;
    }

    protected static byte[] validateAndEncodeSqlFilter(String filter) throws AnalysisException {
        if (filter == null || filter.trim().isEmpty()) {
            throw new AnalysisException("'filter' must not be empty");
        }
        if (filter.indexOf('\0') >= 0) {
            throw new AnalysisException("'filter' must not contain an embedded NUL byte");
        }
        return filter.getBytes(StandardCharsets.UTF_8);
    }

    protected static long parseLong(String value, String property, long min, long max)
            throws AnalysisException {
        try {
            long parsed = Long.parseLong(value);
            if (parsed < min || parsed > max) {
                throw new AnalysisException("'" + property + "' must be between "
                        + min + " and " + max);
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new AnalysisException("'" + property + "' must be an integer", e);
        }
    }

    protected static final class CommonSearch {
        private final Map<String, String> params;
        private final String displayName;
        private final TableName sourceTableName;
        private final LanceExternalTable sourceTable;
        private final LanceTableMetadata metadata;
        private final long topK;
        private final long offset;

        private CommonSearch(Map<String, String> params, String displayName,
                TableName sourceTableName, LanceExternalTable sourceTable,
                LanceTableMetadata metadata, long topK, long offset) {
            this.params = Collections.unmodifiableMap(new TreeMap<>(params));
            this.displayName = displayName;
            this.sourceTableName = sourceTableName;
            this.sourceTable = sourceTable;
            this.metadata = metadata;
            this.topK = topK;
            this.offset = offset;
        }

        protected Map<String, String> params() {
            return params;
        }

        protected LanceTableMetadata metadata() {
            return metadata;
        }

        protected long topK() {
            return topK;
        }

        protected long offset() {
            return offset;
        }
    }

    protected static final class PreparedSearch {
        private final CommonSearch common;
        private final int fieldId;
        private final TExternalSearchRequest searchRequest;
        private final List<Column> columns;

        private PreparedSearch(CommonSearch common, int fieldId,
                TExternalSearchRequest searchRequest, List<Column> columns) {
            this.common = common;
            this.fieldId = fieldId;
            this.searchRequest = searchRequest;
            this.columns = columns;
        }
    }
}
