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
import org.apache.doris.datasource.lance.LanceVectorQuery;
import org.apache.doris.datasource.lance.source.LanceScanNode;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TExternalSearchQuery;
import org.apache.doris.thrift.TExternalSearchRequest;
import org.apache.doris.thrift.TLanceVectorSearchOptions;
import org.apache.doris.thrift.TSearchFilter;
import org.apache.doris.thrift.TSearchFilterFormat;
import org.apache.doris.thrift.TSearchVector;
import org.apache.doris.thrift.TVectorMetric;
import org.apache.doris.thrift.TVectorSearchParams;

import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.types.pojo.Field;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** Relation TVF for a whole-snapshot Lance vector search. */
public class VectorSearchTableValuedFunction extends TableValuedFunctionIf {
    public static final String NAME = "vector_search";
    public static final String DISTANCE_COLUMN = "_distance";

    private static final String TABLE = "table";
    private static final String COLUMN = "column";
    private static final String QUERY_VECTOR = "query_vector";
    private static final String TOP_K = "top_k";
    private static final String OFFSET = "offset";
    private static final String METRIC = "metric";
    private static final String FILTER = "filter";
    private static final String NPROBES = "nprobes";
    private static final String REFINE_FACTOR = "refine_factor";
    private static final String EF = "ef";
    private static final String USE_INDEX = "use_index";
    private static final long UINT32_MAX = 0xFFFF_FFFFL;
    private static final Set<String> PROPERTIES = ImmutableSet.of(
            TABLE, COLUMN, QUERY_VECTOR, TOP_K, OFFSET, METRIC, FILTER,
            NPROBES, REFINE_FACTOR, EF, USE_INDEX);

    private final TableName sourceTableName;
    private final LanceExternalTable sourceTable;
    private final LanceTableMetadata metadata;
    private final List<Column> columns;
    private final TExternalSearchRequest searchRequest;

    public VectorSearchTableValuedFunction(Map<String, String> properties)
            throws AnalysisException {
        Map<String, String> params = normalizeProperties(properties);
        sourceTableName = parseTableName(required(params, TABLE));
        checkSelectPrivilege(sourceTableName);
        sourceTable = resolveLanceTable(sourceTableName);
        try {
            metadata = sourceTable.loadMetadata();
        } catch (RuntimeException e) {
            throw new AnalysisException("Failed to load Lance metadata for vector search on "
                    + sourceTableName + ": " + e.getMessage(), e);
        }
        if (metadata.getVersion() <= 0) {
            throw new AnalysisException("Lance vector search requires a fixed positive dataset version");
        }

        Field vectorField = LanceVectorQuery.resolveVectorField(
                metadata.getSchema(), required(params, COLUMN));
        TSearchVector queryVector = LanceVectorQuery.encode(
                vectorField, required(params, QUERY_VECTOR));
        long topK = parseLong(params.getOrDefault(TOP_K, "10"), TOP_K, 1, Long.MAX_VALUE);
        long offset = parseLong(params.getOrDefault(OFFSET, "0"), OFFSET, 0, Long.MAX_VALUE);
        if (offset > UINT32_MAX || topK > UINT32_MAX - offset) {
            throw new AnalysisException("'top_k + offset' must not exceed " + UINT32_MAX);
        }

        TVectorSearchParams vectorParams = new TVectorSearchParams()
                .setColumn(vectorField.getName())
                .setQueryVector(queryVector)
                .setTopK(topK)
                .setOffset(offset);
        if (params.containsKey(METRIC)) {
            vectorParams.setMetric(parseMetric(params.get(METRIC)));
        }

        searchRequest = new TExternalSearchRequest()
                .setSchemaVersion(1)
                .setQuery(TExternalSearchQuery.vector(vectorParams));
        if (params.containsKey(FILTER)) {
            String filter = params.get(FILTER);
            if (filter == null || filter.trim().isEmpty()) {
                throw new AnalysisException("'filter' must not be empty");
            }
            searchRequest.setFilter(new TSearchFilter()
                    .setFormat(TSearchFilterFormat.SQL)
                    .setPayload(filter.getBytes(StandardCharsets.UTF_8)));
        }

        TLanceVectorSearchOptions lanceOptions = new TLanceVectorSearchOptions();
        boolean hasLanceOptions = false;
        if (params.containsKey(NPROBES)) {
            lanceOptions.setNprobes(parsePositiveInt(params.get(NPROBES), NPROBES));
            hasLanceOptions = true;
        }
        if (params.containsKey(REFINE_FACTOR)) {
            lanceOptions.setRefineFactor(
                    parsePositiveInt(params.get(REFINE_FACTOR), REFINE_FACTOR));
            hasLanceOptions = true;
        }
        if (params.containsKey(EF)) {
            lanceOptions.setEf(parsePositiveInt(params.get(EF), EF));
            hasLanceOptions = true;
        }
        if (params.containsKey(USE_INDEX)) {
            lanceOptions.setUseIndex(parseBoolean(params.get(USE_INDEX), USE_INDEX));
            hasLanceOptions = true;
        }
        if (hasLanceOptions) {
            searchRequest.setLanceOptions(lanceOptions);
        }
        columns = buildOutputColumns(metadata);
    }

    public LanceExternalTable getSourceTable() {
        return sourceTable;
    }

    public LanceTableMetadata getMetadata() {
        return metadata;
    }

    public TExternalSearchRequest getSearchRequest() {
        return searchRequest.deepCopy();
    }

    @Override
    public String getTableName() {
        return "VectorSearchTableValuedFunction<" + sourceTableName + ">";
    }

    @Override
    public List<Column> getTableColumns() {
        return columns;
    }

    @Override
    public ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc, SessionVariable sv) {
        return new LanceScanNode(id, desc, sourceTable, metadata,
                searchRequest, sv);
    }

    private static Map<String, String> normalizeProperties(Map<String, String> properties)
            throws AnalysisException {
        Map<String, String> normalized = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey().toLowerCase(Locale.ROOT);
            if (!PROPERTIES.contains(key)) {
                throw new AnalysisException("'" + entry.getKey()
                        + "' is an invalid property for vector_search()");
            }
            if (normalized.put(key, entry.getValue()) != null) {
                throw new AnalysisException("Duplicate vector_search() property '" + key + "'");
            }
        }
        return normalized;
    }

    private static String required(Map<String, String> params, String key)
            throws AnalysisException {
        String value = params.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new AnalysisException("Missing required vector_search() property '" + key + "'");
        }
        return value.trim();
    }

    private static TableName parseTableName(String value) throws AnalysisException {
        String[] names = value.split("\\.", -1);
        if (names.length != 3 || names[0].isEmpty() || names[1].isEmpty()
                || names[2].isEmpty()) {
            throw new AnalysisException("'table' must be a fully qualified "
                    + "catalog.database.table name");
        }
        return new TableName(names[0], names[1], names[2]);
    }

    private static void checkSelectPrivilege(TableName tableName) throws AnalysisException {
        ConnectContext context = ConnectContext.get();
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(context, tableName, PrivPredicate.SELECT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SELECT",
                    context.getQualifiedUser(), context.getRemoteIP(),
                    tableName.getDb() + ": " + tableName.getTbl());
        }
    }

    private static LanceExternalTable resolveLanceTable(TableName tableName)
            throws AnalysisException {
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

    private static List<Column> buildOutputColumns(LanceTableMetadata metadata)
            throws AnalysisException {
        List<Column> result = new ArrayList<>(metadata.getSchema().getFields().size() + 1);
        int position = 0;
        for (Field field : metadata.getSchema().getFields()) {
            if (field.getName().equalsIgnoreCase(DISTANCE_COLUMN)) {
                throw new AnalysisException("Lance table already contains reserved vector search "
                        + "column '" + DISTANCE_COLUMN + "'");
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
        result.add(new Column(DISTANCE_COLUMN, Type.FLOAT, false, null,
                true, null, true, position));
        return result;
    }

    private static long parseLong(String value, String property, long min, long max)
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

    private static int parsePositiveInt(String value, String property)
            throws AnalysisException {
        long parsed = parseLong(value, property, 1, Integer.MAX_VALUE);
        return (int) parsed;
    }

    private static boolean parseBoolean(String value, String property)
            throws AnalysisException {
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new AnalysisException("'" + property + "' must be 'true' or 'false'");
    }

    private static TVectorMetric parseMetric(String value) throws AnalysisException {
        switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "l2":
                return TVectorMetric.L2;
            case "cosine":
                return TVectorMetric.COSINE;
            case "dot":
            case "dot_product":
                return TVectorMetric.DOT_PRODUCT;
            case "hamming":
                return TVectorMetric.HAMMING;
            default:
                throw new AnalysisException("Unsupported vector metric '" + value
                        + "': expected l2, cosine, dot, or hamming");
        }
    }
}
