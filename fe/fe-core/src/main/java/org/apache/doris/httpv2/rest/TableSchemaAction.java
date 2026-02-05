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

package org.apache.doris.httpv2.rest;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DorisHttpException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.rest.response.GsonSchemaResponse;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Get table schema for specified cluster.database.table with privilege checking
 */
@RestController
public class TableSchemaAction extends RestBaseController {

    /**
     * Build column information map for a given column
     * @param column the column to build info for
     * @return map containing column information
     */
    private Map<String, String> buildColumnInfo(Column column) {
        Map<String, String> columnInfo = new HashMap<>(2);
        Type colType = column.getOriginType();
        PrimitiveType primitiveType = colType.getPrimitiveType();

        if (primitiveType == PrimitiveType.DECIMALV2 || primitiveType.isDecimalV3Type()) {
            ScalarType scalarType = (ScalarType) colType;
            columnInfo.put("precision", scalarType.getPrecision() + "");
            columnInfo.put("scale", scalarType.getScalarScale() + "");
        }

        columnInfo.put("column_uid", String.valueOf(column.getUniqueId()));
        columnInfo.put("type", primitiveType.toString());
        columnInfo.put("comment", column.getComment());
        columnInfo.put("name", column.getDisplayName());

        Optional aggregationType = Optional.ofNullable(column.getAggregationType());
        columnInfo.put("aggregation_type", aggregationType.isPresent()
                ? column.getAggregationType().toSql() : "");
        columnInfo.put("is_nullable", column.isAllowNull() ? "Yes" : "No");
        columnInfo.put("is_key", column.isKey() ? "Yes" : "No");

        return columnInfo;
    }

    @RequestMapping(path = {"/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_schema",
            "/api/{" + CATALOG_KEY + "}/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_schema"}, method = RequestMethod.GET)
    protected Object schema(
            @PathVariable(value = CATALOG_KEY, required = false) String catalogName,
            @PathVariable(value = DB_KEY) final String dbName,
            @PathVariable(value = TABLE_KEY) final String tblName,
            HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        // just allocate 2 slot for top holder map
        Map<String, Object> resultMap = new HashMap<>(2);

        if (StringUtils.isBlank(catalogName)) {
            catalogName = InternalCatalog.INTERNAL_CATALOG_NAME;
        }

        try {
            String fullDbName = getFullDbName(dbName);
            // check privilege for select, otherwise return 401 HTTP status
            checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), catalogName, fullDbName, tblName,
                    PrivPredicate.SELECT);
            TableIf table;
            try {
                CatalogIf catalog = StringUtils.isNotBlank(catalogName) ? Env.getCurrentEnv().getCatalogMgr()
                        .getCatalogOrAnalysisException(catalogName) : Env.getCurrentInternalCatalog();
                DatabaseIf db = catalog.getDbOrMetaException(fullDbName);
                table = db.getTableOrMetaException(tblName);
            } catch (MetaNotFoundException | AnalysisException e) {
                return ResponseEntityBuilder.okWithCommonError(e.getMessage());
            }
            table.readLock();
            try {
                try {
                    List<Column> columns = table.getBaseSchema();
                    List<Map<String, String>> propList = new ArrayList(columns.size());
                    for (Column column : columns) {
                        Map<String, String> baseInfo = buildColumnInfo(column);
                        propList.add(baseInfo);
                    }
                    resultMap.put("status", 200);
                    if (table instanceof OlapTable) {
                        resultMap.put("keysType", ((OlapTable) table).getKeysType().name());
                        resultMap.put("schema_version", ((OlapTable) table).getBaseSchemaVersion());

                        // Add materialized index schemas for debugging
                        OlapTable olapTable = (OlapTable) table;
                        Map<Long, MaterializedIndexMeta> indexIdToMeta = olapTable.getIndexIdToMeta();
                        Map<String, Object> materializedIndexSchemas = new HashMap<>();

                        for (Map.Entry<Long, MaterializedIndexMeta> entry : indexIdToMeta.entrySet()) {
                            Long indexId = entry.getKey();
                            MaterializedIndexMeta indexMeta = entry.getValue();
                            String indexName = olapTable.getIndexNameById(indexId);

                            Map<String, Object> indexInfo = new HashMap<>();
                            indexInfo.put("index_id", indexId);
                            indexInfo.put("keys_type", indexMeta.getKeysType().name());
                            indexInfo.put("schema_version", indexMeta.getSchemaVersion());
                            indexInfo.put("schema_hash", indexMeta.getSchemaHash());
                            indexInfo.put("storage_type", indexMeta.getStorageType().name());

                            // Get schema columns for this materialized index
                            List<Column> indexColumns = indexMeta.getSchema();
                            List<Map<String, String>> indexColumnList = new ArrayList<>();

                            for (Column column : indexColumns) {
                                Map<String, String> columnInfo = buildColumnInfo(column);
                                indexColumnList.add(columnInfo);
                            }

                            indexInfo.put("columns", indexColumnList);
                            materializedIndexSchemas.put(indexName != null ? indexName : "index_" + indexId, indexInfo);
                        }

                        resultMap.put("materialized_indexes", materializedIndexSchemas);
                    }
                    resultMap.put("properties", propList);
                } catch (Exception e) {
                    // Transform the general Exception to custom DorisHttpException
                    return ResponseEntityBuilder.okWithCommonError(e.getMessage());
                }
            } finally {
                table.readUnlock();
            }
        } catch (DorisHttpException e) {
            // status code  should conforms to HTTP semantic
            resultMap.put("status", e.getCode().code());
            resultMap.put("exception", e.getMessage());
        }

        return ResponseEntityBuilder.ok(resultMap);
    }


    private static class DDLRequestBody {
        public Boolean isDropColumn;
        public String columnName;
    }

    /**
     * Request body:
     * {
     * "isDropColumn": 1,   // optional
     * "columnName" : "value1"   // optional
     * }
     */
    @RequestMapping(path = "/api/enable_light_schema_change/{" + DB_KEY
            + "}/{" + TABLE_KEY + "}", method = {RequestMethod.GET})
    public Object columnChangeCanSync(
            @PathVariable(value = DB_KEY) String dbName,
            @PathVariable(value = TABLE_KEY) String tableName,
            HttpServletRequest request, HttpServletResponse response, @RequestBody String body) {
        executeCheckPassword(request, response);
        String fullDbName = getFullDbName(dbName);
        OlapTable table;
        try {
            Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
            table = (OlapTable) db.getTableOrMetaException(tableName, Table.TableType.OLAP);
        } catch (MetaNotFoundException e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
        if (!table.getEnableLightSchemaChange()) {
            return ResponseEntityBuilder.okWithCommonError("table " + tableName + " disable light schema change");
        }
        java.lang.reflect.Type type = new TypeToken<DDLRequestBody>() {
        }.getType();
        DDLRequestBody ddlRequestBody = new Gson().fromJson(body, type);
        if (ddlRequestBody.isDropColumn) {
            boolean enableLightSchemaChange = true;
            // column should be dropped from both base and rollup indexes.
            for (Map.Entry<Long, List<Column>> entry : table.getIndexIdToSchema().entrySet()) {
                List<Column> baseSchema = entry.getValue();
                Iterator<Column> baseIter = baseSchema.iterator();
                while (baseIter.hasNext()) {
                    Column column = baseIter.next();
                    if (column.getName().equalsIgnoreCase(ddlRequestBody.columnName)) {
                        if (column.isKey()) {
                            enableLightSchemaChange = false;
                        }
                        break;
                    }
                }
            }
            if (!enableLightSchemaChange) {
                return ResponseEntityBuilder.okWithCommonError("Column " + ddlRequestBody.columnName
                        + " is primary key in materializedIndex that can't do the light schema change");
            }
        }
        return ResponseEntityBuilder.ok();
    }

    @RequestMapping(path = {"/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_gson_schema",
            "/api/{" + CATALOG_KEY + "}/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_gson_schema"}, method = RequestMethod.GET)
    protected Object gsonSchema(
            @PathVariable(value = CATALOG_KEY, required = false) String catalogName,
            @PathVariable(value = DB_KEY) final String dbName,
            @PathVariable(value = TABLE_KEY) final String tblName,
            HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        GsonSchemaResponse gsonSchemaResponse = new GsonSchemaResponse();
        if (StringUtils.isBlank(catalogName)) {
            catalogName = InternalCatalog.INTERNAL_CATALOG_NAME;
        }

        try {
            String fullDbName = getFullDbName(dbName);
            checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), catalogName, fullDbName, tblName,
                    PrivPredicate.SELECT);
            TableIf table;
            try {
                CatalogIf catalog = StringUtils.isNotBlank(catalogName) ? Env.getCurrentEnv().getCatalogMgr()
                        .getCatalogOrAnalysisException(catalogName) : Env.getCurrentInternalCatalog();
                DatabaseIf db = catalog.getDbOrMetaException(fullDbName);
                table = db.getTableOrMetaException(tblName);
            } catch (MetaNotFoundException | AnalysisException e) {
                return ResponseEntityBuilder.okWithCommonError(e.getMessage());
            }
            table.readLock();
            try {
                List<String> jsonColumns = table.getBaseSchema().stream()
                        .map(GsonUtils.GSON::toJson)
                        .collect(Collectors.toList());
                gsonSchemaResponse.setJsonColumns(jsonColumns);
                if (table instanceof OlapTable) {
                    gsonSchemaResponse.setKeysType(((OlapTable) table).getKeysType());
                } else {
                    gsonSchemaResponse.setKeysType(KeysType.UNKNOWN);
                }
            } finally {
                table.readUnlock();
            }
        } catch (Exception e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }

        return ResponseEntityBuilder.ok(gsonSchemaResponse);
    }

}
