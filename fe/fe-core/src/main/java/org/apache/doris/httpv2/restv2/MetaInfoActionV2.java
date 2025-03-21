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

package org.apache.doris.httpv2.restv2;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.exception.BadRequestException;
import org.apache.doris.httpv2.rest.RestBaseController;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.Setter;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * And meta info like databases, tables and schema
 */
@RestController
@RequestMapping("/rest/v2")
public class MetaInfoActionV2 extends RestBaseController {

    private static final String NAMESPACES = "namespaces";
    private static final String DATABASES = "databases";
    private static final String TABLES = "tables";
    private static final String PARAM_LIMIT = "limit";
    private static final String PARAM_OFFSET = "offset";
    private static final String PARAM_WITH_MV = "with_mv";

    /**
     * Get all databases
     * {
     *   "msg": "success",
     *   "code": 0,
     *   "data": [
     *     "db1",
     *     "doris_audit_db__",
     *     "information_schema"
     *   ],
     *   "count": 0
     * }
     */
    @RequestMapping(path = "/api/meta/" + NAMESPACES + "/{" + NS_KEY + "}/" + DATABASES,
            method = {RequestMethod.GET})
    public Object getAllDatabases(
            @PathVariable(value = NS_KEY) String ns,
            HttpServletRequest request, HttpServletResponse response) {
        checkWithCookie(request, response, false);

        if (!ns.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER) && !ns.equalsIgnoreCase(
                InternalCatalog.INTERNAL_CATALOG_NAME)) {
            return ResponseEntityBuilder.badRequest("Only support 'default_cluster/internal' now");
        }

        // 1. get all database with privilege
        List<String> dbNames = Env.getCurrentInternalCatalog().getDbNames();
        List<String> filteredDbNames = Lists.newArrayList();
        for (String fullName : dbNames) {
            final String db = ClusterNamespace.getNameFromFullName(fullName);
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkDbPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, fullName,
                            PrivPredicate.SHOW)) {
                continue;
            }
            filteredDbNames.add(db);
        }

        Collections.sort(filteredDbNames);

        // handle limit offset
        Pair<Integer, Integer> fromToIndex = getFromToIndex(request, filteredDbNames.size());
        return ResponseEntityBuilder.ok(filteredDbNames.subList(fromToIndex.first, fromToIndex.second));
    }

    /** Get all tables of a database
     * {
     *   "msg": "success",
     *   "code": 0,
     *   "data": [
     *     "tbl1",
     *     "tbl2"
     *   ],
     *   "count": 0
     * }
     */

    @RequestMapping(path = "/api/meta/" + NAMESPACES + "/{" + NS_KEY + "}/" + DATABASES + "/{" + DB_KEY + "}/" + TABLES,
            method = {RequestMethod.GET})
    public Object getTables(
            @PathVariable(value = NS_KEY) String ns, @PathVariable(value = DB_KEY) String dbName,
            HttpServletRequest request, HttpServletResponse response) {
        checkWithCookie(request, response, false);

        if (!ns.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER) && !ns.equalsIgnoreCase(
                InternalCatalog.INTERNAL_CATALOG_NAME)) {
            return ResponseEntityBuilder.badRequest("Only support 'default_cluster/internal' now");
        }

        String fullDbName = getFullDbName(dbName);
        Database db;
        try {
            db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
        } catch (MetaNotFoundException e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }

        List<String> tblNames = Lists.newArrayList();
        db.readLock();
        try {
            for (Table tbl : db.getTables()) {
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, fullDbName,
                                tbl.getName(), PrivPredicate.SHOW)) {
                    continue;
                }
                tblNames.add(tbl.getName());
            }
        } finally {
            db.readUnlock();
        }

        Collections.sort(tblNames);

        // handle limit offset
        Pair<Integer, Integer> fromToIndex = getFromToIndex(request, tblNames.size());
        return ResponseEntityBuilder.ok(tblNames.subList(fromToIndex.first, fromToIndex.second));
    }

    /**
     * {
     *     "msg": "success",
     *     "code": 0,
     *     "data": {
     *         "engineType": "OLAP",
     *         "schemaInfo": {
     *             "schemaMap": {
     *                 "tbl1": {
     *                     "schema": [{
     *                         "field": "k2",
     *                         "type": "INT",
     *                         "isNull": "true",
     *                         "defaultVal": null,
     *                         "key": "true",
     *                         "aggrType": "None",
     *                         "comment": ""
     *                     }],
     *                     "keyType": "DUP_KEYS",
     *                     "baseIndex": true
     *                 }
     *             }
     *         }
     *     },
     *     "count": 0
     * }
     */
    @RequestMapping(path = "/api/meta/" + NAMESPACES + "/{" + NS_KEY + "}/" + DATABASES + "/{" + DB_KEY + "}/" + TABLES
            + "/{" + TABLE_KEY + "}/schema",
            method = {RequestMethod.GET})
    public Object getTableSchema(
            @PathVariable(value = NS_KEY) String ns, @PathVariable(value = DB_KEY) String dbName,
            @PathVariable(value = TABLE_KEY) String tblName,
            HttpServletRequest request, HttpServletResponse response) throws UserException {
        checkWithCookie(request, response, false);

        if (!ns.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER) && !ns.equalsIgnoreCase(
                InternalCatalog.INTERNAL_CATALOG_NAME)) {
            return ResponseEntityBuilder.badRequest("Only support 'default_cluster/internal' now");
        }

        String fullDbName = getFullDbName(dbName);
        checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, tblName, PrivPredicate.SHOW);
        String withMvPara = request.getParameter(PARAM_WITH_MV);
        boolean withMv = !Strings.isNullOrEmpty(withMvPara) && withMvPara.equals("1");

        try {
            Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
            db.readLock();
            try {
                Table tbl = db.getTableOrMetaException(tblName, Table.TableType.OLAP);

                TableSchemaInfo tableSchemaInfo = new TableSchemaInfo();
                tableSchemaInfo.setEngineType(tbl.getType().toString());
                SchemaInfo schemaInfo = generateSchemaInfo(tbl, withMv);
                tableSchemaInfo.setSchemaInfo(schemaInfo);
                return ResponseEntityBuilder.ok(tableSchemaInfo);
            } finally {
                db.readUnlock();
            }
        } catch (MetaNotFoundException e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
    }

    private SchemaInfo generateSchemaInfo(Table tbl, boolean withMv) {
        SchemaInfo schemaInfo = new SchemaInfo();
        Map<String, TableSchema> schemaMap = Maps.newHashMap();
        if (tbl.getType() == Table.TableType.OLAP) {
            OlapTable olapTable = (OlapTable) tbl;
            long baseIndexId = olapTable.getBaseIndexId();
            TableSchema baseTableSchema = new TableSchema();
            baseTableSchema.setBaseIndex(true);
            baseTableSchema.setKeyType(olapTable.getKeysTypeByIndexId(baseIndexId).name());
            List<Schema> baseSchema = generateSchame(olapTable.getSchemaByIndexId(baseIndexId));
            baseTableSchema.setSchema(baseSchema);
            schemaMap.put(olapTable.getIndexNameById(baseIndexId), baseTableSchema);

            if (withMv) {
                for (long indexId : olapTable.getIndexIdListExceptBaseIndex()) {
                    TableSchema tableSchema = new TableSchema();
                    tableSchema.setBaseIndex(false);
                    tableSchema.setKeyType(olapTable.getKeysTypeByIndexId(indexId).name());
                    List<Schema> schema = generateSchame(olapTable.getSchemaByIndexId(indexId));
                    tableSchema.setSchema(schema);
                    schemaMap.put(olapTable.getIndexNameById(indexId), tableSchema);
                }
            }

        } else {
            TableSchema tableSchema = new TableSchema();
            tableSchema.setBaseIndex(false);
            List<Schema> schema = generateSchame(tbl.getBaseSchema());
            tableSchema.setSchema(schema);
            schemaMap.put(tbl.getName(), tableSchema);
            schemaInfo.setSchemaMap(schemaMap);
        }
        schemaInfo.setSchemaMap(schemaMap);
        return schemaInfo;
    }

    private List<Schema> generateSchame(List<Column> columns) {
        return columns.stream().map(column -> {
            Schema schema = new Schema();
            schema.setField(column.getName());
            schema.setType(column.getType().toString());
            schema.setIsNull(String.valueOf(column.isAllowNull()));
            schema.setDefaultVal(column.getDefaultValue());
            schema.setKey(String.valueOf(column.isKey()));
            schema.setAggrType(column.getAggregationType() == null
                    ? "None" : column.getAggregationType().toString());
            schema.setComment(column.getComment());
            return schema;
        }).collect(Collectors.toList());
    }

    private void generateResult(Table tbl, boolean isBaseIndex,
                                Map<String, Map<String, Object>> result) throws UserException {
        Map<String, Object> propMap = result.get(tbl.getName());
        if (propMap == null) {
            propMap = Maps.newHashMap();
            result.put(tbl.getName(), propMap);
        }

        propMap.put("isBase", isBaseIndex);
        propMap.put("tableType", tbl.getEngine());
        if (tbl.getType() == Table.TableType.OLAP) {
            propMap.put("keyType", ((OlapTable) tbl).getKeysType());
        }
        propMap.put("schema", generateSchema(tbl.getBaseSchema()));
    }

    List<Map<String, String>> generateSchema(List<Column> columns) throws UserException {
        List<Map<String, String>> schema = Lists.newArrayList();
        for (Column column : columns) {
            Map<String, String> colSchema = Maps.newHashMap();
            colSchema.put("Field", column.getName());
            colSchema.put("Type", column.getType().toString());
            colSchema.put("Null", String.valueOf(column.isAllowNull()));
            colSchema.put("Default", column.getDefaultValue());
            colSchema.put("Key", String.valueOf(column.isKey()));
            colSchema.put("AggType", column.getAggregationType().toString());
            colSchema.put("Comment", column.getComment());
            schema.add(colSchema);
        }
        return schema;
    }

    private String convertIfNull(String val) {
        return val.equals(FeConstants.null_string) ? null : val;
    }

    // get limit and offset from query parameter
    // and return fromIndex and toIndex of a list
    private Pair<Integer, Integer> getFromToIndex(HttpServletRequest request, int maxNum) {
        String limitStr = request.getParameter(PARAM_LIMIT);
        String offsetStr = request.getParameter(PARAM_OFFSET);

        int offset = 0;
        int limit = Integer.MAX_VALUE;
        if (Strings.isNullOrEmpty(limitStr)) {
            // limit not set
            if (!Strings.isNullOrEmpty(offsetStr)) {
                throw new BadRequestException("Param offset should be set with param limit");
            }
        } else {
            // limit is set
            limit = Integer.valueOf(limitStr);
            if (limit < 0) {
                throw new BadRequestException("Param limit should >= 0");
            }

            offset = 0;
            if (!Strings.isNullOrEmpty(offsetStr)) {
                offset = Integer.valueOf(offsetStr);
                if (offset < 0) {
                    throw new BadRequestException("Param offset should >= 0");
                }
            }
        }

        if (maxNum <= 0) {
            return Pair.of(0, 0);
        }
        return Pair.of(Math.min(offset, maxNum - 1), Math.min(limit + offset, maxNum));
    }

    @Getter
    @Setter
    public static class TableSchemaInfo {
        private String engineType;
        private SchemaInfo schemaInfo;
    }

    @Getter
    @Setter
    public static class SchemaInfo {
        // tbl(index name) -> schema
        private Map<String, TableSchema> schemaMap;
    }

    @Getter
    @Setter
    public static class TableSchema {
        private List<Schema> schema;
        private boolean isBaseIndex;
        private String keyType;
    }

    @Getter
    @Setter
    public static class Schema {
        private String field;
        private String type;
        private String isNull;
        private String defaultVal;
        private String key;
        private String aggrType;
        private String comment;
    }
}
