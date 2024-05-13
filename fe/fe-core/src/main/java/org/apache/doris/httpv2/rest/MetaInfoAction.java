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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.common.proc.ProcService;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.exception.BadRequestException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * And meta info like databases, tables and schema
 */
@RestController
public class MetaInfoAction extends RestBaseController {

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
        boolean checkAuth = Config.enable_all_http_auth ? true : false;
        checkWithCookie(request, response, checkAuth);

        // use NS_KEY as catalog, but NS_KEY's default value is 'default_cluster'.
        if (ns.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            ns = InternalCatalog.INTERNAL_CATALOG_NAME;
        }

        // 1. get all database with privilege
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(ns);
        if (catalog == null) {
            return ResponseEntityBuilder.badRequest("Unknown catalog " + ns);
        }
        List<String> dbNames = catalog.getDbNames();
        List<String> dbNameSet = Lists.newArrayList();
        for (String fullName : dbNames) {
            final String db = ClusterNamespace.getNameFromFullName(fullName);
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkDbPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, fullName,
                            PrivPredicate.SHOW)) {
                continue;
            }
            dbNameSet.add(db);
        }

        Collections.sort(dbNames);

        // handle limit offset
        Pair<Integer, Integer> fromToIndex = getFromToIndex(request, dbNames.size());
        return ResponseEntityBuilder.ok(dbNames.subList(fromToIndex.first, fromToIndex.second));
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
        boolean checkAuth = Config.enable_all_http_auth ? true : false;
        checkWithCookie(request, response, checkAuth);

        if (!ns.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            return ResponseEntityBuilder.badRequest("Only support 'default_cluster' now");
        }

        String fullDbName = getFullDbName(dbName);
        Database db;
        try {
            db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
        } catch (MetaNotFoundException e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }

        List<String> tblNames = Lists.newArrayList();
        for (Table tbl : db.getTables()) {
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, fullDbName,
                            tbl.getName(),
                            PrivPredicate.SHOW)) {
                continue;
            }
            tblNames.add(tbl.getName());
        }

        Collections.sort(tblNames);

        // handle limit offset
        Pair<Integer, Integer> fromToIndex = getFromToIndex(request, tblNames.size());
        return ResponseEntityBuilder.ok(tblNames.subList(fromToIndex.first, fromToIndex.second));
    }

    /** Get schema of a table
     * {
     *   "msg": "success",
     *   "code": 0,
     *   "data": {
     *     "tbl1": {
     *       "schema": [{
     *         "Field": "k1",
     *         "Type": "INT",
     *         "Null": "Yes",
     *         "Extra": "",
     *         "Default": null,
     *         "Key": "true"
     *       }, {
     *         "Field": "k2",
     *         "Type": "INT",
     *         "Null": "Yes",
     *         "Extra": "",
     *         "Default": null,
     *         "Key": "true"
     *       }],
     *       "is_base": true
     *     },
     *     "r1": {
     *       "schema": [{
     *         "Field": "k1",
     *         "Type": "INT",
     *         "Null": "Yes",
     *         "Extra": "",
     *         "Default": null,
     *         "Key": "true"
     *       }],
     *       "is_base": false
     *     }
     *   },
     *   "count": 0
     * }
     */
    @RequestMapping(path = "/api/meta/" + NAMESPACES + "/{" + NS_KEY + "}/" + DATABASES + "/{" + DB_KEY + "}/" + TABLES
            + "/{" + TABLE_KEY + "}/schema",
            method = {RequestMethod.GET})
    public Object getTableSchema(
            @PathVariable(value = NS_KEY) String ns, @PathVariable(value = DB_KEY) String dbName,
            @PathVariable(value = TABLE_KEY) String tblName,
            HttpServletRequest request, HttpServletResponse response) throws UserException {
        boolean checkAuth = Config.enable_all_http_auth ? true : false;
        checkWithCookie(request, response, checkAuth);

        if (!ns.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            return ResponseEntityBuilder.badRequest("Only support 'default_cluster' now");
        }

        String fullDbName = getFullDbName(dbName);
        checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, tblName, PrivPredicate.SHOW);

        String withMvPara = request.getParameter(PARAM_WITH_MV);
        boolean withMv = !Strings.isNullOrEmpty(withMvPara) && withMvPara.equals("1");

        // get all proc paths
        Map<String, Map<String, Object>> result = Maps.newHashMap();
        Database db;
        Table tbl;
        try {
            db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
            tbl = db.getTableOrMetaException(tblName);
        } catch (MetaNotFoundException e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
        tbl.readLock();
        try {
            long baseId = -1;
            if (tbl.getType() == Table.TableType.OLAP) {
                baseId = ((OlapTable) tbl).getBaseIndexId();
            } else {
                baseId += tbl.getId();
            }
            String procPath = Joiner.on("/").join("", "dbs", db.getId(), tbl.getId(), "index_schema/", baseId);
            generateResult(tblName, true, procPath, result);

            if (withMv && tbl.getType() == Table.TableType.OLAP) {
                OlapTable olapTable = (OlapTable) tbl;
                for (long indexId : olapTable.getIndexIdListExceptBaseIndex()) {
                    procPath = Joiner.on("/").join("", "dbs", db.getId(), tbl.getId(), "index_schema/", indexId);
                    generateResult(olapTable.getIndexNameById(indexId), false, procPath, result);
                }
            }
        } finally {
            tbl.readUnlock();
        }

        return ResponseEntityBuilder.ok(result);
    }

    private void generateResult(String indexName, boolean isBaseIndex, String procPath,
                                Map<String, Map<String, Object>> result) throws UserException {
        Map<String, Object> propMap = result.get(indexName);
        if (propMap == null) {
            propMap = Maps.newHashMap();
            result.put(indexName, propMap);
        }

        propMap.put("is_base", isBaseIndex);
        propMap.put("schema", generateSchema(procPath));
    }

    List<Map<String, String>> generateSchema(String procPath) throws UserException {
        ProcNodeInterface node = ProcService.getInstance().open(procPath);
        if (node == null) {
            throw new DdlException("get schema with proc path failed: " + procPath);
        }

        List<Map<String, String>> schema = Lists.newArrayList();
        ProcResult procResult = node.fetchResult();
        List<String> colNames = procResult.getColumnNames();
        List<List<String>> rows = procResult.getRows();
        for (List<String> row : rows) {
            Preconditions.checkState(row.size() == colNames.size());
            Map<String, String> fieldMap = Maps.newHashMap();
            for (int i = 0; i < row.size(); i++) {
                fieldMap.put(colNames.get(i), convertIfNull(row.get(i)));
            }
            schema.add(fieldMap);
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
}
