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

package org.apache.doris.http.rest;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.DorisHttpException;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

import org.codehaus.jackson.map.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * This class is responsible for fetch the approximate row count of the specified table from cluster-meta data,
 * the approximate row maybe used for some computing system to decide use which compute-algorithm can be used
 * such as shuffle join or broadcast join.
 * <p>
 * This API is not intended to compute the exact row count of the specified table, if you need the exact row count,
 * please consider using the sql syntax `select count(*) from {table}`
 */
public class TableRowCountAction extends RestBaseAction {
    public TableRowCountAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET,
                                   "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_count",
                                   new TableRowCountAction(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException {
        // just allocate 2 slot for top holder map
        Map<String, Object> resultMap = new HashMap<>(4);
        String dbName = request.getSingleParameter(DB_KEY);
        String tableName = request.getSingleParameter(TABLE_KEY);
        try {
            if (Strings.isNullOrEmpty(dbName)
                    || Strings.isNullOrEmpty(tableName)) {
                throw new DorisHttpException(HttpResponseStatus.BAD_REQUEST, "{database}/{table} must be selected");
            }
            String fullDbName = ClusterNamespace.getFullName(ConnectContext.get().getClusterName(), dbName);
            // check privilege for select, otherwise return HTTP 401
            checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, tableName, PrivPredicate.SELECT);
            Database db = Catalog.getCurrentCatalog().getDb(fullDbName);
            if (db == null) {
                throw new DorisHttpException(HttpResponseStatus.NOT_FOUND, "Database [" + dbName + "] " + "does not exists");
            }

            Table table = db.getTable(tableName);
            if (table == null) {
                throw new DorisHttpException(HttpResponseStatus.NOT_FOUND, "Table [" + tableName + "] " + "does not exists");
            }
            // just only support OlapTable, ignore others such as ESTable
            if (!(table instanceof OlapTable)) {
                // Forbidden
                throw new DorisHttpException(HttpResponseStatus.FORBIDDEN, "Table [" + tableName + "] "
                        + "is not a OlapTable, only support OlapTable currently");
            }

            table.writeLock();
            try {
                OlapTable olapTable = (OlapTable) table;
                resultMap.put("status", 200);
                resultMap.put("size", olapTable.proximateRowCount());
            } finally {
                table.writeUnlock();
            }
        } catch (DorisHttpException e) {
            // status code  should conforms to HTTP semantic
            resultMap.put("status", e.getCode().code());
            resultMap.put("exception", e.getMessage());
        }
        ObjectMapper mapper = new ObjectMapper();
        try {
            String result = mapper.writeValueAsString(resultMap);
            // send result with extra information
            response.setContentType("application/json");
            response.getContent().append(result);
            sendResult(request, response, HttpResponseStatus.valueOf(Integer.parseInt(String.valueOf(resultMap.get("status")))));
        } catch (Exception e) {
            // may be this never happen
            response.getContent().append(e.getMessage());
            sendResult(request, response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
