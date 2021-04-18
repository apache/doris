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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Get table schema for specified cluster.database.table with privilege checking
 */
public class TableSchemaAction extends RestBaseAction {

    public TableSchemaAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        // the extra `/api` path is so disgusting
        controller.registerHandler(HttpMethod.GET,
                                   "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_schema", new TableSchemaAction
                                           (controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {
        // just allocate 2 slot for top holder map
        Map<String, Object> resultMap = new HashMap<>(2);
        String dbName = request.getSingleParameter(DB_KEY);
        String tableName = request.getSingleParameter(TABLE_KEY);
        try {
            if (Strings.isNullOrEmpty(dbName)
                    || Strings.isNullOrEmpty(tableName)) {
                throw new DorisHttpException(HttpResponseStatus.BAD_REQUEST, "No database or table selected.");
            }
            String fullDbName = ClusterNamespace.getFullName(ConnectContext.get().getClusterName(), dbName);
            // check privilege for select, otherwise return 401 HTTP status
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

            table.readLock();
            try {
                List<Column> columns = table.getBaseSchema();
                List<Map<String, String>> propList = new ArrayList(columns.size());
                for (Column column : columns) {
                    Map<String, String> baseInfo = new HashMap<>(2);
                    Type colType = column.getOriginType();
                    PrimitiveType primitiveType = colType.getPrimitiveType();
                    if (primitiveType == PrimitiveType.DECIMALV2 || primitiveType == PrimitiveType.DECIMAL) {
                        ScalarType scalarType = (ScalarType) colType;
                        baseInfo.put("precision", scalarType.getPrecision() + "");
                        baseInfo.put("scale", scalarType.getScalarScale() + "");
                    }
                    baseInfo.put("type", primitiveType.toString());
                    baseInfo.put("comment", column.getComment());
                    baseInfo.put("name", column.getDisplayName());
                    propList.add(baseInfo);
                }
                resultMap.put("status", 200);
                resultMap.put("properties", propList);
            } catch (Exception e) {
                // Transform the general Exception to custom DorisHttpException
                throw new DorisHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage() == null ? "Null Pointer Exception" : e.getMessage());
            } finally {
                table.readUnlock();
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
