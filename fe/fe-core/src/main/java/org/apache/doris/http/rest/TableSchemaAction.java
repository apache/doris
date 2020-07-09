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
import org.apache.doris.http.entity.HttpStatus;
import org.apache.doris.http.entity.ResponseEntity;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Get table schema for specified cluster.database.table with privilege checking
 */
public class TableSchemaAction extends RestBaseController {


    @RequestMapping(path = "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_schema",method = RequestMethod.GET)
    protected Object schema(HttpServletRequest request, HttpServletResponse response) throws DdlException {
        executeCheckPassword(request,response);
        // just allocate 2 slot for top holder map
        Map<String, Object> resultMap = new HashMap<>(2);
        String dbName = request.getParameter(DB_KEY);
        String tableName = request.getParameter(TABLE_KEY);
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build("Success");

        try {
            if (Strings.isNullOrEmpty(dbName)
                    || Strings.isNullOrEmpty(tableName)) {
                entity.setCode(HttpStatus.BAD_REQUEST.value());
                entity.setMsg("No database or table selected.");
                return entity;
            }
            String fullDbName = ClusterNamespace.getFullName(ConnectContext.get().getClusterName(), dbName);
            // check privilege for select, otherwise return 401 HTTP status
            checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, tableName, PrivPredicate.SELECT);
            Database db = Catalog.getCurrentCatalog().getDb(fullDbName);
            if (db == null) {
                entity.setCode(HttpStatus.NOT_FOUND.value());
                entity.setMsg("Database [" + dbName + "] " + "does not exists");
                return entity;
            }
            db.readLock();
            try {
                Table table = db.getTable(tableName);
                if (table == null) {
                    entity.setCode(HttpStatus.NOT_FOUND.value());
                    entity.setMsg("Table [" + tableName + "] " + "does not exists");
                    return entity;
                }
                // just only support OlapTable, ignore others such as ESTable
                if (!(table instanceof OlapTable)) {
                    entity.setCode(HttpStatus.FORBIDDEN.value());
                    entity.setMsg("Table [" + tableName + "] "
                            + "is not a OlapTable, only support OlapTable currently");
                    return entity;
                }
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
                        baseInfo.put("name", column.getName());
                        propList.add(baseInfo);
                    }
                    resultMap.put("status", 200);
                    resultMap.put("properties", propList);
                } catch (Exception e) {
                    entity.setCode(HttpStatus.INTERNAL_SERVER_ERROR.value());
                    entity.setMsg(e.getMessage() == null ? "Null Pointer Exception" : e.getMessage());
                    return entity;
                }
            } finally {
                db.readUnlock();
            }
        } catch (DorisHttpException e) {
            // status code  should conforms to HTTP semantic
            resultMap.put("status", e.getCode().code());
            resultMap.put("exception", e.getMessage());
        }
        try {
            entity.setData(resultMap);
        } catch (Exception e) {
            // may be this never happen
            entity.setData(e.getMessage());
        }
        return entity;
    }
}
