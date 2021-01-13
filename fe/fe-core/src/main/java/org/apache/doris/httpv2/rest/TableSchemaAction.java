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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.DorisHttpException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Get table schema for specified cluster.database.table with privilege checking
 */
@RestController
public class TableSchemaAction extends RestBaseController {

    @RequestMapping(path = "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_schema", method = RequestMethod.GET)
    protected Object schema(
            @PathVariable(value = DB_KEY) final String dbName,
            @PathVariable(value = TABLE_KEY) final String tblName,
            HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        // just allocate 2 slot for top holder map
        Map<String, Object> resultMap = new HashMap<>(2);

        try {
            String fullDbName = getFullDbName(dbName);
            // check privilege for select, otherwise return 401 HTTP status
            checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, tblName, PrivPredicate.SELECT);
            Database db = Catalog.getCurrentCatalog().getDb(fullDbName);
            if (db == null) {
                return ResponseEntityBuilder.okWithCommonError("Database [" + dbName + "] " + "does not exists");
            }
            Table table = null;
            try {
                table = db.getTableOrThrowException(tblName, Table.TableType.OLAP);
            } catch (MetaNotFoundException e) {
                return ResponseEntityBuilder.okWithCommonError(e.getMessage());
            }
            table.readLock();
            try {
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
}
