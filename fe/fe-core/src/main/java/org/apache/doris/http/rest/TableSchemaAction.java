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
import org.apache.doris.common.DorisHttpException;
import org.apache.doris.http.entity.ResponseEntityBuilder;
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
            db.readLock();
            try {
                Table table = db.getTable(tblName);
                if (table == null) {
                    return ResponseEntityBuilder.okWithCommonError("Table [" + tblName + "] " + "does not exists");
                }
                // just only support OlapTable, ignore others such as ESTable
                if (!(table instanceof OlapTable)) {
                    return ResponseEntityBuilder.okWithCommonError("Table [" + tblName + "] "
                            + "is not a OlapTable, only support OlapTable currently");
                }

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
            } finally {
                db.readUnlock();
            }
        } catch (DorisHttpException e) {
            // status code  should conforms to HTTP semantic
            resultMap.put("status", e.getCode().code());
            resultMap.put("exception", e.getMessage());
        }

        return ResponseEntityBuilder.ok(resultMap);
    }
}
