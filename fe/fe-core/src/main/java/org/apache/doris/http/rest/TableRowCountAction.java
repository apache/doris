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
import org.apache.doris.common.DorisHttpException;
import org.apache.doris.http.entity.ResponseEntityBuilder;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This class is responsible for fetch the approximate row count of the specified table from cluster-meta data,
 * the approximate row maybe used for some computing system to decide use which compute-algorithm can be used
 * such as shuffle join or broadcast join.
 * <p>
 * This API is not intended to compute the exact row count of the specified table, if you need the exact row count,
 * please consider using the sql syntax `select count(*) from {table}`
 */
@RestController
public class TableRowCountAction extends RestBaseController {

    @RequestMapping(path = "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_count", method = RequestMethod.GET)
    public Object count(
            @PathVariable(value = DB_KEY) final String dbName,
            @PathVariable(value = TABLE_KEY) final String tblName,
            HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        // just allocate 2 slot for top holder map
        Map<String, Object> resultMap = new HashMap<>(4);
        try {
            String fullDbName = getFullDbName(dbName);
            // check privilege for select, otherwise return HTTP 401
            checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, tblName, PrivPredicate.SELECT);
            Database db = Catalog.getCurrentCatalog().getDb(fullDbName);
            if (db == null) {
                return ResponseEntityBuilder.okWithCommonError("Database [" + dbName + "] " + "does not exists");
            }
            db.writeLock();
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
                OlapTable olapTable = (OlapTable) table;
                resultMap.put("status", 200);
                resultMap.put("size", olapTable.proximateRowCount());
            } finally {
                db.writeUnlock();
            }
        } catch (DorisHttpException e) {
            // status code  should conforms to HTTP semantic
            resultMap.put("status", e.getCode().code());
            resultMap.put("exception", e.getMessage());
        }

        return ResponseEntityBuilder.ok(resultMap);
    }
}
