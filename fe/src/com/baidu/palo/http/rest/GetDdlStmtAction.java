// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.http.rest;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.Table;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.http.ActionController;
import com.baidu.palo.http.BaseRequest;
import com.baidu.palo.http.BaseResponse;
import com.baidu.palo.http.IllegalArgException;
import com.baidu.palo.mysql.privilege.PrivPredicate;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.List;
import java.util.Map;

import io.netty.handler.codec.http.HttpMethod;

/*
 * used to get a table's ddl stmt
 * eg:
 *  fe_host:http_port/api/_get_ddl?db=xxx&tbl=yyy
 */
public class GetDdlStmtAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(GetDdlStmtAction.class);
    private static final String DB_PARAM = "db";
    private static final String TABLE_PARAM = "tbl";

    public GetDdlStmtAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        GetDdlStmtAction action = new GetDdlStmtAction(controller);
        controller.registerHandler(HttpMethod.GET, "/api/_get_ddl", action);
    }

    @Override
    public void executeWithoutPassword(AuthorizationInfo authInfo, BaseRequest request, BaseResponse response)
            throws DdlException {
        checkGlobalAuth(authInfo, PrivPredicate.ADMIN);

        String dbName = request.getSingleParameter(DB_PARAM);
        String tableName = request.getSingleParameter(TABLE_PARAM);

        if (Strings.isNullOrEmpty(dbName) || Strings.isNullOrEmpty(tableName)) {
            throw new DdlException("Missing params. Need database name and Table name");
        }

        Database db = Catalog.getInstance().getDb(dbName);
        if (db == null) {
            throw new DdlException("Database[" + dbName + "] does not exist");
        }

        List<String> createTableStmt = Lists.newArrayList();
        List<String> addPartitionStmt = Lists.newArrayList();
        List<String> createRollupStmt = Lists.newArrayList();

        db.readLock();
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                throw new DdlException("Table[" + tableName + "] does not exist");
            }

            Catalog.getDdlStmt(table, createTableStmt, addPartitionStmt, createRollupStmt, true, (short) 1);

        } finally {
            db.readUnlock();
        }

        Map<String, List<String>> results = Maps.newHashMap();
        results.put("TABLE", createTableStmt);
        results.put("PARTITION", addPartitionStmt);
        results.put("ROLLUP", createRollupStmt);

        // to json response
        String result = "";
        ObjectMapper mapper = new ObjectMapper();
        try {
            result = mapper.writeValueAsString(results);
        } catch (Exception e) {
            //  do nothing
        }

        // send result
        response.setContentType("application/json");
        response.getContent().append(result);
        sendResult(request, response);
    }
}