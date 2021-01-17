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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/*
 * used to get a table's ddl stmt
 * eg:
 *  fe_host:http_port/api/_get_ddl?db=xxx&tbl=yyy
 */
@RestController
public class GetDdlStmtAction extends RestBaseController {

    private static final Logger LOG = LogManager.getLogger(GetDdlStmtAction.class);

    @RequestMapping(path = "/api/_get_ddl", method = RequestMethod.GET)
    public Object execute(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        String dbName = request.getParameter(DB_KEY);
        String tableName = request.getParameter(TABLE_KEY);

        if (Strings.isNullOrEmpty(dbName) || Strings.isNullOrEmpty(tableName)) {
            return ResponseEntityBuilder.badRequest("Missing params. Need database name and Table name");
        }

        String fullDbName = getFullDbName(dbName);
        Database db = Catalog.getCurrentCatalog().getDb(fullDbName);
        if (db == null) {
            return ResponseEntityBuilder.okWithCommonError("Database[" + dbName + "] does not exist");
        }

        List<String> createTableStmt = Lists.newArrayList();
        List<String> addPartitionStmt = Lists.newArrayList();
        List<String> createRollupStmt = Lists.newArrayList();

        Table table = db.getTable(tableName);
        if (table == null) {
            return ResponseEntityBuilder.okWithCommonError("Table[" + tableName + "] does not exist");
        }

        table.readLock();
        try {
            Catalog.getDdlStmt(table, createTableStmt, addPartitionStmt, createRollupStmt, true, false /* show password */);
        } finally {
            table.readUnlock();
        }

        Map<String, List<String>> results = Maps.newHashMap();
        results.put("create_table", createTableStmt);
        results.put("create_partition", addPartitionStmt);
        results.put("create_rollup", createRollupStmt);

        return ResponseEntityBuilder.ok(results);
    }
}
