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
import org.apache.doris.catalog.Table;
import org.apache.doris.common.DdlException;
import org.apache.doris.http.entity.HttpStatus;
import org.apache.doris.http.entity.ResponseEntity;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.List;
import java.util.Map;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

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
    private static final String DB_PARAM = "db";
    private static final String TABLE_PARAM = "tbl";

    @RequestMapping(path = "/api/_get_ddl",method = RequestMethod.GET)
    public Object execute(HttpServletRequest request, HttpServletResponse response)
            throws DdlException {
        executeCheckPassword(request,response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build("Success");

        String dbName = request.getParameter(DB_PARAM);
        String tableName = request.getParameter(TABLE_PARAM);

        if (Strings.isNullOrEmpty(dbName) || Strings.isNullOrEmpty(tableName)) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("Missing params. Need database name and Table name");
            return  entity;
        }

        Database db = Catalog.getCurrentCatalog().getDb(dbName);
        if (db == null) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("Database[" + dbName + "] does not exist");
            return  entity;
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

            Catalog.getDdlStmt(table, createTableStmt, addPartitionStmt, createRollupStmt, true, false /* show password */);

        } finally {
            db.readUnlock();
        }

        Map<String, List<String>> results = Maps.newHashMap();
        results.put("TABLE", createTableStmt);
        results.put("PARTITION", addPartitionStmt);
        results.put("ROLLUP", createRollupStmt);

        entity.setData(results);
        return entity;
    }
}