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
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TStorageType;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;

@RestController
public class StorageTypeCheckAction extends RestBaseController {

    @RequestMapping(path = "/api/_check_storagetype", method = RequestMethod.GET)
    protected Object check_storagetype(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        String dbName = request.getParameter(DB_KEY);
        if (Strings.isNullOrEmpty(dbName)) {
            return ResponseEntityBuilder.badRequest("No database selected");
        }

        String fullDbName = getFullDbName(dbName);
        Database db = Catalog.getCurrentCatalog().getDb(fullDbName);
        if (db == null) {
            return ResponseEntityBuilder.badRequest("Database " + dbName + " does not exist");
        }

        Map<String, Map<String, String>> result = Maps.newHashMap();
        List<Table> tbls = db.getTables();
        for (Table tbl : tbls) {
            if (tbl.getType() != TableType.OLAP) {
                continue;
            }

            OlapTable olapTbl = (OlapTable) tbl;
            olapTbl.readLock();
            try {
                Map<String, String> indexMap = Maps.newHashMap();
                for (Map.Entry<Long, MaterializedIndexMeta> entry : olapTbl.getIndexIdToMeta().entrySet()) {
                    MaterializedIndexMeta indexMeta = entry.getValue();
                    if (indexMeta.getStorageType() == TStorageType.ROW) {
                        indexMap.put(olapTbl.getIndexNameById(entry.getKey()), indexMeta.getStorageType().name());
                    }
                }
                result.put(tbl.getName(), indexMap);
            } finally {
                olapTbl.readUnlock();
            }
        }
        return ResponseEntityBuilder.ok(result);
    }
}
