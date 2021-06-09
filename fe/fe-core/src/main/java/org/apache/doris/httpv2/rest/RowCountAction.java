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
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;

/*
 * calc row count from replica to table
 * fe_host:fe_http_port/api/rowcount?db=dbname&table=tablename
 */
@RestController
public class RowCountAction extends RestBaseController {

    @RequestMapping(path = "/api/rowcount", method = RequestMethod.GET)
    protected Object rowcount(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        String dbName = request.getParameter(DB_KEY);
        if (Strings.isNullOrEmpty(dbName)) {
            return ResponseEntityBuilder.badRequest("No database selected");
        }

        String tableName = request.getParameter(TABLE_KEY);
        if (Strings.isNullOrEmpty(tableName)) {
            return ResponseEntityBuilder.badRequest("No table selected");
        }

        String fullDbName = getFullDbName(dbName);
        Map<String, Long> indexRowCountMap = Maps.newHashMap();
        Catalog catalog = Catalog.getCurrentCatalog();
        Database db = catalog.getDb(fullDbName);
        if (db == null) {
            return ResponseEntityBuilder.okWithCommonError("Database[" + fullDbName + "] does not exist");
        }
        OlapTable olapTable = null;
        try {
            olapTable = (OlapTable) db.getTableOrThrowException(tableName, Table.TableType.OLAP);
        } catch (MetaNotFoundException e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
        olapTable.readLock();
        try {
            for (Partition partition : olapTable.getAllPartitions()) {
                long version = partition.getVisibleVersion();
                long versionHash = partition.getVisibleVersionHash();
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    long indexRowCount = 0L;
                    for (Tablet tablet : index.getTablets()) {
                        long tabletRowCount = 0L;
                        for (Replica replica : tablet.getReplicas()) {
                            if (replica.checkVersionCatchUp(version, versionHash, false)
                                    && replica.getRowCount() > tabletRowCount) {
                                tabletRowCount = replica.getRowCount();
                            }
                        }
                        indexRowCount += tabletRowCount;
                    } // end for tablets
                    index.setRowCount(indexRowCount);
                    indexRowCountMap.put(olapTable.getIndexNameById(index.getId()), indexRowCount);
                } // end for indices
            } // end for partitions
        } finally {
            olapTable.readUnlock();
        }
        return ResponseEntityBuilder.ok(indexRowCountMap);
    }
}
