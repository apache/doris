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
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.http.entity.HttpStatus;
import org.apache.doris.http.entity.ResponseEntity;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/*
 * used to get table's sorted tablet info
 * eg:
 *  fe_host:http_port/api/_migration?db=xxx&tbl=yyy
 */
@RestController
public class MigrationAction extends RestBaseController{
    private static final Logger LOG = LogManager.getLogger(MigrationAction.class);
    private static final String DB_PARAM = "db";
    private static final String TABLE_PARAM = "tbl";

    @RequestMapping(path = "/api/_migration",method = RequestMethod.GET)
    protected Object execute(HttpServletRequest request, HttpServletResponse response) throws DdlException {
        executeCheckPassword(request,response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build("Success");

        String dbName = request.getParameter(DB_PARAM);
        String tableName = request.getParameter(TABLE_PARAM);

        if (Strings.isNullOrEmpty(dbName)) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("Missing params. Need database name");
            return entity;
        }

        Database db = Catalog.getCurrentCatalog().getDb(dbName);
        if (db == null) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("Database[" + dbName + "] does not exist");
            return entity;
        }

        List<List<Comparable>> rows = Lists.newArrayList();
        db.readLock();
        try {
            if (!Strings.isNullOrEmpty(tableName)) {
                Table table = db.getTable(tableName);
                if (table == null) {
                    entity.setCode(HttpStatus.NOT_FOUND.value());
                    entity.setMsg("Table[" + tableName + "] does not exist");
                    return entity;
                }

                if (table.getType() != TableType.OLAP) {
                    entity.setCode(HttpStatus.NOT_FOUND.value());
                    entity.setMsg("Table[" + tableName + "] is not OlapTable");
                    return entity;
                }

                OlapTable olapTable = (OlapTable) table;

                for (Partition partition : olapTable.getPartitions()) {
                    String partitionName = partition.getName();
                    MaterializedIndex baseIndex = partition.getBaseIndex();
                    for (Tablet tablet : baseIndex.getTablets()) {
                        List<Comparable> row = Lists.newArrayList();
                        row.add(tableName);
                        row.add(partitionName);
                        row.add(tablet.getId());
                        row.add(olapTable.getSchemaHashByIndexId(baseIndex.getId()));
                        for (Replica replica : tablet.getReplicas()) {
                            row.add(replica.getBackendId());
                            break;
                        }
                        rows.add(row);
                    }
                }
            } else {
                // get all olap table
                for (Table table : db.getTables()) {
                    if (table.getType() != TableType.OLAP) {
                        continue;
                    }

                    OlapTable olapTable = (OlapTable) table;
                    tableName = table.getName();

                    for (Partition partition : olapTable.getPartitions()) {
                        String partitionName = partition.getName();
                        MaterializedIndex baseIndex = partition.getBaseIndex();
                        for (Tablet tablet : baseIndex.getTablets()) {
                            List<Comparable> row = Lists.newArrayList();
                            row.add(tableName);
                            row.add(partitionName);
                            row.add(tablet.getId());
                            row.add(olapTable.getSchemaHashByIndexId(baseIndex.getId()));
                            for (Replica replica : tablet.getReplicas()) {
                                row.add(replica.getBackendId());
                                break;
                            }
                            rows.add(row);
                        }
                    }
                }
            }

        } finally {
            db.readUnlock();
        }

        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0, 1, 2);
        Collections.sort(rows, comparator);

        entity.setData(rows);
        return entity;
    }
}
