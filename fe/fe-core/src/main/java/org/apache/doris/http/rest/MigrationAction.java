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
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Collections;
import java.util.List;

import io.netty.handler.codec.http.HttpMethod;

/*
 * used to get table's sorted tablet info
 * eg:
 *  fe_host:http_port/api/_migration?db=xxx&tbl=yyy
 */
public class MigrationAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(MigrationAction.class);
    private static final String DB_PARAM = "db";
    private static final String TABLE_PARAM = "tbl";

    public MigrationAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        MigrationAction action = new MigrationAction(controller);
        controller.registerHandler(HttpMethod.GET, "/api/_migration", action);
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        String dbName = request.getSingleParameter(DB_PARAM);
        String tableName = request.getSingleParameter(TABLE_PARAM);

        if (Strings.isNullOrEmpty(dbName)) {
            throw new DdlException("Missing params. Need database name");
        }

        Database db = Catalog.getCurrentCatalog().getDb(dbName);
        if (db == null) {
            throw new DdlException("Database[" + dbName + "] does not exist");
        }

        List<List<Comparable>> rows = Lists.newArrayList();



        if (!Strings.isNullOrEmpty(tableName)) {
            Table table = db.getTable(tableName);
            if (table == null) {
                throw new DdlException("Table[" + tableName + "] does not exist");
            }

            if (table.getType() != TableType.OLAP) {
                throw new DdlException("Table[" + tableName + "] is not OlapTable");
            }

            OlapTable olapTable = (OlapTable) table;
            olapTable.readLock();
            try {
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
            } finally {
                olapTable.readUnlock();
            }
        } else {
            List<Table> tableList = null;
            db.readLock();
            try {
                tableList = db.getTables();
            } finally {
                db.readUnlock();
            }

            // get all olap table
            for (Table table : tableList) {
                if (table.getType() != TableType.OLAP) {
                    continue;
                }

                OlapTable olapTable = (OlapTable) table;
                table.readLock();
                try {
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
                } finally {
                    table.readUnlock();
                }
            }
        }
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0, 1, 2);
        Collections.sort(rows, comparator);

        // to json response
        String result = "";
        ObjectMapper mapper = new ObjectMapper();
        try {
            result = mapper.writeValueAsString(rows);
        } catch (Exception e) {
            //  do nothing
        }

        // send result
        response.setContentType("application/json");
        response.getContent().append(result);
        sendResult(request, response);
    }

    public static void print(String msg) {
        System.out.println(System.currentTimeMillis() + " " + msg);
    }
}
