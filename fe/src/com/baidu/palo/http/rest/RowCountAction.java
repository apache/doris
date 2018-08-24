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
import com.baidu.palo.catalog.MaterializedIndex;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.catalog.Partition;
import com.baidu.palo.catalog.Replica;
import com.baidu.palo.catalog.Table;
import com.baidu.palo.catalog.Table.TableType;
import com.baidu.palo.catalog.Tablet;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.http.ActionController;
import com.baidu.palo.http.BaseRequest;
import com.baidu.palo.http.BaseResponse;
import com.baidu.palo.http.IllegalArgException;
import com.baidu.palo.mysql.privilege.PrivPredicate;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import org.codehaus.jackson.map.ObjectMapper;

import java.util.Map;

import io.netty.handler.codec.http.HttpMethod;

/*
 * calc row count from replica to table
 * fe_host:fe_http_port/api/rowcount?db=dbname&table=tablename
 */
public class RowCountAction extends RestBaseAction {
    public static final String DB_NAME_PARAM = "db";
    public static final String TABLE_NAME_PARAM = "table";

    public RowCountAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/rowcount", new RowCountAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) throws DdlException {
        AuthorizationInfo authInfo = getAuthorizationInfo(request);
        checkGlobalAuth(authInfo, PrivPredicate.ADMIN);

        String dbName = request.getSingleParameter(DB_NAME_PARAM);
        if (Strings.isNullOrEmpty(dbName)) {
            throw new DdlException("No database selected.");
        }

        String tableName = request.getSingleParameter(TABLE_NAME_PARAM);
        if (Strings.isNullOrEmpty(tableName)) {
            throw new DdlException("No table selected.");
        }

        Map<String, Long> indexRowCountMap = Maps.newHashMap();
        Catalog catalog = Catalog.getInstance();
        Database db = catalog.getDb(dbName);
        if (db == null) {
            throw new DdlException("Database[" + dbName + "] does not exist");
        }
        db.writeLock();
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                throw new DdlException("Table[" + tableName + "] does not exist");
            }
            
            if (table.getType() != TableType.OLAP) {
                throw new DdlException("Table[" + tableName + "] is not OLAP table");
            }
            
            OlapTable olapTable = (OlapTable) table;
            for (Partition partition : olapTable.getPartitions()) {
                long version = partition.getCommittedVersion();
                long versionHash = partition.getCommittedVersionHash();
                for (MaterializedIndex index : partition.getMaterializedIndices()) {
                    long indexRowCount = 0L;
                    for (Tablet tablet : index.getTablets()) {
                        long tabletRowCount = 0L;
                        for (Replica replica : tablet.getReplicas()) {
                            if (replica.checkVersionCatchUp(version, versionHash)
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
            db.writeUnlock();
        }

        // to json response
        String result = "";
        ObjectMapper mapper = new ObjectMapper();
        try {
            result = mapper.writeValueAsString(indexRowCountMap);
        } catch (Exception e) {
            //  do nothing
        }

        // send result
        response.setContentType("application/json");
        response.getContent().append(result);
        sendResult(request, response);
    }
}
