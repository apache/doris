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

import com.google.common.base.Strings;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.DorisHttpException;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is responsible for fetch the approximate row count of the specified table from cluster-meta data,
 * the approximate row maybe used for some computing system to decide use which compute-algorithm can be used
 * such as shuffle join or broadcast join.
 * <p>
 * This API is not intended to compute the exact row count of the specified table, if you need the exact row count,
 * please consider using the sql syntax `select count(*) from {table}`
 */
public class RestCountAction extends RestBaseAction {
    public RestCountAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET,
                "/api/{cluster}/{database}/{table}/_count", new RestCountAction(controller));
        controller.registerHandler(HttpMethod.GET,
                "/api/{database}/{table}/_count", new RestCountAction(controller));
    }

    @Override
    protected void executeWithoutPassword(AuthorizationInfo authInfo, BaseRequest request, BaseResponse response) throws DdlException {
        // just allocate 2 slot for top holder map
        Map<String, Object> resultMap = new HashMap<>(4);
        String clusterName = request.getSingleParameter("cluster");
        String dbName = request.getSingleParameter("database");
        String tableName = request.getSingleParameter("table");
        try {
            if (Strings.isNullOrEmpty(clusterName)
                    || Strings.isNullOrEmpty(dbName)
                    || Strings.isNullOrEmpty(tableName)) {
                throw new DorisHttpException(HttpResponseStatus.BAD_REQUEST, "{cluster}/{database}/{table} must be selected");
            }
            String fullDbName = ClusterNamespace.getFullName(authInfo.cluster, dbName);
            // check privilege for select, otherwise return HTTP 401
            checkTblAuth(authInfo, fullDbName, tableName, PrivPredicate.SELECT);
            Database db = Catalog.getCurrentCatalog().getDb(fullDbName);
            if (db == null) {
                throw new DorisHttpException(HttpResponseStatus.NOT_FOUND, "Database [" + dbName + "] " + "does not exists");
            } else {
                db.writeLock();
                try {
                    Table table = db.getTable(tableName);
                    if (table == null) {
                        throw new DorisHttpException(HttpResponseStatus.NOT_FOUND, "Table [" + tableName + "] " + "does not exists");
                    } else {
                        // just only support OlapTable, ignore others such as ESTable, KuduTable
                        if (!(table instanceof OlapTable)) {
                            // Forbidden
                            throw new DorisHttpException(HttpResponseStatus.FORBIDDEN, "Table [" + tableName + "] "
                                    + "is not a OlapTable, only support OlapTable currently");
                        } else {
                            OlapTable olapTable = (OlapTable) table;
                            long totalCount = 0;
                            for (Partition partition : olapTable.getPartitions()) {
                                long version = partition.getVisibleVersion();
                                long versionHash = partition.getVisibleVersionHash();
                                for (MaterializedIndex index : partition.getMaterializedIndices()) {
                                    for (Tablet tablet : index.getTablets()) {
                                        long tabletRowCount = 0L;
                                        for (Replica replica : tablet.getReplicas()) {
                                            if (replica.checkVersionCatchUp(version, versionHash)
                                                    && replica.getRowCount() > tabletRowCount) {
                                                tabletRowCount = replica.getRowCount();
                                            }
                                        }
                                        totalCount += tabletRowCount;
                                    } // end for tablets
                                } // end for indices
                            } // end for partitions
                            resultMap.put("status", 200);
                            resultMap.put("size", totalCount);
                        }
                    }
                } finally {
                    db.writeUnlock();
                }
            }

        } catch (DorisHttpException e) {
            // status code  should conforms to HTTP semantic
            resultMap.put("status", e.getCode().code());
            resultMap.put("exception", e.getMessage());
        }
        ObjectMapper mapper = new ObjectMapper();
        try {
            String result = mapper.writeValueAsString(resultMap);
            // send result with extra information
            response.setContentType("application/json");
            response.getContent().append(result);
            sendResult(request, response, HttpResponseStatus.valueOf(Integer.parseInt(String.valueOf(resultMap.get("status")))));
        } catch (Exception e) {
            // may be this never happen
            response.getContent().append(e.getMessage());
            sendResult(request, response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
