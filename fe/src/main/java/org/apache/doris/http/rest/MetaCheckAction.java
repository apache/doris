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
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.common.DdlException;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import java.util.List;
import java.util.Map;

import io.netty.handler.codec.http.HttpMethod;

/*
 * This action is used to check metadata.
 * It is usually used to check some metadata information, such as whether the metadata information is correct.
 */
public class MetaCheckAction extends RestBaseAction {

    private static final String TYPE_PARAM = "type";
    private static final String TYPE_REPLICA_ALLOCATION = "replica_allocation";

    public MetaCheckAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/metacheck/", new MetaCheckAction(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response) {
        if (Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            response.setContentType("text/plain");
            response.getContent().append("Access denied. Need ADMIN privilege");
            sendResult(request, response);
        }

        String type = request.getSingleParameter(TYPE_PARAM);
        if (Strings.isNullOrEmpty(type)) {
            response.setContentType("text/plain");
            response.getContent().append("Missing type parameter");
            sendResult(request, response);
        } else if (type.equalsIgnoreCase(TYPE_REPLICA_ALLOCATION)) {
            checkReplicaAllocation(request, response);
        } else {
            response.setContentType("text/plain");
            response.getContent().append("Unsupported type: " + type);
            sendResult(request, response);
        }
    }

    /*
     * check the following metadata:
     * 1. All olap table's partition info has been set replica allocation, and idToReplicationNum is empty
     * 2. All backend's tag set has been set
     */
    private void checkReplicaAllocation(BaseRequest request, BaseResponse response) {
        Gson gson = new Gson();
        Map<String, Object> result = Maps.newHashMap();
        result.put("status", "ok");
        Map<String, String> partitionInfos = Maps.newHashMap();
        Map<Long, String> backendInfos = Maps.newHashMap();

        result.put("partition_info", partitionInfos);
        result.put("backend", backendInfos);

        // 1. check partition info
        Catalog catalog = Catalog.getCheckpoint();
        List<Long> dbIds = catalog.getDbIds();
        for (Long dbId : dbIds) {
            Database db = catalog.getDb(dbId);
            if (db == null) {
                continue;
            }
            db.readLock();
            try {
                for (Table table : db.getTables()) {
                    if (table.getType() != TableType.OLAP) {
                        continue;
                    }
                    PartitionInfo partitionInfo = ((OlapTable) table).getPartitionInfo();
                    try {
                        partitionInfo.checkReplicaAllocation();
                    } catch (DdlException e) {
                        result.put("status", "failed");
                        partitionInfos.put(db.getFullName() + "." + table.getName(), e.getMessage());
                    }
                }
            } finally {
                db.readUnlock();
            }
        }

        // 2. check backends
        SystemInfoService infoService = Catalog.getCurrentSystemInfo();
        List<Long> backendIds = infoService.getBackendIds(false);
        for (Long beId : backendIds) {
            Backend be = infoService.getBackend(beId);
            if (be == null) {
                continue;
            }

            try {
                be.checkTagSetConverted();
            } catch (DdlException e) {
                result.put("status", "failed");
                backendInfos.put(be.getId(), e.getMessage());
            }
        }

        response.setContentType("application/json");
        response.getContent().append(gson.toJson(result));
        sendResult(request, response);
    }
}
