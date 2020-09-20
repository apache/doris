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
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;
import org.apache.doris.system.Backend;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import io.netty.handler.codec.http.HttpMethod;

import org.json.JSONObject;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TabletDiskDistributionAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(TabletDiskDistributionAction.class);
    private static final String BE_HOST = "be_host";
    private static final String HTTP_PORT = "http_port";

    public TabletDiskDistributionAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction (ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/tablet_disk_distribution", new TabletDiskDistributionAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) throws DdlException {
        String be_host = request.getSingleParameter(BE_HOST);
        String http_port = request.getSingleParameter(HTTP_PORT);

        if (Strings.isNullOrEmpty(be_host) || Strings.isNullOrEmpty(http_port)) {
            throw new DdlException("Missing params. Need be_host and http_port");
        }

        Backend backend = Catalog.getCurrentSystemInfo().getBackendWithHttpPort(be_host, Integer.parseInt(http_port));
        ImmutableMap<String, DiskInfo> disks = backend.getDisks();

        Map<Long, List<Pair<Long, Integer>>> partitionToTablet = new HashMap<Long, List<Pair<Long, Integer>>>();
        Map<Pair<Long, Integer>, Long> tabletToDataSize = new HashMap<Pair<Long, Integer>, Long>();
        Map<Pair<Long, Integer>, Long> tabletToPathHash = new HashMap<Pair<Long, Integer>, Long>();
        Map<Long, String> partitionToTable = new HashMap<Long, String>();
        int tabletTotal = 0;
        List<Long> dbIds = Catalog.getCurrentCatalog().getDbIds();
        for (long dbId : dbIds) {
            Database db = Catalog.getCurrentCatalog().getDb(dbId);
            if (db == null) {
                continue;
            }
            db.readLock();
            try {
                List<Table> tables = db.getTables();
                for (Table table : tables) {
                    if (table == null) {
                        continue;
                    }
                    if (table.getType() != TableType.OLAP) {
                        continue;
                    }
                    String tableName = table.getName();
                    OlapTable olapTable = (OlapTable) table;
                    for (Partition partition : olapTable.getPartitions()) {
                        if (partition == null) {
                            continue;
                        }
                        long partitionId = partition.getId();
                        partitionToTable.put(partitionId, tableName);
                        List<Pair<Long, Integer>> tabletSchemaHash = Lists.newArrayList();
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                            for (Tablet tablet : index.getTablets()) {
                                if (tablet == null) {
                                    continue;
                                }
                                for (Replica replica : tablet.getReplicas()) {
                                    if (replica == null) {
                                        continue;
                                    }
                                    if (replica.getBackendId() == backend.getId()) {
                                        Pair<Long, Integer> pair = Pair.create(tablet.getId(), olapTable.getSchemaHashByIndexId(index.getId()));
                                        tabletSchemaHash.add(pair);
                                        tabletToDataSize.put(pair, replica.getDataSize());
                                        tabletToPathHash.put(pair, replica.getPathHash());
                                        tabletTotal++;
                                    }
                                }
                            }
                        }
                        partitionToTablet.put(partitionId, tabletSchemaHash);
                    }
                }
            } catch (Exception e) {
                LOG.info(e);
            } finally {
                db.readUnlock();
            }
        }

        JSONObject jsonObjectBackend = new JSONObject();
        List<JSONObject> jsonArrayPartition = new ArrayList<JSONObject>();
        for (long partitionId : partitionToTablet.keySet()) {
            JSONObject jsonObjectPartition = new JSONObject();
            jsonObjectPartition.put("partition_id", partitionId);
            jsonObjectPartition.put("table_name", partitionToTable.get(partitionId));
            List<JSONObject> jsonArrayDisk = new ArrayList<JSONObject>();
            for (DiskInfo disk : disks.values()) {
                long diskPathHash = disk.getPathHash();
                JSONObject jsonObjectDisk = new JSONObject();
                jsonObjectDisk.put("diskPathHash", diskPathHash);
                List<JSONObject> jsonArrayTablet = new ArrayList<JSONObject>();
                int tabletCount = 0;
                List<Pair<Long, Integer>> tabletSchemaHash = partitionToTablet.get(partitionId);
                for (Pair<Long, Integer> tablet_info : tabletSchemaHash) {
                    long path_hash = tabletToPathHash.get(tablet_info);
                    if (path_hash == diskPathHash) {
                        long tabletId = tablet_info.first;
                        int schemaHash = tablet_info.second;
                        long dataSize = tabletToDataSize.get(tablet_info);
                        JSONObject jsonObjectTablet = new JSONObject();
                        jsonObjectTablet.put("tablet_id", tabletId);
                        jsonObjectTablet.put("schema_hash", schemaHash);
                        jsonObjectTablet.put("tablet_size", dataSize);
                        jsonArrayTablet.add(jsonObjectTablet);
                        tabletCount++;
                    }
                }
                jsonObjectDisk.put("tablets", jsonArrayTablet);
                jsonObjectDisk.put("tablets_num", tabletCount);
                jsonObjectDisk.put("total_capacity", disk.getTotalCapacityB());
                jsonObjectDisk.put("used_percentage", disk.getUsedPct());
                jsonObjectDisk.put("aviliable_capacity", disk.getAvailableCapacityB());
                jsonArrayDisk.add(jsonObjectDisk);
            }
            jsonObjectPartition.put("disks", jsonArrayDisk);
            jsonArrayPartition.add(jsonObjectPartition);
        }
        jsonObjectBackend.put("tablets_distribution", jsonArrayPartition);
        jsonObjectBackend.put("backend", backend.getHost());
        jsonObjectBackend.put("http_port", backend.getHttpPort());

        JSONObject jsonObjectResult = new JSONObject();
        jsonObjectResult.put("msg", "OK");
        jsonObjectResult.put("code", 0);
        jsonObjectResult.put("data", jsonObjectBackend);
        jsonObjectResult.put("count", tabletTotal);
        String result =jsonObjectResult.toString();

        response.setContentType("application/json");
        response.getContent().append(result);
        sendResult(request, response);
    }
}
