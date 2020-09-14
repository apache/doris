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
        int tablet_total = 0;
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
                                        tablet_total++;
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

        JSONObject jsonObject_backend = new JSONObject();
        List<JSONObject> jsonArray_partition = new ArrayList<JSONObject>();
        for (long partitionId : partitionToTablet.keySet()) {
            JSONObject jsonObject_partition = new JSONObject();
            jsonObject_partition.put("partition_id", partitionId);
            jsonObject_partition.put("table_name", partitionToTable.get(partitionId));
            List<JSONObject> jsonArray_disk = new ArrayList<JSONObject>();
            for (DiskInfo disk : disks.values()) {
                long disk_path_hash = disk.getPathHash();
                JSONObject jsonObject_disk = new JSONObject();
                jsonObject_disk.put("disk_path_hash", disk_path_hash);
                List<JSONObject> jsonArray_tablet = new ArrayList<JSONObject>();
                int tablet_count = 0;
                List<Pair<Long, Integer>> tabletSchemaHash = partitionToTablet.get(partitionId);
                for (Pair<Long, Integer> tablet_info : tabletSchemaHash) {
                    long path_hash = tabletToPathHash.get(tablet_info);
                    if (path_hash == disk_path_hash) {
                        long tablet_id = tablet_info.first;
                        int schema_hash = tablet_info.second;
                        long data_size = tabletToDataSize.get(tablet_info);
                        JSONObject jsonObject_tablet = new JSONObject();
                        jsonObject_tablet.put("tablet_id", tablet_id);
                        jsonObject_tablet.put("schema_hash", schema_hash);
                        jsonObject_tablet.put("tablet_footprint", data_size);
                        jsonArray_tablet.add(jsonObject_tablet);
                        tablet_count++;
                    }
                }
                jsonObject_disk.put("tablets", jsonArray_tablet);
                jsonObject_disk.put("tablets_num", tablet_count);
                jsonObject_disk.put("total_capacity", disk.getTotalCapacityB());
                jsonObject_disk.put("used_percentage", disk.getUsedPct());
                jsonObject_disk.put("aviliable_capacity", disk.getAvailableCapacityB());
                jsonArray_disk.add(jsonObject_disk);
            }
            jsonObject_partition.put("disks", jsonArray_disk);
            jsonArray_partition.add(jsonObject_partition);
        }
        jsonObject_backend.put("tablets_distribution", jsonArray_partition);
        jsonObject_backend.put("backend", backend.getHost());
        jsonObject_backend.put("http_port", backend.getHttpPort());

        JSONObject jsonObject_result = new JSONObject();
        jsonObject_result.put("msg", "OK");
        jsonObject_result.put("code", 0);
        jsonObject_result.put("data", jsonObject_backend);
        jsonObject_result.put("count", tablet_total);
        String result =jsonObject_result.toString();

        response.setContentType("application/json");
        response.getContent().append(result);
        sendResult(request, response);
    }
}