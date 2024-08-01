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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.statistics.query.QueryStatsUtil;
import org.apache.doris.system.Backend;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.List;

/*
 * SHOW PROC /dbs/dbId/tableId/partitions/partitionId/indexId/tabletId
 * show replicas' detail info within a tablet
 */
public class ReplicasProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>().add("ReplicaId")
            .add("BackendId").add("Version").add("LstSuccessVersion").add("LstFailedVersion").add("LstFailedTime")
            .add("SchemaHash").add("LocalDataSize").add("RemoteDataSize").add("RowCount").add("State").add("IsBad")
            .add("IsUserDrop")
            .add("VisibleVersionCount").add("VersionCount").add("PathHash").add("Path")
            .add("MetaUrl").add("CompactionStatus").add("CooldownReplicaId")
            .add("CooldownMetaId").add("QueryHits").build();

    private long tabletId;
    private List<Replica> replicas;

    public ReplicasProcNode(long tabletId, List<Replica> replicas) {
        this.tabletId = tabletId;
        this.replicas = replicas;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        ImmutableMap<Long, Backend> backendMap = Env.getCurrentSystemInfo().getAllBackendsByAllCluster();

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        TabletMeta tabletMeta = Env.getCurrentInvertedIndex().getTabletMeta(tabletId);
        Tablet tablet = null;
        try {
            OlapTable table = (OlapTable) Env.getCurrentInternalCatalog().getDbNullable(tabletMeta.getDbId())
                    .getTableNullable(tabletMeta.getTableId());
            table.readLock();
            try {
                tablet = table.getPartition(tabletMeta.getPartitionId()).getIndex(tabletMeta.getIndexId())
                        .getTablet(tabletId);
            } finally {
                table.readUnlock();
            }
        } catch (Exception e) {
            return result;
        }

        for (Replica replica : replicas) {
            Backend be = backendMap.get(replica.getBackendId());
            String host = (be == null ? Backend.DUMMY_IP : be.getHost());
            int port = (be == null ? 0 : be.getHttpPort());
            String hostPort = NetUtils.getHostPortInAccessibleFormat(host, port);
            String metaUrl = String.format("http://" + hostPort + "/api/meta/header/%d", tabletId);
            String compactionUrl = String.format("http://" + hostPort + "/api/compaction/show?tablet_id=%d", tabletId);

            String path = "";
            if (be != null) {
                DiskInfo diskInfo = be.getDisks().values().stream()
                        .filter(disk -> disk.getPathHash() == replica.getPathHash())
                        .findFirst().orElse(null);
                if (diskInfo != null) {
                    path = diskInfo.getRootPath();
                }
            }

            String cooldownMetaId = "";
            if (replica.getCooldownMetaId() != null) {
                cooldownMetaId = replica.getCooldownMetaId().toString();
            }
            long queryHits = 0L;
            if (Config.enable_query_hit_stats) {
                queryHits = QueryStatsUtil.getMergedReplicaStats(replica.getId());
            }
            result.addRow(Arrays.asList(String.valueOf(replica.getId()),
                                        String.valueOf(replica.getBackendId()),
                                        String.valueOf(replica.getVersion()),
                                        String.valueOf(replica.getLastSuccessVersion()),
                                        String.valueOf(replica.getLastFailedVersion()),
                                        TimeUtils.longToTimeString(replica.getLastFailedTimestamp()),
                                        String.valueOf(replica.getSchemaHash()),
                                        String.valueOf(replica.getDataSize()),
                                        String.valueOf(replica.getRemoteDataSize()),
                                        String.valueOf(replica.getRowCount()),
                                        String.valueOf(replica.getState()),
                                        String.valueOf(replica.isBad()),
                                        String.valueOf(replica.isUserDrop()),
                                        String.valueOf(replica.getVisibleVersionCount()),
                                        String.valueOf(replica.getTotalVersionCount()),
                                        String.valueOf(replica.getPathHash()),
                                        path,
                                        metaUrl,
                                        compactionUrl,
                                        String.valueOf(tablet.getCooldownConf().first),
                                        cooldownMetaId,
                                        String.valueOf(queryHits)));
        }
        return result;
    }
}
