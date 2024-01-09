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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.statistics.query.QueryStatsUtil;
import org.apache.doris.system.Backend;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * SHOW PROC /dbs/dbId/tableId/partitions/partitionId/indexId
 * show tablets' detail info within an index
 */
public class TabletsProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TabletId").add("ReplicaId").add("BackendId").add("SchemaHash").add("Version")
            .add("LstSuccessVersion").add("LstFailedVersion").add("LstFailedTime")
            .add("LocalDataSize").add("RemoteDataSize").add("RowCount").add("State")
            .add("LstConsistencyCheckTime").add("CheckVersion")
            .add("VersionCount").add("QueryHits").add("PathHash").add("MetaUrl").add("CompactionStatus")
            .add("CooldownReplicaId").add("CooldownMetaId").build();

    private Table table;
    private MaterializedIndex index;

    public TabletsProcDir(Table table, MaterializedIndex index) {
        this.table = table;
        this.index = index;
    }

    public List<List<Comparable>> fetchComparableResult(long version, long backendId, Replica.ReplicaState state) {
        Preconditions.checkNotNull(table);
        Preconditions.checkNotNull(index);
        ImmutableMap<Long, Backend> backendMap = Env.getCurrentSystemInfo().getIdToBackend();

        List<List<Comparable>> tabletInfos = new ArrayList<List<Comparable>>();
        table.readLock();
        try {
            Map<Long, Long> replicaIdToQueryHits = new HashMap<>();
            if (Config.enable_query_hit_stats) {
                List<Long> replicaIds = new ArrayList<Long>();
                for (Tablet tablet : index.getTablets()) {
                    for (Replica replica : tablet.getReplicas()) {
                        replicaIds.add(replica.getId());
                    }
                }
                replicaIdToQueryHits = QueryStatsUtil.getMergedReplicasStats(replicaIds);
            }
            // get infos
            for (Tablet tablet : index.getTablets()) {
                long tabletId = tablet.getId();
                if (tablet.getReplicas().size() == 0) {
                    List<Comparable> tabletInfo = new ArrayList<Comparable>();
                    tabletInfo.add(tabletId);
                    tabletInfo.add(-1); // replica id
                    tabletInfo.add(-1); // backend id
                    tabletInfo.add(-1); // schema hash
                    tabletInfo.add(FeConstants.null_string); // host name
                    tabletInfo.add(-1); // version
                    tabletInfo.add(-1); // version hash
                    tabletInfo.add(-1); // lst success version
                    tabletInfo.add(-1); // lst success version hash
                    tabletInfo.add(-1); // lst failed version
                    tabletInfo.add(-1); // lst failed version hash
                    tabletInfo.add(-1); // lst failed time
                    tabletInfo.add(-1); // data size
                    tabletInfo.add(-1); // remote data size
                    tabletInfo.add(-1); // row count
                    tabletInfo.add(FeConstants.null_string); // state
                    tabletInfo.add(-1); // lst consistency check time
                    tabletInfo.add(-1); // check version
                    tabletInfo.add(-1); // check version hash
                    tabletInfo.add(-1); // version count
                    tabletInfo.add(0L); // query hits
                    tabletInfo.add(-1); // path hash
                    tabletInfo.add(FeConstants.null_string); // meta url
                    tabletInfo.add(FeConstants.null_string); // compaction status
                    tabletInfo.add(-1); // cooldown replica id
                    tabletInfo.add(""); // cooldown meta id

                    tabletInfos.add(tabletInfo);
                } else {
                    for (Replica replica : tablet.getReplicas()) {
                        if ((version > -1 && replica.getVersion() != version)
                                || (backendId > -1 && replica.getBackendId() != backendId)
                                || (state != null && replica.getState() != state)) {
                            continue;
                        }
                        List<Comparable> tabletInfo = new ArrayList<Comparable>();
                        // tabletId -- replicaId -- backendId -- version -- dataSize -- rowCount -- state
                        tabletInfo.add(tabletId);
                        tabletInfo.add(replica.getId());
                        tabletInfo.add(replica.getBackendId());
                        tabletInfo.add(replica.getSchemaHash());
                        tabletInfo.add(replica.getVersion());
                        tabletInfo.add(replica.getLastSuccessVersion());
                        tabletInfo.add(replica.getLastFailedVersion());
                        tabletInfo.add(TimeUtils.longToTimeString(replica.getLastFailedTimestamp()));
                        tabletInfo.add(replica.getDataSize());
                        tabletInfo.add(replica.getRemoteDataSize());
                        tabletInfo.add(replica.getRowCount());
                        tabletInfo.add(replica.getState());

                        tabletInfo.add(TimeUtils.longToTimeString(tablet.getLastCheckTime()));
                        tabletInfo.add(tablet.getCheckedVersion());
                        tabletInfo.add(replica.getVersionCount());
                        tabletInfo.add(replicaIdToQueryHits.getOrDefault(replica.getId(), 0L));
                        tabletInfo.add(replica.getPathHash());
                        Backend be = backendMap.get(replica.getBackendId());
                        String host = (be == null ? Backend.DUMMY_IP : be.getHost());
                        int port = (be == null ? 0 : be.getHttpPort());
                        String hostPort = NetUtils.getHostPortInAccessibleFormat(host, port);
                        String metaUrl = String.format("http://" + hostPort + "/api/meta/header/%d", tabletId);
                        tabletInfo.add(metaUrl);
                        String compactionUrl = String.format(
                                "http://" + hostPort + "/api/compaction/show?tablet_id=%d", tabletId);
                        tabletInfo.add(compactionUrl);
                        tabletInfo.add(tablet.getCooldownConf().first);
                        if (replica.getCooldownMetaId() == null) {
                            tabletInfo.add("");
                        } else {
                            tabletInfo.add(replica.getCooldownMetaId().toString());
                        }
                        tabletInfos.add(tabletInfo);
                    }
                }
            }
        } finally {
            table.readUnlock();
        }
        return tabletInfos;
    }

    private List<List<Comparable>> fetchComparableResult() {
        return fetchComparableResult(-1, -1, null);
    }

    @Override
    public ProcResult fetchResult() {
        List<List<Comparable>> tabletInfos = fetchComparableResult();
        // sort by tabletId, replicaId
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0, 1);
        Collections.sort(tabletInfos, comparator);

        // set result
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        for (int i = 0; i < tabletInfos.size(); i++) {
            List<Comparable> info = tabletInfos.get(i);
            List<String> row = new ArrayList<String>(info.size());
            for (int j = 0; j < info.size(); j++) {
                row.add(info.get(j).toString());
            }
            result.addRow(row);
        }
        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String tabletIdStr) throws AnalysisException {
        Preconditions.checkNotNull(table);
        Preconditions.checkNotNull(index);

        long tabletId = -1L;
        try {
            tabletId = Long.valueOf(tabletIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid tablet id format: " + tabletIdStr);
        }

        Tablet tablet = index.getTablet(tabletId);
        if (tablet == null) {
            throw new AnalysisException("Tablet[" + tabletId + "] does not exist");
        }

        List<Replica> replicas = tablet.getReplicas();
        return new ReplicasProcNode(tabletId, replicas);
    }

    public static int analyzeColumn(String columnName) throws AnalysisException {
        for (String title : TITLE_NAMES) {
            if (title.equalsIgnoreCase(columnName)) {
                return TITLE_NAMES.indexOf(title);
            }
        }

        throw new AnalysisException("Title name[" + columnName + "] does not exist");
    }
}
