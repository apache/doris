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

package org.apache.doris.alter;

import org.apache.doris.alter.AlterJob.JobState;
import org.apache.doris.alter.DecommissionBackendJob.DecommissionType;
import org.apache.doris.analysis.AddBackendClause;
import org.apache.doris.analysis.AddFollowerClause;
import org.apache.doris.analysis.AddObserverClause;
import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.AlterLoadErrorUrlClause;
import org.apache.doris.analysis.CancelAlterSystemStmt;
import org.apache.doris.analysis.CancelStmt;
import org.apache.doris.analysis.DecommissionBackendClause;
import org.apache.doris.analysis.DropBackendClause;
import org.apache.doris.analysis.DropFollowerClause;
import org.apache.doris.analysis.DropObserverClause;
import org.apache.doris.analysis.ModifyBrokerClause;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Backend.BackendState;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentTask;
import org.apache.doris.thrift.TTabletInfo;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class SystemHandler extends AlterHandler {
    private static final Logger LOG = LogManager.getLogger(SystemHandler.class);

    public SystemHandler() {
        super("cluster");
    }

    @Override
    public void handleFinishedReplica(AgentTask task, TTabletInfo finishTabletInfo, long reportVersion)
            throws MetaNotFoundException {
        AlterJob alterJob = getAlterJob(-1L);
        if (alterJob == null) {
            throw new MetaNotFoundException("Cannot find " + task.getTaskType().name() + " job");
        }
        alterJob.handleFinishedReplica(task, finishTabletInfo, reportVersion);
    }

    @Override
    protected void runAfterCatalogReady() {
        super.runAfterCatalogReady();

        List<AlterJob> cancelledJobs = Lists.newArrayList();
        List<AlterJob> finishedJobs = Lists.newArrayList();

        for (AlterJob alterJob : alterJobs.values()) {
            AlterJob decommissionBackendJob = (DecommissionBackendJob) alterJob;
            JobState state = decommissionBackendJob.getState();
            switch (state) {
                case PENDING: {
                    // send tasks
                    decommissionBackendJob.sendTasks();
                    break;
                }
                case RUNNING: {
                    // no timeout
                    // try finish job
                    decommissionBackendJob.tryFinishJob();
                    break;
                }
                case FINISHED: {
                    // remove from alterJobs
                    finishedJobs.add(decommissionBackendJob);
                    break;
                }
                case CANCELLED: {
                    Preconditions.checkState(false);
                    break;
                }
                default:
                    Preconditions.checkState(false);
                    break;
            }
        } // end for jobs

        // handle cancelled jobs
        for (AlterJob dropBackendJob : cancelledJobs) {
            dropBackendJob.cancel(null, "cancelled");
            jobDone(dropBackendJob);
        }

        // handle finished jobs
        for (AlterJob dropBackendJob : finishedJobs) {
            jobDone(dropBackendJob);
        }
    }

    @Override
    public List<List<Comparable>> getAlterJobInfosByDb(Database db) {
        throw new NotImplementedException();
    }

    @Override
    // add synchronized to avoid process 2 or more stmt at same time
    public synchronized void process(List<AlterClause> alterClauses, String clusterName, Database dummyDb,
            OlapTable dummyTbl) throws DdlException {
        Preconditions.checkArgument(alterClauses.size() == 1);
        AlterClause alterClause = alterClauses.get(0);

        if (alterClause instanceof AddBackendClause) {
            AddBackendClause addBackendClause = (AddBackendClause) alterClause;
            final String destClusterName = addBackendClause.getDestCluster();
            
            if (!Strings.isNullOrEmpty(destClusterName) 
                    && Catalog.getInstance().getCluster(destClusterName) == null) {
                throw new DdlException("Cluster: " + destClusterName + " does not exist.");
            }
            Catalog.getCurrentSystemInfo().addBackends(addBackendClause.getHostPortPairs(), 
                addBackendClause.isFree(), addBackendClause.getDestCluster());
        } else if (alterClause instanceof DropBackendClause) {
            DropBackendClause dropBackendClause = (DropBackendClause) alterClause;
            if (!dropBackendClause.isForce()) {
                throw new DdlException("It is highly NOT RECOMMENDED to use DROP BACKEND stmt."
                        + "It is not safe to directly drop a backend. "
                        + "All data on this backend will be discarded permanently. "
                        + "If you insist, use DROPP BACKEND stmt (double P).");
            }
            Catalog.getCurrentSystemInfo().dropBackends(dropBackendClause.getHostPortPairs());
        } else if (alterClause instanceof DecommissionBackendClause) {
            DecommissionBackendClause decommissionBackendClause = (DecommissionBackendClause) alterClause;

            // check request
            // clusterName -> (beId -> backend)
            Map<String, Map<Long, Backend>> clusterBackendsMap = checkDecommission(decommissionBackendClause);

            // set backend's state as 'decommissioned'
            for (Map<Long, Backend> backends : clusterBackendsMap.values()) {
                for (Backend backend : backends.values()) {
                    if (((DecommissionBackendClause) alterClause).getType() == DecommissionType.ClusterDecommission) {
                        backend.setBackendState(BackendState.offline);
                    }
                    backend.setDecommissioned(true);
                    backend.setDecommissionType(((DecommissionBackendClause) alterClause).getType());
                    Catalog.getInstance().getEditLog().logBackendStateChange(backend);
                }
            }

            // add job
            long jobId = Catalog.getInstance().getNextId();
            DecommissionBackendJob decommissionBackendJob = new DecommissionBackendJob(jobId, clusterBackendsMap);
            decommissionBackendJob.setDecommissionType(decommissionBackendClause.getType());
            addAlterJob(decommissionBackendJob);

            // log
            Catalog.getInstance().getEditLog().logStartDecommissionBackend(decommissionBackendJob);
            LOG.info("decommission backend job[{}] created. {}", jobId, decommissionBackendClause.toSql());
        } else if (alterClause instanceof AddObserverClause) {
            AddObserverClause clause = (AddObserverClause) alterClause;
            Catalog.getInstance().addFrontend(FrontendNodeType.OBSERVER, clause.getHost(), clause.getPort());
        } else if (alterClause instanceof DropObserverClause) {
            DropObserverClause clause = (DropObserverClause) alterClause;
            Catalog.getInstance().dropFrontend(FrontendNodeType.OBSERVER, clause.getHost(), clause.getPort());
        } else if (alterClause instanceof AddFollowerClause) {
            AddFollowerClause clause = (AddFollowerClause) alterClause;
            Catalog.getInstance().addFrontend(FrontendNodeType.FOLLOWER, clause.getHost(), clause.getPort());
        } else if (alterClause instanceof DropFollowerClause) {
            DropFollowerClause clause = (DropFollowerClause) alterClause;
            Catalog.getInstance().dropFrontend(FrontendNodeType.FOLLOWER, clause.getHost(), clause.getPort());
        } else if (alterClause instanceof ModifyBrokerClause) {
            ModifyBrokerClause clause = (ModifyBrokerClause) alterClause;
            Catalog.getInstance().getBrokerMgr().execute(clause);
        } else if (alterClause instanceof AlterLoadErrorUrlClause) {
            AlterLoadErrorUrlClause clause = (AlterLoadErrorUrlClause) alterClause;
            Catalog.getInstance().getLoadInstance().setLoadErrorHubInfo(clause.getProperties());
        } else {
            Preconditions.checkState(false, alterClause.getClass());
        }
    }

    private Map<String, Map<Long, Backend>> checkDecommission(DecommissionBackendClause decommissionBackendClause)
            throws DdlException {
        return checkDecommission(decommissionBackendClause.getHostPortPairs());
    }

    public static Map<String, Map<Long, Backend>> checkDecommission(List<Pair<String, Integer>> hostPortPairs)
            throws DdlException {
        // check
        Catalog.getCurrentSystemInfo().checkBackendsExist(hostPortPairs);

        // in Multi-Tenancy , we will check decommission in every cluster
        // check if backend is under decommissioned

        // clusterName -> (beId -> backend)
        final Map<String, Map<Long, Backend>> decommClusterBackendsMap = Maps.newHashMap();
        for (Pair<String, Integer> pair : hostPortPairs) {
            Backend backend = Catalog.getCurrentSystemInfo().getBackendWithHeartbeatPort(pair.first, pair.second);

            if (backend.isDecommissioned()) {
                // it's ok to resend decommission command. just log
                LOG.info("backend[{}] is already under decommissioned.", backend.getHost());
            }

            Map<Long, Backend> backends = decommClusterBackendsMap.get(backend.getOwnerClusterName());
            if (backends == null) {
                backends = Maps.newHashMap();
                decommClusterBackendsMap.put(backend.getOwnerClusterName(), backends);
            }
            backends.put(backend.getId(), backend);
        }

        for (String clusterName : decommClusterBackendsMap.keySet()) {
            // check available capacity for decommission.
            // we need to make sure that there is enough space in this cluster
            // to store the data from decommissioned backends.
            long totalAvailableCapacityB = 0L;
            long totalNeededCapacityB = 0L; // decommission + dead
            int availableBackendNum = 0;
            final Map<Long, Backend> decommBackendsInCluster = decommClusterBackendsMap.get(clusterName);

            // get all backends in this cluster
            final Map<Long, Backend> idToBackendsInCluster =
                    Catalog.getCurrentSystemInfo().getBackendsInCluster(clusterName);
            for (Entry<Long, Backend> entry : idToBackendsInCluster.entrySet()) {
                long backendId = entry.getKey();
                Backend backend = entry.getValue();
                if (decommBackendsInCluster.containsKey(backendId)
                        || !backend.isAvailable()) {
                    totalNeededCapacityB += backend.getDataUsedCapacityB();
                } else {
                    ++availableBackendNum;
                    totalAvailableCapacityB += backend.getAvailableCapacityB();
                }
            }

            // if the space we needed is larger than the current available capacity * 0.85,
            // we refuse this decommission operation.
            if (totalNeededCapacityB > totalAvailableCapacityB * (Config.storage_high_watermark_usage_percent / 100.0)) {
                throw new DdlException("No available capacity for decommission in cluster: " + clusterName
                        + ", needed: " + totalNeededCapacityB + ", available: " + totalAvailableCapacityB
                        + ", threshold: " + Config.storage_high_watermark_usage_percent);
            }

            // backend num not enough
            if (availableBackendNum == 0) {
                throw new DdlException("No available backend");
            }

            // check if meet replication number requirement
            List<String> dbNames;
            try {
                dbNames = Catalog.getInstance().getClusterDbNames(clusterName);
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }

            for (String dbName : dbNames) {
                Database db = Catalog.getInstance().getDb(dbName);
                if (db == null) {
                    continue;
                }
                db.readLock();
                try {
                    for (Table table : db.getTables()) {
                        if (table.getType() != TableType.OLAP) {
                            continue;
                        }
                        OlapTable olapTable = (OlapTable) table;
                        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                        for (Partition partition : olapTable.getPartitions()) {
                            short replicationNum = partitionInfo.getReplicationNum(partition.getId());
                            if (availableBackendNum < replicationNum) {
                                throw new DdlException("Table[" + table.getName() + "] in database[" + dbName
                                        + "] need more than " + replicationNum
                                        + " available backends to meet replication num requirement");
                            }
                        }

                    }
                } finally {
                    db.readUnlock();
                }
            }
        }

        return decommClusterBackendsMap;
    }

    @Override
    public synchronized void cancel(CancelStmt stmt) throws DdlException {
        CancelAlterSystemStmt cancelAlterClusterStmt = (CancelAlterSystemStmt) stmt;
        cancelAlterClusterStmt.getHostPortPairs();

        SystemInfoService clusterInfo = Catalog.getCurrentSystemInfo();
        // check if backends is under decommission
        List<Backend> backends = Lists.newArrayList();
        List<Pair<String, Integer>> hostPortPairs = cancelAlterClusterStmt.getHostPortPairs();
        for (Pair<String, Integer> pair : hostPortPairs) {
            // check if exist
            Backend backend = clusterInfo.getBackendWithHeartbeatPort(pair.first, pair.second);
            if (backend == null) {
                throw new DdlException("Backend does not exists[" + pair.first + "]");
            }

            if (!backend.isDecommissioned()) {
                // it's ok. just log
                LOG.info("backend is not decommissioned[{}]", pair.first);
            }

            backends.add(backend);
        }

        for (Backend backend : backends) {
            if (backend.setDecommissioned(false)) {
                Catalog.getInstance().getEditLog().logBackendStateChange(backend);
            } else {
                LOG.info("backend is not decommissioned[{}]", backend.getHost());
            }
        }
    }

    @Override
    public void replayInitJob(AlterJob alterJob, Catalog catalog) {
        DecommissionBackendJob decommissionBackendJob = (DecommissionBackendJob) alterJob;
        LOG.debug("replay init decommision backend job: {}", decommissionBackendJob.getBackendIdsString());
        addAlterJob(alterJob);
    }

    @Override
    public void replayFinish(AlterJob alterJob, Catalog catalog) {
        LOG.debug("replay finish decommision backend job: {}",
                ((DecommissionBackendJob) alterJob).getBackendIdsString());
        removeAlterJob(alterJob.getTableId());
        alterJob.setState(JobState.FINISHED);
        addFinishedOrCancelledAlterJob(alterJob);
    }

    @Override
    public void replayCancel(AlterJob alterJob, Catalog catalog) {
        throw new NotImplementedException();
    }
}
