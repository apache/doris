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

package com.baidu.palo.alter;

import com.baidu.palo.alter.AlterJob.JobState;
import com.baidu.palo.alter.DecommissionBackendJob.DecomissionType;
import com.baidu.palo.analysis.AddBackendClause;
import com.baidu.palo.analysis.AddObserverClause;
import com.baidu.palo.analysis.AddFollowerClause;
import com.baidu.palo.analysis.AlterClause;
import com.baidu.palo.analysis.AlterLoadErrorUrlClause;
import com.baidu.palo.analysis.CancelAlterSystemStmt;
import com.baidu.palo.analysis.CancelStmt;
import com.baidu.palo.analysis.DecommissionBackendClause;
import com.baidu.palo.analysis.DropBackendClause;
import com.baidu.palo.analysis.DropObserverClause;
import com.baidu.palo.analysis.DropFollowerClause;
import com.baidu.palo.analysis.ModifyBrokerClause;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.catalog.Partition;
import com.baidu.palo.catalog.PartitionInfo;
import com.baidu.palo.catalog.Table;
import com.baidu.palo.catalog.Table.TableType;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.MetaNotFoundException;
import com.baidu.palo.common.Pair;
import com.baidu.palo.ha.FrontendNodeType;
import com.baidu.palo.system.Backend;
import com.baidu.palo.system.Backend.BackendState;
import com.baidu.palo.system.SystemInfoService;
import com.baidu.palo.task.AgentTask;
import com.baidu.palo.thrift.TTabletInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.LinkedList;
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
    protected void runOneCycle() {
        super.runOneCycle();

        List<AlterJob> cancelledJobs = new LinkedList<AlterJob>();
        this.jobsLock.writeLock().lock();
        try {
            Iterator<Entry<Long, AlterJob>> iterator = this.alterJobs.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<Long, AlterJob> entry = iterator.next();
                AlterJob decommissionBackendJob = entry.getValue();

                JobState state = decommissionBackendJob.getState();
                switch (state) {
                    case PENDING: {
                        // send tasks
                        decommissionBackendJob.sendTasks();
                        break;
                    }
                    case RUNNING: {
                        // no timeout
    
                        // send tasks
                        decommissionBackendJob.sendTasks();
    
                        // try finish job
                        decommissionBackendJob.tryFinishJob();
    
                        break;
                    }
                    case FINISHED: {
                        // remove from alterJobs
                        iterator.remove();
                        addFinishedOrCancelledAlterJob(decommissionBackendJob);
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
        } finally {
            this.jobsLock.writeLock().unlock();
        }

        // handle cancelled jobs
        for (AlterJob dropBackendJob : cancelledJobs) {
            cancelInternal(dropBackendJob, null, null);
        }
    }

    @Override
    public List<List<Comparable>> getAlterJobInfosByDb(Database db) {
        throw new NotImplementedException();
    }

    @Override
    // add synchronized to avoid process 2 or more stmt at same time
    public synchronized void process(List<AlterClause> alterClauses, String clusterName, Database unuseDb,
            OlapTable unuseTbl) throws DdlException {
        Preconditions.checkArgument(alterClauses.size() == 1);
        AlterClause alterClause = alterClauses.get(0);

        if (alterClause instanceof AddBackendClause) {
            AddBackendClause addBackendClause = (AddBackendClause) alterClause;
            Catalog.getCurrentSystemInfo().addBackends(addBackendClause.getHostPortPairs());
        } else if (alterClause instanceof DropBackendClause) {
            DropBackendClause dropBackendClause = (DropBackendClause) alterClause;
            Catalog.getCurrentSystemInfo().dropBackends(dropBackendClause.getHostPortPairs());
        } else if (alterClause instanceof DecommissionBackendClause) {
            DecommissionBackendClause decommissionBackendClause = (DecommissionBackendClause) alterClause;

            // check request
            Map<String, Map<Long, Backend>> clusterBackendsMap = checkDecommission(decommissionBackendClause);

            // set backend's state as 'decommissioned'
            for (Map<Long, Backend> backends : clusterBackendsMap.values()) {
                for (Backend backend : backends.values()) {
                    if (((DecommissionBackendClause) alterClause).getType() == DecomissionType.ClusterDecomission) {
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
            decommissionBackendJob.setDecomissionType(decommissionBackendClause.getType());
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
            Catalog.getInstance().getLoadInstance().changeLoadErrorHubInfo(clause.getParam());
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
        // Map<Long, Backend> backends = Maps.newHashMap();
        final Map<String, Map<Long, Backend>> clusterBackendsMap = Maps.newHashMap();
        for (Pair<String, Integer> pair : hostPortPairs) {
            Backend backend = Catalog.getCurrentSystemInfo().getBackendWithHeartbeatPort(pair.first, pair.second);

            if (backend.isDecommissioned()) {
                // it's ok to resend decommission command. just log
                LOG.info("backend[{}] is already under decommissioned.", backend.getHost());
            }
            // backends.put(backend.getId(), backend);
            Map<Long, Backend> backends = null;
            if (!clusterBackendsMap.containsKey(backend.getOwnerClusterName())) {
                backends = Maps.newHashMap();
                clusterBackendsMap.put(backend.getOwnerClusterName(), backends);
            } else {
                backends = clusterBackendsMap.get(backend.getOwnerClusterName());
            }
            backends.put(backend.getId(), backend);
        }

        for (String cluster : clusterBackendsMap.keySet()) {
            // check available capacity for decommission
            long totalAvailableCapacityB = 0L;
            long totalUnavailableCapacityB = 0L;
            int availableBackendNum = 0;
            final Map<Long, Backend> backendsInCluster = clusterBackendsMap.get(cluster);
            final Map<Long, Backend> idToBackend = Catalog.getCurrentSystemInfo().getIdToBackend();
            for (Entry<Long, Backend> entry : idToBackend.entrySet()) {
                long backendId = entry.getKey();
                Backend backend = entry.getValue();
                if (backendsInCluster.containsKey(backendId) || backend.isDecommissioned() || !backend.isAlive()) {
                    totalUnavailableCapacityB += backend.getTotalCapacityB() - backend.getAvailableCapacityB();
                } else {
                    ++availableBackendNum;
                    totalAvailableCapacityB += backend.getAvailableCapacityB();
                }
            }
            if (totalAvailableCapacityB < totalUnavailableCapacityB) {
                throw new DdlException("No available capacity for decommission");
            }

            // check if meet replication number requirment
            if (availableBackendNum == 0) {
                throw new DdlException("Too little available backend number");
            }
            List<String> dbNames = Catalog.getInstance().getDbNames();
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
                                        + " available backends to meet replication requirement");
                            }
                        }

                    }
                } finally {
                    db.readUnlock();
                }
            }
        }

        return clusterBackendsMap;
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
