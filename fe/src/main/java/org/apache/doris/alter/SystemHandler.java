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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentTask;
import org.apache.doris.thrift.TTabletInfo;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
 * SystemHandler is for
 * 1. add/drop/decommisson backends
 * 2. add/drop frontends
 * 3. add/drop/modify brokers
 */
public class SystemHandler extends AlterHandler {
    private static final Logger LOG = LogManager.getLogger(SystemHandler.class);

    public SystemHandler() {
        super("cluster");
    }

    @Override
    public void handleFinishedReplica(AgentTask task, TTabletInfo finishTabletInfo, long reportVersion)
            throws MetaNotFoundException {
    }

    @Override
    protected void runAfterCatalogReady() {
        super.runAfterCatalogReady();
        runOldAlterJob();
        runAlterJobV2();
    }

    @Deprecated
    private void runOldAlterJob() {
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

    private void runAlterJobV2() {
        Iterator<Map.Entry<Long, AlterJobV2>> iter = alterJobsV2.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Long, AlterJobV2> entry = iter.next();
            AlterJobV2 alterJob = entry.getValue();
            if (alterJob.isDone()) {
                continue;
            }
            alterJob.run();
        }
    }

    @Override
    public List<List<Comparable>> getAlterJobInfosByDb(Database db) {
        throw new NotImplementedException();
    }

    @Override
    // add synchronized to avoid process 2 or more stmts at same time
    public synchronized void process(List<AlterClause> alterClauses, String clusterName, Database dummyDb,
            OlapTable dummyTbl) throws DdlException {
        Preconditions.checkArgument(alterClauses.size() == 1);
        AlterClause alterClause = alterClauses.get(0);

        if (alterClause instanceof AddBackendClause) {
            // add backend
            AddBackendClause addBackendClause = (AddBackendClause) alterClause;
            final String destClusterName = addBackendClause.getDestCluster();
            
            if (!Strings.isNullOrEmpty(destClusterName) 
                    && Catalog.getInstance().getCluster(destClusterName) == null) {
                throw new DdlException("Cluster: " + destClusterName + " does not exist.");
            }
            Catalog.getCurrentSystemInfo().addBackends(addBackendClause.getHostPortPairs(), 
                addBackendClause.isFree(), addBackendClause.getDestCluster());
        } else if (alterClause instanceof DropBackendClause) {
            // drop backend
            DropBackendClause dropBackendClause = (DropBackendClause) alterClause;
            if (!dropBackendClause.isForce()) {
                throw new DdlException("It is highly NOT RECOMMENDED to use DROP BACKEND stmt."
                        + "It is not safe to directly drop a backend. "
                        + "All data on this backend will be discarded permanently. "
                        + "If you insist, use DROPP BACKEND stmt (double P).");
            }
            Catalog.getCurrentSystemInfo().dropBackends(dropBackendClause.getHostPortPairs());
        } else if (alterClause instanceof DecommissionBackendClause) {
            // decommission
            DecommissionBackendClause decommissionBackendClause = (DecommissionBackendClause) alterClause;
            // check request
            List<Backend> decommissionBackends = checkDecommission(decommissionBackendClause);

            // set backend's state as 'decommissioned'
            for (Backend backend : decommissionBackends) {
                backend.setDecommissioned(true);
                Catalog.getCurrentCatalog().getEditLog().logBackendStateChange(backend);
            }

            // add job
            long jobId = Catalog.getInstance().getNextId();
            List<Long> backendIds = decommissionBackends.stream().map(b -> b.getId()).collect(Collectors.toList());
            DecommissionBackendJobV2 decommissionBackendJob = new DecommissionBackendJobV2(jobId, backendIds);
            addAlterJobV2(decommissionBackendJob);

            // log
            Catalog.getInstance().getEditLog().logAlterJob(decommissionBackendJob);
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

    private List<Backend> checkDecommission(DecommissionBackendClause decommissionBackendClause)
            throws DdlException {
        return checkDecommission(decommissionBackendClause.getHostPortPairs());
    }

    /*
     * check if the specified backends can be decommissioned
     * 1. backend should exist.
     * 2. after decommission, the remaining backend num should meet the replication num.
     * 3. after decommission, The remaining space capacity can store data on decommissioned backends.
     */
    public static List<Backend> checkDecommission(List<Pair<String, Integer>> hostPortPairs)
            throws DdlException {
        SystemInfoService infoService = Catalog.getCurrentSystemInfo();
        List<Backend> decommissionBackends = Lists.newArrayList();
        // check if exist
        for (Pair<String, Integer> pair : hostPortPairs) {
            Backend backend = infoService.getBackendWithHeartbeatPort(pair.first, pair.second);
            if (backend == null) {
                throw new DdlException("Backend does not exist[" + pair.first + ":" + pair.second + "]");
            }
            if (backend.isDecommissioned()) {
                // already under decommission, ignore it
                continue;
            }
            decommissionBackends.add(backend);
        }

        // TODO(cmy): check if replication num can be met
        // TODO(cmy): check remaining space

        return decommissionBackends;
    }

    @Override
    public synchronized void cancel(CancelStmt stmt) throws DdlException {
        CancelAlterSystemStmt cancelAlterSystemStmt = (CancelAlterSystemStmt) stmt;
        cancelAlterSystemStmt.getHostPortPairs();

        SystemInfoService infoService = Catalog.getCurrentSystemInfo();
        // check if backends is under decommission
        List<Backend> backends = Lists.newArrayList();
        List<Pair<String, Integer>> hostPortPairs = cancelAlterSystemStmt.getHostPortPairs();
        for (Pair<String, Integer> pair : hostPortPairs) {
            // check if exist
            Backend backend = infoService.getBackendWithHeartbeatPort(pair.first, pair.second);
            if (backend == null) {
                throw new DdlException("Backend does not exists[" + pair.first + "]");
            }

            if (!backend.isDecommissioned()) {
                // it's ok. just log
                LOG.info("backend is not decommissioned[{}]", pair.first);
                continue;
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
