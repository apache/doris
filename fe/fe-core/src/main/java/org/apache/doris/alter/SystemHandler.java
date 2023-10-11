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

import org.apache.doris.analysis.AddBackendClause;
import org.apache.doris.analysis.AddFollowerClause;
import org.apache.doris.analysis.AddObserverClause;
import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.CancelAlterSystemStmt;
import org.apache.doris.analysis.CancelStmt;
import org.apache.doris.analysis.DecommissionBackendClause;
import org.apache.doris.analysis.DropBackendClause;
import org.apache.doris.analysis.DropFollowerClause;
import org.apache.doris.analysis.DropObserverClause;
import org.apache.doris.analysis.ModifyBackendClause;
import org.apache.doris.analysis.ModifyBackendHostNameClause;
import org.apache.doris.analysis.ModifyBrokerClause;
import org.apache.doris.analysis.ModifyFrontendHostNameClause;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.system.SystemInfoService.HostInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/*
 * SystemHandler is for
 * 1. add/drop/decommission backends
 * 2. add/drop frontends
 * 3. add/drop/modify brokers
 */
public class SystemHandler extends AlterHandler {
    private static final Logger LOG = LogManager.getLogger(SystemHandler.class);

    public SystemHandler() {
        super("cluster");
    }

    @Override
    protected void runAfterCatalogReady() {
        super.runAfterCatalogReady();
        runAlterJobV2();
    }

    // check all decommissioned backends, if there is no available tablet on that backend, drop it.
    private void runAlterJobV2() {
        SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        // check if decommission is finished
        for (Long beId : systemInfoService.getAllBackendIds(false)) {
            Backend backend = systemInfoService.getBackend(beId);
            if (backend == null || !backend.isDecommissioned()) {
                continue;
            }

            List<Long> backendTabletIds = invertedIndex.getTabletIdsByBackendId(beId);
            if (Config.drop_backend_after_decommission && checkTablets(beId, backendTabletIds)) {
                try {
                    systemInfoService.dropBackend(beId);
                    LOG.info("no available tablet on decommission backend {}, drop it", beId);
                } catch (DdlException e) {
                    // does not matter, may be backend not exist
                    LOG.info("backend {} is dropped failed after decommission {}", beId, e.getMessage());
                }
                continue;
            }

            LOG.info("backend {} lefts {} replicas to decommission: {}", beId, backendTabletIds.size(),
                    backendTabletIds.subList(0, Math.min(10, backendTabletIds.size())));
        }
    }

    @Override
    public List<List<Comparable>> getAlterJobInfosByDb(Database db) {
        throw new NotImplementedException("getAlterJobInfosByDb is not supported in SystemHandler");
    }

    @Override
    // add synchronized to avoid process 2 or more stmts at same time
    public synchronized void process(String rawSql, List<AlterClause> alterClauses, String clusterName,
                                     Database dummyDb,
                                     OlapTable dummyTbl) throws UserException {
        Preconditions.checkArgument(alterClauses.size() == 1);
        AlterClause alterClause = alterClauses.get(0);

        if (alterClause instanceof AddBackendClause) {
            // add backend
            AddBackendClause addBackendClause = (AddBackendClause) alterClause;
            Env.getCurrentSystemInfo().addBackends(addBackendClause.getHostInfos(), addBackendClause.getTagMap());
        } else if (alterClause instanceof DropBackendClause) {
            // drop backend
            DropBackendClause dropBackendClause = (DropBackendClause) alterClause;
            if (!dropBackendClause.isForce()) {
                throw new DdlException("It is highly NOT RECOMMENDED to use DROP BACKEND stmt."
                        + "It is not safe to directly drop a backend. "
                        + "All data on this backend will be discarded permanently. "
                        + "If you insist, use DROPP instead of DROP");
            }
            Env.getCurrentSystemInfo().dropBackends(dropBackendClause.getHostInfos());
        } else if (alterClause instanceof DecommissionBackendClause) {
            // decommission
            DecommissionBackendClause decommissionBackendClause = (DecommissionBackendClause) alterClause;
            // check request
            List<Backend> decommissionBackends = checkDecommission(decommissionBackendClause);

            // set backend's state as 'decommissioned'
            // for decommission operation, here is no decommission job. the system handler will check
            // all backend in decommission state
            for (Backend backend : decommissionBackends) {
                backend.setDecommissioned(true);
                Env.getCurrentEnv().getEditLog().logBackendStateChange(backend);
                LOG.info("set backend {} to decommission", backend.getId());
            }

        } else if (alterClause instanceof AddObserverClause) {
            AddObserverClause clause = (AddObserverClause) alterClause;
            Env.getCurrentEnv().addFrontend(FrontendNodeType.OBSERVER, clause.getHost(),
                    clause.getPort());
        } else if (alterClause instanceof DropObserverClause) {
            DropObserverClause clause = (DropObserverClause) alterClause;
            Env.getCurrentEnv().dropFrontend(FrontendNodeType.OBSERVER, clause.getHost(),
                    clause.getPort());
        } else if (alterClause instanceof AddFollowerClause) {
            AddFollowerClause clause = (AddFollowerClause) alterClause;
            Env.getCurrentEnv().addFrontend(FrontendNodeType.FOLLOWER, clause.getHost(),
                    clause.getPort());
        } else if (alterClause instanceof DropFollowerClause) {
            DropFollowerClause clause = (DropFollowerClause) alterClause;
            Env.getCurrentEnv().dropFrontend(FrontendNodeType.FOLLOWER, clause.getHost(),
                    clause.getPort());
        } else if (alterClause instanceof ModifyBrokerClause) {
            ModifyBrokerClause clause = (ModifyBrokerClause) alterClause;
            Env.getCurrentEnv().getBrokerMgr().execute(clause);
        } else if (alterClause instanceof ModifyBackendClause) {
            Env.getCurrentSystemInfo().modifyBackends(((ModifyBackendClause) alterClause));
        } else if (alterClause instanceof ModifyFrontendHostNameClause) {
            ModifyFrontendHostNameClause clause = (ModifyFrontendHostNameClause) alterClause;
            Env.getCurrentEnv().modifyFrontendHostName(clause.getHost(), clause.getPort(), clause.getNewHost());
        } else if (alterClause instanceof ModifyBackendHostNameClause) {
            Env.getCurrentSystemInfo().modifyBackendHost((ModifyBackendHostNameClause) alterClause);
        } else {
            Preconditions.checkState(false, alterClause.getClass());
        }
    }

    /*
     * check if the specified backends can be dropped
     * 1. backend does not have any tablet.
     * 2. all tablets in backend have been recycled.
     */
    private boolean checkTablets(Long beId, List<Long> backendTabletIds) {
        if (backendTabletIds.isEmpty()) {
            return true;
        }
        if (backendTabletIds.size() < Config.decommission_tablet_check_threshold
                && Env.getCurrentRecycleBin().allTabletsInRecycledStatus(backendTabletIds)) {
            LOG.info("tablet size is {}, all tablets on decommissioned backend {} have been recycled,"
                    + " so this backend will be dropped immediately", backendTabletIds.size(), beId);
            return true;
        }
        return false;
    }

    private List<Backend> checkDecommission(DecommissionBackendClause decommissionBackendClause)
            throws DdlException {
        return checkDecommission(decommissionBackendClause.getHostInfos());
    }

    /*
     * check if the specified backends can be decommissioned
     * 1. backend should exist.
     * 2. after decommission, the remaining backend num should meet the replication num.
     * 3. after decommission, The remaining space capacity can store data on decommissioned backends.
     */
    public static List<Backend> checkDecommission(List<HostInfo> hostInfos)
            throws DdlException {
        SystemInfoService infoService = Env.getCurrentSystemInfo();
        List<Backend> decommissionBackends = Lists.newArrayList();
        // check if exist
        for (HostInfo hostInfo : hostInfos) {
            Backend backend = infoService.getBackendWithHeartbeatPort(hostInfo.getHost(),
                    hostInfo.getPort());
            if (backend == null) {
                throw new DdlException("Backend does not exist["
                        + NetUtils.getHostPortInAccessibleFormat(hostInfo.getHost(), hostInfo.getPort()) + "]");
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
        SystemInfoService infoService = Env.getCurrentSystemInfo();
        // check if backends is under decommission
        List<Backend> backends = Lists.newArrayList();
        List<HostInfo> hostInfos = cancelAlterSystemStmt.getHostInfos();
        for (HostInfo hostInfo : hostInfos) {
            // check if exist
            Backend backend = infoService.getBackendWithHeartbeatPort(hostInfo.getHost(),
                    hostInfo.getPort());
            if (backend == null) {
                throw new DdlException("Backend does not exist["
                        + NetUtils.getHostPortInAccessibleFormat(hostInfo.getHost(), hostInfo.getPort()) + "]");
            }

            if (!backend.isDecommissioned()) {
                // it's ok. just log
                LOG.info("backend is not decommissioned[{}]", backend.getId());
                continue;
            }

            backends.add(backend);
        }

        for (Backend backend : backends) {
            if (backend.setDecommissioned(false)) {
                Env.getCurrentEnv().getEditLog().logBackendStateChange(backend);
            } else {
                LOG.info("backend is not decommissioned[{}]", backend.getHost());
            }
        }
    }
}
