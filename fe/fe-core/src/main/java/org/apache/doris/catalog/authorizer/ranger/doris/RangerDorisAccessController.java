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

package org.apache.doris.catalog.authorizer.ranger.doris;

import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.authorizer.ranger.RangerAccessController;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AuthorizationException;
import org.apache.doris.mysql.privilege.PrivBitSet;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.Privilege;
import org.apache.doris.resource.workloadgroup.WorkloadGroupMgr;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.service.RangerAuthContextListener;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class RangerDorisAccessController extends RangerAccessController {
    private static final Logger LOG = LogManager.getLogger(RangerDorisAccessController.class);
    // ranger must set name, we agreed that this name must be used
    private static final String GLOBAL_PRIV_FIXED_NAME = "*";

    private RangerBasePlugin dorisPlugin;
    // private static ScheduledThreadPoolExecutor logFlushTimer = ThreadPoolManager.newDaemonScheduledThreadPool(1,
    //        "ranger-doris-audit-log-flusher-timer", true);
    // private RangerHiveAuditHandler auditHandler;

    public RangerDorisAccessController(String serviceName) {
        this(serviceName, null);
    }

    public RangerDorisAccessController(String serviceName, RangerAuthContextListener rangerAuthContextListener) {
        dorisPlugin = new RangerDorisPlugin(serviceName, rangerAuthContextListener);
        // auditHandler = new RangerHiveAuditHandler(dorisPlugin.getConfig());
        // start a timed log flusher
        // logFlushTimer.scheduleAtFixedRate(new RangerHiveAuditLogFlusher(auditHandler), 10, 20L, TimeUnit.SECONDS);
    }

    @VisibleForTesting
    public RangerDorisAccessController(RangerBasePlugin plugin) {
        dorisPlugin = plugin;
    }

    private RangerAccessRequestImpl createRequest(UserIdentity currentUser, DorisAccessType accessType) {
        RangerAccessRequestImpl request = createRequest(currentUser);
        request.setAction(accessType.name());
        request.setAccessType(accessType.name());
        return request;
    }

    @Override
    protected RangerAccessRequestImpl createRequest(UserIdentity currentUser) {
        RangerAccessRequestImpl request = new RangerAccessRequestImpl();
        request.setUser(ClusterNamespace.getNameFromFullName(currentUser.getQualifiedUser()));
        Set<String> roles = Env.getCurrentEnv().getAuth().getRolesByUser(currentUser, false);
        request.setUserRoles(roles.stream().map(role -> ClusterNamespace.getNameFromFullName(role)).collect(
                Collectors.toSet()));

        request.setClientIPAddress(currentUser.getHost());
        request.setClusterType(CLIENT_TYPE_DORIS);
        request.setClientType(CLIENT_TYPE_DORIS);
        request.setAccessTime(new Date());

        return request;
    }

    private void checkPrivileges(UserIdentity currentUser, DorisAccessType accessType,
            List<RangerDorisResource> dorisResources) throws AuthorizationException {
        List<RangerAccessRequest> requests = new ArrayList<>();
        for (RangerDorisResource resource : dorisResources) {
            RangerAccessRequestImpl request = createRequest(currentUser, accessType);
            request.setResource(resource);
            requests.add(request);
        }

        Collection<RangerAccessResult> results = dorisPlugin.isAccessAllowed(requests);
        checkRequestResults(results, accessType.name());
    }

    private boolean checkPrivilegeByPlugin(UserIdentity currentUser, DorisAccessType accessType,
            RangerDorisResource resource) {
        RangerAccessRequestImpl request = createRequest(currentUser, accessType);
        request.setResource(resource);
        if (LOG.isDebugEnabled()) {
            LOG.debug("ranger request: {}", request);
        }
        RangerAccessResult result = dorisPlugin.isAccessAllowed(request);
        return checkRequestResult(request, result, accessType.name());
    }

    private boolean checkPrivilege(UserIdentity currentUser, PrivPredicate wanted,
            RangerDorisResource resource, PrivBitSet checkedPrivs) {
        PrivBitSet copy = wanted.getPrivs().copy();
        // avoid duplicate check auth at different levels
        copy.remove(checkedPrivs);
        for (Privilege privilege : copy.toPrivilegeList()) {
            boolean res = checkPrivilegeByPlugin(currentUser, DorisAccessType.toAccessType(privilege), resource);
            if (res) {
                checkedPrivs.set(privilege.getIdx());
            }
            if (Privilege.satisfy(checkedPrivs, wanted)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean checkGlobalPriv(UserIdentity currentUser, PrivPredicate wanted) {
        PrivBitSet checkedPrivs = PrivBitSet.of();
        return checkGlobalPrivInternal(currentUser, wanted, checkedPrivs);
    }

    public boolean checkGlobalPrivInternal(UserIdentity currentUser, PrivPredicate wanted, PrivBitSet checkedPrivs) {
        RangerDorisResource resource = new RangerDorisResource(DorisObjectType.GLOBAL, GLOBAL_PRIV_FIXED_NAME);
        return checkPrivilege(currentUser, wanted, resource, checkedPrivs);
    }

    @Override
    public boolean checkCtlPriv(UserIdentity currentUser, String ctl, PrivPredicate wanted) {
        PrivBitSet checkedPrivs = PrivBitSet.of();
        return checkGlobalPrivInternal(currentUser, wanted, checkedPrivs)
                || checkCtlPrivInternal(currentUser, ctl, wanted, checkedPrivs);
    }

    public boolean checkCtlPrivInternal(UserIdentity currentUser, String ctl, PrivPredicate wanted,
            PrivBitSet checkedPrivs) {
        RangerDorisResource resource = new RangerDorisResource(DorisObjectType.CATALOG, ctl);
        return checkPrivilege(currentUser, wanted, resource, checkedPrivs);
    }

    @Override
    public boolean checkDbPriv(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted) {
        PrivBitSet checkedPrivs = PrivBitSet.of();
        return checkGlobalPrivInternal(currentUser, wanted, checkedPrivs)
                || checkCtlPrivInternal(currentUser, ctl, wanted, checkedPrivs)
                || checkDbPrivInternal(currentUser, ctl, db, wanted, checkedPrivs);
    }

    public boolean checkDbPrivInternal(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted,
            PrivBitSet checkedPrivs) {
        RangerDorisResource resource = new RangerDorisResource(DorisObjectType.DATABASE, ctl,
                ClusterNamespace.getNameFromFullName(db));
        return checkPrivilege(currentUser, wanted, resource, checkedPrivs);
    }

    @Override
    public boolean checkTblPriv(UserIdentity currentUser, String ctl, String db, String tbl, PrivPredicate wanted) {
        PrivBitSet checkedPrivs = PrivBitSet.of();
        return checkGlobalPrivInternal(currentUser, wanted, checkedPrivs)
                || checkCtlPrivInternal(currentUser, ctl, wanted, checkedPrivs)
                || checkDbPrivInternal(currentUser, ctl, db, wanted, checkedPrivs)
                || checkTblPrivInternal(currentUser, ctl, db, tbl, wanted, checkedPrivs);
    }

    public boolean checkTblPrivInternal(UserIdentity currentUser, String ctl, String db, String tbl,
            PrivPredicate wanted, PrivBitSet checkedPrivs) {
        RangerDorisResource resource = new RangerDorisResource(DorisObjectType.TABLE,
                ctl, ClusterNamespace.getNameFromFullName(db), tbl);
        return checkPrivilege(currentUser, wanted, resource, checkedPrivs);
    }

    @Override
    public void checkColsPriv(UserIdentity currentUser, String ctl, String db, String tbl, Set<String> cols,
            PrivPredicate wanted) throws AuthorizationException {
        for (String col : cols) {
            if (!checkColPriv(currentUser, ctl, db, tbl, col, wanted)) {
                throw new AuthorizationException(String.format(
                        "Permission denied: user [%s] does not have privilege for [%s] command on [%s].[%s].[%s].[%s]",
                        currentUser, wanted, ctl, db, tbl, col));
            }
        }
    }

    public boolean checkColPriv(UserIdentity currentUser, String ctl, String db, String tbl,
            String col, PrivPredicate wanted) {
        PrivBitSet checkedPrivs = PrivBitSet.of();
        return checkGlobalPrivInternal(currentUser, wanted, checkedPrivs)
                || checkCtlPrivInternal(currentUser, ctl, wanted, checkedPrivs)
                || checkDbPrivInternal(currentUser, ctl, db, wanted, checkedPrivs)
                || checkTblPrivInternal(currentUser, ctl, db, tbl, wanted, checkedPrivs)
                || checkColPrivInternal(currentUser, ctl, db, tbl, col, wanted, checkedPrivs);
    }

    private boolean checkColPrivInternal(UserIdentity currentUser, String ctl, String db, String tbl, String col,
            PrivPredicate wanted, PrivBitSet checkedPrivs) {
        RangerDorisResource resource = new RangerDorisResource(DorisObjectType.COLUMN,
                ctl, ClusterNamespace.getNameFromFullName(db), tbl, col);
        return checkPrivilege(currentUser, wanted, resource, checkedPrivs);
    }

    @Override
    public boolean checkCloudPriv(UserIdentity currentUser, String resourceName,
            PrivPredicate wanted, ResourceTypeEnum type) {
        return false;
    }

    @Override
    public boolean checkResourcePriv(UserIdentity currentUser, String resourceName, PrivPredicate wanted) {
        PrivBitSet checkedPrivs = PrivBitSet.of();
        return checkGlobalPrivInternal(currentUser, wanted, checkedPrivs)
                || checkResourcePrivInternal(currentUser, resourceName, wanted, checkedPrivs);
    }

    private boolean checkResourcePrivInternal(UserIdentity currentUser, String resourceName, PrivPredicate wanted,
            PrivBitSet checkedPrivs) {
        RangerDorisResource resource = new RangerDorisResource(DorisObjectType.RESOURCE, resourceName);
        return checkPrivilege(currentUser, wanted, resource, checkedPrivs);
    }

    @Override
    public boolean checkWorkloadGroupPriv(UserIdentity currentUser, String workloadGroupName, PrivPredicate wanted) {
        // For compatibility with older versions, it is not needed to check the privileges of the default group.
        if (WorkloadGroupMgr.DEFAULT_GROUP_NAME.equals(workloadGroupName)) {
            return true;
        }
        PrivBitSet checkedPrivs = PrivBitSet.of();
        return checkGlobalPrivInternal(currentUser, wanted, checkedPrivs)
                || checkWorkloadGroupInternal(currentUser, workloadGroupName, wanted, checkedPrivs);
    }

    private boolean checkWorkloadGroupInternal(UserIdentity currentUser, String workloadGroupName, PrivPredicate wanted,
            PrivBitSet checkedPrivs) {
        RangerDorisResource resource = new RangerDorisResource(DorisObjectType.WORKLOAD_GROUP, workloadGroupName);
        return checkPrivilege(currentUser, wanted, resource, checkedPrivs);
    }

    @Override
    protected RangerDorisResource createResource(String ctl, String db, String tbl) {
        return new RangerDorisResource(DorisObjectType.TABLE,
                ctl, ClusterNamespace.getNameFromFullName(db), tbl);
    }

    @Override
    protected RangerDorisResource createResource(String ctl, String db, String tbl, String col) {
        return new RangerDorisResource(DorisObjectType.COLUMN,
                ctl, ClusterNamespace.getNameFromFullName(db), tbl, col);
    }

    @Override
    protected RangerBasePlugin getPlugin() {
        return dorisPlugin;
    }

    @Override
    protected RangerAccessResultProcessor getAccessResultProcessor() {
        return null;
    }

    // For test only
    public static void main(String[] args) {
        RangerDorisAccessController ac = new RangerDorisAccessController("doris");
        UserIdentity user = new UserIdentity("user1", "127.0.0.1");
        user.setIsAnalyzed();
        boolean res = ac.checkDbPriv(user, "internal", "db1", PrivPredicate.SHOW);
        System.out.println("res: " + res);
        user = new UserIdentity("user2", "127.0.0.1");
        user.setIsAnalyzed();
        res = ac.checkTblPriv(user, "internal", "db1", "tbl1", PrivPredicate.SELECT);
        System.out.println("res: " + res);
    }
}
