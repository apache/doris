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

package org.apache.doris.catalog.authorizer;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AuthorizationException;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.mysql.privilege.CatalogAccessController;
import org.apache.doris.mysql.privilege.PrivPredicate;

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RangerHiveAccessController implements CatalogAccessController {
    public static final String CLIENT_TYPE_DORIS = "doris";
    private static final Logger LOG = LogManager.getLogger(RangerHiveAccessController.class);
    private static ScheduledThreadPoolExecutor logFlushTimer = ThreadPoolManager.newDaemonScheduledThreadPool(1,
            "ranger-hive-audit-log-flusher-timer", true);
    private RangerHivePlugin hivePlugin;
    private RangerHiveAuditHandler auditHandler;

    public RangerHiveAccessController(Map<String, String> properties) {
        String serviceName = properties.get("ranger.service.name");
        hivePlugin = new RangerHivePlugin(serviceName);
        auditHandler = new RangerHiveAuditHandler(hivePlugin.getConfig());
        //start a timed log flusher
        logFlushTimer.scheduleAtFixedRate(new RangerHiveAuditLogFlusher(auditHandler), 10, 20L, TimeUnit.SECONDS);
    }

    private RangerAccessRequestImpl createRequest(UserIdentity currentUser, HiveAccessType accessType) {
        RangerAccessRequestImpl request = new RangerAccessRequestImpl();
        String user = currentUser.getQualifiedUser();
        request.setUser(ClusterNamespace.getNameFromFullName(user));
        Set<String> roles = Env.getCurrentEnv().getAuth().getRolesByUser(currentUser, false);
        request.setUserRoles(roles.stream().map(role -> ClusterNamespace.getNameFromFullName(role)).collect(
                Collectors.toSet()));
        request.setAction(accessType.name());
        if (accessType == HiveAccessType.USE) {
            request.setAccessType(RangerPolicyEngine.ANY_ACCESS);
        } else {
            request.setAccessType(accessType.name().toLowerCase());
        }
        request.setClientIPAddress(currentUser.getHost());
        request.setClusterType(CLIENT_TYPE_DORIS);
        request.setClientType(CLIENT_TYPE_DORIS);
        request.setAccessTime(new Date());

        return request;
    }

    private void checkPrivileges(UserIdentity currentUser, HiveAccessType accessType,
            List<RangerHiveResource> hiveResources) throws AuthorizationException {
        List<RangerAccessRequest> requests = new ArrayList<>();
        for (RangerHiveResource resource : hiveResources) {
            RangerAccessRequestImpl request = createRequest(currentUser, accessType);
            request.setResource(resource);

            requests.add(request);
        }

        Collection<RangerAccessResult> results = hivePlugin.isAccessAllowed(requests, auditHandler);
        for (RangerAccessResult result : results) {
            LOG.debug(String.format("request %s match policy %s", result.getAccessRequest(), result.getPolicyId()));
            if (!result.getIsAllowed()) {
                LOG.debug(result.getReason());
                throw new AuthorizationException(String.format(
                        "Permission denied: user [%s] does not have privilege for [%s] command on [%s]",
                        result.getAccessRequest().getUser(), accessType.name(),
                        result.getAccessRequest().getResource().getAsString().replaceAll("/", ".")));
            }
        }
    }

    private boolean checkPrivilege(UserIdentity currentUser, HiveAccessType accessType,
            RangerHiveResource resource) {
        RangerAccessRequestImpl request = createRequest(currentUser, accessType);
        request.setResource(resource);

        RangerAccessResult result = hivePlugin.isAccessAllowed(request, auditHandler);

        if (result == null) {
            LOG.warn(String.format("Error getting authorizer result, please check your ranger config. Make sure "
                    + "ranger policy engine is initialized. Request: %s", request));
            return false;
        }

        if (result.getIsAllowed()) {
            LOG.debug(String.format("request %s match policy %s", request, result.getPolicyId()));
            return true;
        } else {
            LOG.debug(String.format(
                    "Permission denied: user [%s] does not have privilege for [%s] command on [%s]",
                    result.getAccessRequest().getUser(), accessType.name(),
                    result.getAccessRequest().getResource().getAsString()));
            return false;
        }
    }

    public String getFilterExpr(UserIdentity currentUser, HiveAccessType accessType,
            RangerHiveResource resource) throws HiveAccessControlException {
        RangerAccessRequestImpl request = createRequest(currentUser, accessType);
        request.setResource(resource);
        RangerAccessResult result = hivePlugin.isAccessAllowed(request, auditHandler);

        return result.getFilterExpr();
    }

    public void getColumnMask(UserIdentity currentUser, HiveAccessType accessType,
            RangerHiveResource resource) {
        RangerAccessRequestImpl request = createRequest(currentUser, accessType);
        request.setResource(resource);
        RangerAccessResult result = hivePlugin.isAccessAllowed(request, auditHandler);

        LOG.debug(String.format("maskType: %s, maskTypeDef: %s, maskedValue: %s", result.getMaskType(),
                result.getMaskTypeDef(), result.getMaskedValue()));
    }

    public HiveAccessType convertToAccessType(PrivPredicate predicate) {
        if (predicate == PrivPredicate.SHOW) {
            return HiveAccessType.USE;
        } else if (predicate == PrivPredicate.SELECT) {
            return HiveAccessType.SELECT;
        } else if (predicate == PrivPredicate.ADMIN || predicate == PrivPredicate.ALL) {
            return HiveAccessType.ALL;
        } else if (predicate == PrivPredicate.LOAD) {
            return HiveAccessType.UPDATE;
        } else if (predicate == PrivPredicate.ALTER) {
            return HiveAccessType.ALTER;
        } else if (predicate == PrivPredicate.CREATE) {
            return HiveAccessType.CREATE;
        } else if (predicate == PrivPredicate.DROP) {
            return HiveAccessType.DROP;
        } else {
            return HiveAccessType.NONE;
        }
    }

    @Override
    public boolean checkCtlPriv(UserIdentity currentUser, String ctl, PrivPredicate wanted) {
        return true;
    }

    @Override
    public boolean checkDbPriv(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted) {
        RangerHiveResource resource = new RangerHiveResource(HiveObjectType.DATABASE,
                ClusterNamespace.getNameFromFullName(db));
        return checkPrivilege(currentUser, convertToAccessType(wanted), resource);
    }

    @Override
    public boolean checkTblPriv(UserIdentity currentUser, String ctl, String db, String tbl, PrivPredicate wanted) {
        RangerHiveResource resource = new RangerHiveResource(HiveObjectType.TABLE,
                ClusterNamespace.getNameFromFullName(db), tbl);
        return checkPrivilege(currentUser, convertToAccessType(wanted), resource);
    }

    @Override
    public void checkColsPriv(UserIdentity currentUser, String ctl, String db, String tbl, Set<String> cols,
            PrivPredicate wanted) throws AuthorizationException {
        List<RangerHiveResource> resources = new ArrayList<>();
        for (String col : cols) {
            RangerHiveResource resource = new RangerHiveResource(HiveObjectType.COLUMN,
                    ClusterNamespace.getNameFromFullName(db), tbl, col);
            resources.add(resource);
        }

        checkPrivileges(currentUser, convertToAccessType(wanted), resources);
    }
}
