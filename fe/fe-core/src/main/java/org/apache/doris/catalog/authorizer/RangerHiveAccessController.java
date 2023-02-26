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
import org.apache.doris.common.AuthorizationException;
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

public class RangerHiveAccessController implements CatalogAccessController {
    public static final String CLIENT_TYPE_DORIS = "doris";
    private static final Logger LOG = LogManager.getLogger(RangerHiveAccessController.class);
    private static volatile RangerHivePlugin hivePlugin = null;

    public RangerHiveAccessController(Map<String, String> properties) {
        String serviceName = properties.get("ranger.service.name");
        hivePlugin = new RangerHivePlugin(serviceName);
    }

    private RangerAccessRequestImpl createRequest(UserIdentity currentUser, HiveAccessType accessType) {
        RangerAccessRequestImpl request = new RangerAccessRequestImpl();
        request.setUser(currentUser.getQualifiedUser());
        request.setUserRoles(currentUser.getRoles());
        request.setAction(accessType.name());
        if (accessType == HiveAccessType.USE) {
            request.setAccessType(RangerPolicyEngine.ANY_ACCESS);
        } else {
            request.setAccessType(accessType.name().toLowerCase());
        }
        request.setClientIPAddress(currentUser.getHost());
        request.setClientType(CLIENT_TYPE_DORIS);
        request.setAccessTime(new Date());

        return request;
    }

    public void checkPrivileges(UserIdentity currentUser, HiveAccessType accessType,
                                List<RangerHiveResource> hiveResources) throws AuthorizationException {
        List<RangerAccessRequest> requests = new ArrayList<>();
        for (RangerHiveResource resource : hiveResources) {
            RangerAccessRequestImpl request = createRequest(currentUser, accessType);
            request.setResource(resource);

            requests.add(request);
        }

        RangerHiveAuditHandler auditHandler = new RangerHiveAuditHandler(hivePlugin.getConfig());
        Collection<RangerAccessResult> results = hivePlugin.isAccessAllowed(requests, auditHandler);
        for (RangerAccessResult result : results) {
            LOG.debug("match policy:" + result.getPolicyId());
            LOG.debug("will audit: " + result.toString());
            auditHandler.processResult(result);
            if (!result.getIsAllowed()) {
                LOG.warn(result.getReason());
                throw new AuthorizationException(String.format(
                    "Permission denied: user [%s] does not have privilege for [%s] command on [%s]",
                    currentUser.getQualifiedUser(), accessType.name(),
                    result.getAccessRequest().getResource().getAsString()));
            }
        }
    }

    public boolean checkPrivilege(UserIdentity currentUser, HiveAccessType accessType,
                                  RangerHiveResource resource) {
        RangerHiveAuditHandler auditHandler = new RangerHiveAuditHandler(hivePlugin.getConfig());
        RangerAccessRequestImpl request = createRequest(currentUser, accessType);
        request.setResource(resource);

        RangerAccessResult result = hivePlugin.isAccessAllowed(request, auditHandler);
        auditHandler.processResult(result);
        auditHandler.flushAudit();

        if (result == null) {
            LOG.warn("Error getting authorizer result, please check your ranger config");
            return false;
        }

        if (result.getIsAllowed()) {
            LOG.debug("pass");
            return true;
        } else {
            LOG.warn(String.format(
                    "Permission denied: user [%s] does not have privilege for [%s] command on [%s]",
                    currentUser.getQualifiedUser(), accessType.name(),
                    result.getAccessRequest().getResource().getAsString()));
            return false;
        }
    }

    public void getFilterExpr(UserIdentity currentUser, HiveAccessType accessType,
                              RangerHiveResource resource) throws HiveAccessControlException {
        RangerAccessRequestImpl request = createRequest(currentUser, accessType);
        request.setResource(resource);
        RangerHiveAuditHandler auditHandler = new RangerHiveAuditHandler(hivePlugin.getConfig());
        RangerAccessResult result = hivePlugin.isAccessAllowed(request, auditHandler);
        LOG.debug("getFilterExpr: " + result.getFilterExpr());
    }

    public void getColumnMask(UserIdentity currentUser, HiveAccessType accessType,
                              RangerHiveResource resource) {
        RangerAccessRequestImpl request = createRequest(currentUser, accessType);
        request.setResource(resource);
        RangerHiveAuditHandler auditHandler = new RangerHiveAuditHandler(hivePlugin.getConfig());
        RangerAccessResult result = hivePlugin.isAccessAllowed(request, auditHandler);
        LOG.debug("getColumnMask:" + result.getMaskType());
        LOG.debug("getColumnMask:" + result.getMaskTypeDef());
        LOG.debug("getColumnMask:" + result.getMaskedValue());
    }

    public HiveAccessType convertToAccessType(PrivPredicate predicate) {
        if (predicate == PrivPredicate.SHOW) {
            return HiveAccessType.USE;
        } else if (predicate == PrivPredicate.ADMIN) {
            return HiveAccessType.ALL;
        } else if (predicate == PrivPredicate.LOAD) {
            return HiveAccessType.UPDATE;
        } else if (predicate == PrivPredicate.ALTER) {
            return HiveAccessType.ALTER;
        } else if (predicate == PrivPredicate.CREATE) {
            return HiveAccessType.CREATE;
        } else if (predicate == PrivPredicate.DROP) {
            return HiveAccessType.DROP;
        } else if (predicate == PrivPredicate.SELECT) {
            return HiveAccessType.SELECT;
        } else {
            return HiveAccessType.NONE;
        }
    }

    @Override
    public boolean checkCtlPriv(UserIdentity currentUser, String ctl, PrivPredicate wanted) {
        return false;
    }

    @Override
    public boolean checkDbPriv(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted) {
        RangerHiveResource resource = new RangerHiveResource(HiveObjectType.DATABASE, db);
        return checkPrivilege(currentUser, convertToAccessType(wanted), resource);
    }

    @Override
    public boolean checkTblPriv(UserIdentity currentUser, String ctl, String db, String tbl, PrivPredicate wanted) {
        RangerHiveResource resource = new RangerHiveResource(HiveObjectType.TABLE, db, tbl);
        return checkPrivilege(currentUser, convertToAccessType(wanted), resource);
    }

    @Override
    public void checkColsPriv(UserIdentity currentUser, String ctl, String db, String tbl, Set<String> cols,
                              PrivPredicate wanted) throws AuthorizationException {
        List<RangerHiveResource> resources = new ArrayList<>();
        for (String col : cols) {
            RangerHiveResource resource = new RangerHiveResource(HiveObjectType.COLUMN, db, tbl, col);
            resources.add(resource);
        }

        checkPrivileges(currentUser, convertToAccessType(wanted), resources);
    }
}
