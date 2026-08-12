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

package org.apache.doris.catalog.authorizer.ranger;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authorization.DataMaskSpec;
import org.apache.doris.authorization.RowFilterSpec;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.authorizer.ranger.doris.DorisAccessType;
import org.apache.doris.common.AuthorizationException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.CatalogAccessController;
import org.apache.doris.mysql.privilege.PrivPredicate;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

public abstract class RangerAccessController implements CatalogAccessController {
    private static final Logger LOG = LogManager.getLogger(RangerAccessController.class);

    protected static final String CLIENT_TYPE_DORIS = "doris";

    /**
     * Whether the privilege is already held at global scope, which the Ranger plugins honour as a grant on
     * everything they govern.
     *
     * <p>Global scope is not a Ranger catalog: it belongs to whichever controller {@code access_controller_type}
     * installs, so that is who gets asked. With the built-in controller there this reproduces "an administrator
     * of the cluster can reach a Ranger-governed catalog"; with Ranger installed globally, Ranger decides its
     * own exemptions and the built-in grants stay out of it. Deciding this here rather than in the engine is
     * what lets a third-party controller refuse the exemption outright.
     *
     * <p>Returns false without asking when this controller is itself the global-scope authority: the caller's
     * own global check answers the same question one line later.
     */
    protected boolean grantedByGlobalScopeAuthority(UserIdentity currentUser, PrivPredicate wanted) {
        AccessControllerManager manager = Env.getCurrentEnv().getAccessManager();
        return !manager.isGlobalScopeAuthority(this) && manager.checkGlobalPriv(currentUser, wanted);
    }

    protected static boolean checkRequestResult(RangerAccessRequestImpl request,
            RangerAccessResult result, String name) {
        if (result == null) {
            LOG.warn("Error getting authorizer result, please check your ranger config. Make sure "
                    + "ranger policy engine is initialized. Request: {}", request);
            return false;
        }

        if (result.getIsAllowed()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("request {} match policy {}", request, result.getPolicyId());
            }
            return true;
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format(
                        "Permission denied: user [%s] does not have privilege for [%s] command on [%s]",
                        result.getAccessRequest().getUser(), name,
                        result.getAccessRequest().getResource().getAsString()));
            }
            return false;
        }
    }

    public static void checkRequestResults(Collection<RangerAccessResult> results, String name)
            throws AuthorizationException {
        for (RangerAccessResult result : results) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("request {} match policy {}", result.getAccessRequest(), result.getPolicyId());
            }
            if (!result.getIsAllowed()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(result.getReason());
                }
                throw new AuthorizationException(String.format(
                        "Permission denied: user [%s] does not have privilege for [%s] command on [%s]",
                        result.getAccessRequest().getUser(), name,
                        Optional.ofNullable(result.getAccessRequest().getResource().getAsString())
                                .orElse("unknown resource").replaceAll("/", ".")));
            }
        }
    }

    /**
     * Identifies a Ranger policy for auditing and for the SQL cache's change detection. The version is part of
     * it on purpose: an administrator editing a policy in place keeps its id, and without the version an
     * updated policy would compare equal to the one the cached result was planned with.
     */
    private static String policyIdent(RangerAccessResult policy) {
        return policy.getPolicyId() + ":" + policy.getPolicyVersion();
    }

    @Override
    public List<RowFilterSpec> evalRowFilterPolicies(UserIdentity currentUser, String ctl, String db,
            String tbl) {
        RangerAccessResourceImpl resource = createResource(ctl, db, tbl);
        RangerAccessRequestImpl request = createRequest(currentUser);
        // If the access type is not set here, it defaults to ANY1 ACCESS.
        // The internal logic of the ranger is to traverse all permission items.
        // Since the ranger UI will set the access type to 'SELECT',
        // we will keep it consistent with the UI here to avoid performance issues
        request.setAccessType(DorisAccessType.SELECT.name());
        request.setResource(resource);

        if (LOG.isDebugEnabled()) {
            LOG.debug("ranger request: {}", request);
        }
        List<RowFilterSpec> res = Lists.newArrayList();
        RangerAccessResult policy = getPlugin().evalRowFilterPolicies(request, getAccessResultProcessor());
        if (LOG.isDebugEnabled()) {
            LOG.debug("ranger response: {}", policy);
        }
        if (policy == null) {
            return res;
        }
        String filterExpr = policy.getFilterExpr();
        if (StringUtils.isEmpty(filterExpr)) {
            return res;
        }
        // Ranger row filters are always restrictive: it returns at most one expression and the row must match it.
        res.add(RowFilterSpec.restrictive(policyIdent(policy), filterExpr));
        return res;
    }

    @Override
    public Optional<DataMaskSpec> evalDataMaskPolicy(UserIdentity currentUser, String ctl, String db, String tbl,
            String col) {
        RangerAccessResourceImpl resource = createResource(ctl, db, tbl, col);
        RangerAccessRequestImpl request = createRequest(currentUser);
        request.setAccessType(DorisAccessType.SELECT.name());
        request.setResource(resource);

        if (LOG.isDebugEnabled()) {
            LOG.debug("ranger request: {}", request);
        }
        RangerAccessResult policy = getPlugin().evalDataMaskPolicies(request, getAccessResultProcessor());
        if (LOG.isDebugEnabled()) {
            LOG.debug("ranger response: {}", policy);
        }
        if (policy == null) {
            return Optional.empty();
        }
        String maskType = policy.getMaskType();
        if (StringUtils.isEmpty(maskType)) {
            return Optional.empty();
        }
        switch (maskType) {
            case "MASK_NULL":
                return Optional.of(new DataMaskSpec(policyIdent(policy), "NULL"));
            case "MASK_NONE":
                return Optional.empty();
            case "CUSTOM":
                String maskedValue = policy.getMaskedValue();
                if (StringUtils.isEmpty(maskedValue)) {
                    return Optional.empty();
                }
                return Optional.of(new DataMaskSpec(policyIdent(policy), maskedValue.replace("{col}", col)));
            default:
                String transformer = policy.getMaskTypeDef().getTransformer();
                if (StringUtils.isEmpty(transformer)) {
                    return Optional.empty();
                }
                return Optional.of(new DataMaskSpec(policyIdent(policy), transformer.replace("{col}", col)));
        }
    }

    protected abstract RangerAccessRequestImpl createRequest(UserIdentity currentUser);

    protected abstract RangerAccessResourceImpl createResource(String ctl, String db, String tbl);

    protected abstract RangerAccessResourceImpl createResource(String ctl, String db, String tbl, String col);

    protected abstract RangerBasePlugin getPlugin();

    protected abstract RangerAccessResultProcessor getAccessResultProcessor();
}
