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
import org.apache.doris.catalog.authorizer.ranger.doris.DorisAccessType;
import org.apache.doris.common.AuthorizationException;
import org.apache.doris.mysql.privilege.CatalogAccessController;
import org.apache.doris.mysql.privilege.DataMaskPolicy;
import org.apache.doris.mysql.privilege.RangerDataMaskPolicy;
import org.apache.doris.mysql.privilege.RangerRowFilterPolicy;
import org.apache.doris.mysql.privilege.RowFilterPolicy;

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

    @Override
    public List<? extends RowFilterPolicy> evalRowFilterPolicies(UserIdentity currentUser, String ctl, String db,
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
        List<RangerRowFilterPolicy> res = Lists.newArrayList();
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
        res.add(new RangerRowFilterPolicy(currentUser, ctl, db, tbl, policy.getPolicyId(), policy.getPolicyVersion(),
                filterExpr));
        return res;
    }

    @Override
    public Optional<DataMaskPolicy> evalDataMaskPolicy(UserIdentity currentUser, String ctl, String db, String tbl,
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
                return Optional.of(new RangerDataMaskPolicy(currentUser, ctl, db, tbl, col, policy.getPolicyId(),
                        policy.getPolicyVersion(), maskType, "NULL"));
            case "MASK_NONE":
                return Optional.empty();
            case "CUSTOM":
                String maskedValue = policy.getMaskedValue();
                if (StringUtils.isEmpty(maskedValue)) {
                    return Optional.empty();
                }
                return Optional.of(new RangerDataMaskPolicy(currentUser, ctl, db, tbl, col, policy.getPolicyId(),
                        policy.getPolicyVersion(), maskType, maskedValue.replace("{col}", col)));
            default:
                String transformer = policy.getMaskTypeDef().getTransformer();
                if (StringUtils.isEmpty(transformer)) {
                    return Optional.empty();
                }
                return Optional.of(new RangerDataMaskPolicy(currentUser, ctl, db, tbl, col, policy.getPolicyId(),
                        policy.getPolicyVersion(), maskType, transformer.replace("{col}", col)));
        }
    }

    protected abstract RangerAccessRequestImpl createRequest(UserIdentity currentUser);

    protected abstract RangerAccessResourceImpl createResource(String ctl, String db, String tbl);

    protected abstract RangerAccessResourceImpl createResource(String ctl, String db, String tbl, String col);

    protected abstract RangerBasePlugin getPlugin();

    protected abstract RangerAccessResultProcessor getAccessResultProcessor();
}
