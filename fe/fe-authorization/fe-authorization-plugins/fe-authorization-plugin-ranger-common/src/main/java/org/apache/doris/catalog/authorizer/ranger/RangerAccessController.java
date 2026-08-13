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

import org.apache.doris.authorization.AccessContext;
import org.apache.doris.authorization.AccessDeniedException;
import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.AuthorizedSubject;
import org.apache.doris.authorization.DataMaskSpec;
import org.apache.doris.authorization.RowFilterSpec;
import org.apache.doris.authorization.spi.AuthorizationContext;
import org.apache.doris.authorization.spi.AuthorizationPlugin;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * What the Ranger-backed authorization sources have in common: they answer out of a Ranger service's
 * policies, and they honour whoever governs instance scope.
 */
public abstract class RangerAccessController implements AuthorizationPlugin {
    private static final Logger LOG = LogManager.getLogger(RangerAccessController.class);

    protected static final String CLIENT_TYPE_DORIS = "doris";

    /**
     * Property switching off deference to whoever governs instance scope, so that a deployment can decide
     * that inside what Ranger governs, only Ranger's policies grant anything - not even to an administrator
     * of the instance. Defaults to deferring, which is what Doris did before this was a source's own choice.
     */
    public static final String DEFER_TO_GLOBAL_SCOPE_AUTHORITY = "ranger.defer_to_global_scope_authority";

    private final AuthorizationContext context;
    private final boolean deferToGlobalScopeAuthority;

    protected RangerAccessController(Map<String, String> properties, AuthorizationContext context) {
        this.context = Objects.requireNonNull(context, "authorization context is required");
        this.deferToGlobalScopeAuthority = deferenceFrom(properties);
    }

    /** What this source may ask the engine. */
    protected AuthorizationContext getContext() {
        return context;
    }

    /**
     * Whether the requirement is already held at instance scope, which these sources honour as a grant on
     * everything they govern.
     *
     * <p>Instance scope is not a Ranger service: it belongs to whichever source {@code access_controller_type}
     * installs, so that is who the engine asks on our behalf. With the built-in model there this reproduces
     * "an administrator of the cluster can reach a Ranger-governed catalog"; with Ranger installed for the
     * instance, Ranger decides its own exemptions and the built-in grants stay out of it. Deciding it here
     * rather than in the engine is what lets a source refuse the exemption outright - as this one does when
     * configured to.
     */
    protected boolean grantedByGlobalScopeAuthority(AuthorizedSubject subject, AccessRequirement requirement) {
        return deferToGlobalScopeAuthority && context.grantedByGlobalScopeAuthority(subject, requirement);
    }

    private static boolean deferenceFrom(Map<String, String> properties) {
        String configured = properties == null ? null : properties.get(DEFER_TO_GLOBAL_SCOPE_AUTHORITY);
        if (configured == null) {
            return true;
        }
        String value = configured.trim();
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        // Parsing this leniently would read a typo as "false" and silently take away an administrator's
        // access to every Ranger-governed object.
        throw new IllegalArgumentException(DEFER_TO_GLOBAL_SCOPE_AUTHORITY + " must be true or false, but is \""
                + configured + "\"");
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

    /**
     * Refuses on the first request Ranger denied, naming the resource that request was about - which is the
     * answer when a batch of requests stands for the columns of one table.
     */
    protected void checkRequestResults(Collection<RangerAccessResult> results, String name,
            AuthorizedResource resource) throws AccessDeniedException {
        for (RangerAccessResult result : results) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("request {} match policy {}", result.getAccessRequest(), result.getPolicyId());
            }
            if (!result.getIsAllowed()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(result.getReason());
                }
                throw AccessDeniedException.withMessage(String.format(
                        "Permission denied: user [%s] does not have privilege for [%s] command on [%s]",
                        result.getAccessRequest().getUser(), name,
                        Optional.ofNullable(result.getAccessRequest().getResource().getAsString())
                                .orElse("unknown resource").replaceAll("/", ".")),
                        resource, name());
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
    public List<RowFilterSpec> getRowFilters(AuthorizedSubject subject, AuthorizedResource.Table table,
            AccessContext context) {
        RangerAccessResourceImpl resource = createResource(table.getCatalog(), table.getDatabase(),
                table.getTable());
        RangerAccessRequestImpl request = createRequest(subject);
        request.setAccessType(readAccessTypeName());
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
    public Map<String, DataMaskSpec> getDataMasks(AuthorizedSubject subject, AuthorizedResource.Table table,
            Set<String> columns, AccessContext context) {
        Map<String, DataMaskSpec> masks = new HashMap<>();
        for (String column : columns) {
            // One request per column: a masking policy in Ranger is written against a column, and the plugin
            // evaluates them one resource at a time.
            evalDataMaskPolicy(subject, table, column).ifPresent(mask -> masks.put(column, mask));
        }
        return masks;
    }

    private Optional<DataMaskSpec> evalDataMaskPolicy(AuthorizedSubject subject, AuthorizedResource.Table table,
            String col) {
        RangerAccessResourceImpl resource = createResource(table.getCatalog(), table.getDatabase(),
                table.getTable(), col);
        RangerAccessRequestImpl request = createRequest(subject);
        request.setAccessType(readAccessTypeName());
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

    /**
     * How this Ranger service type spells the access type a read is asked with.
     *
     * <p>Row-filter and data-mask lookups are made with it rather than left unset: unset means "any access",
     * which makes Ranger walk every permission item, and the Ranger UI writes these policies against the read
     * access type anyway, so asking with anything else costs time without changing the answer.
     *
     * <p>Every service type Doris talks to happens to spell it {@code SELECT}, which is why this used to be
     * one hard-coded value for both. It is asked of the subclass because the spelling belongs to the Ranger
     * service definition, not to Doris.
     */
    protected abstract String readAccessTypeName();

    protected abstract RangerAccessRequestImpl createRequest(AuthorizedSubject subject);

    protected abstract RangerAccessResourceImpl createResource(String ctl, String db, String tbl);

    protected abstract RangerAccessResourceImpl createResource(String ctl, String db, String tbl, String col);

    protected abstract RangerBasePlugin getPlugin();

    protected abstract RangerAccessResultProcessor getAccessResultProcessor();
}
