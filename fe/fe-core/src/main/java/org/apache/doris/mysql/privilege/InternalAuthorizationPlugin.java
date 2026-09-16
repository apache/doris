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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authorization.AccessContext;
import org.apache.doris.authorization.AccessDeniedException;
import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.AuthorizedSubject;
import org.apache.doris.authorization.DataMaskSpec;
import org.apache.doris.authorization.RowFilterMergeType;
import org.apache.doris.authorization.RowFilterSpec;
import org.apache.doris.authorization.spi.AuthorizationPlugin;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.AuthorizationException;
import org.apache.doris.policy.FilterType;
import org.apache.doris.policy.RowPolicy;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The privilege model Doris ships with: users, roles and {@code GRANT} statements.
 *
 * <p>It is an authorization source like any other and is reached the same way, which is what keeps the
 * built-in behaviour describable in the same terms as an external system's: it governs whatever no plugin has
 * been bound to, and what it decides there it decides alone.
 *
 * <p>Every scoped check answers "globally, then at this scope". That order is this implementation's own
 * decision, not something the engine arranges for it, and it is not redundant with the checks {@link Auth}
 * runs per role: {@link Auth} refuses NODE privileges below global level, so a caller holding only global
 * NODE_PRIV is granted by the global check here and by nothing else. The system-wide objects below - a
 * resource, a workload group, a vault, a cloud object - are deliberately not preceded by one: {@link Auth}
 * already folds the global grants into those answers itself.
 */
public class InternalAuthorizationPlugin implements AuthorizationPlugin {

    /** The name the built-in model is selected by, and the value {@code access_controller_type} defaults to. */
    public static final String NAME = "default";

    private final Auth auth;

    public InternalAuthorizationPlugin(Auth auth) {
        this.auth = auth;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void checkPrivilege(AuthorizedSubject subject, AuthorizedResource resource,
            AccessRequirement requirement, AccessContext context) throws AccessDeniedException {
        UserIdentity currentUser = AccessTranslation.userIdentityOf(subject);
        PrivPredicate wanted = AccessTranslation.privPredicateOf(requirement);
        switch (resource.getKind()) {
            case GLOBAL:
                refuseUnless(auth.checkGlobalPriv(currentUser, wanted), subject, resource, requirement);
                return;
            case CATALOG: {
                AuthorizedResource.Catalog catalog = (AuthorizedResource.Catalog) resource;
                refuseUnless(auth.checkGlobalPriv(currentUser, wanted)
                                || auth.checkCtlPriv(currentUser, catalog.getCatalog(), wanted),
                        subject, resource, requirement);
                return;
            }
            case DATABASE: {
                AuthorizedResource.Database database = (AuthorizedResource.Database) resource;
                refuseUnless(auth.checkGlobalPriv(currentUser, wanted)
                                || auth.checkDbPriv(currentUser, database.getCatalog(),
                                        database.getDatabase(), wanted),
                        subject, resource, requirement);
                return;
            }
            case TABLE: {
                AuthorizedResource.Table table = (AuthorizedResource.Table) resource;
                refuseUnless(auth.checkGlobalPriv(currentUser, wanted)
                                || auth.checkTblPriv(currentUser, table.getCatalog(), table.getDatabase(),
                                        table.getTable(), wanted),
                        subject, resource, requirement);
                return;
            }
            case COLUMNS: {
                AuthorizedResource.Columns columns = (AuthorizedResource.Columns) resource;
                if (auth.checkGlobalPriv(currentUser, wanted)) {
                    return;
                }
                try {
                    auth.checkColsPriv(currentUser, columns.getCatalog(), columns.getDatabase(),
                            columns.getTable(), columns.getColumns(), wanted);
                } catch (AuthorizationException e) {
                    // The message names the column that failed, which is the answer here; rephrasing it
                    // would reduce "this column" to "these columns". Carried as the bare wording rather
                    // than as rendered: the engine puts this back into an AuthorizationException on the way
                    // out, and that class prefixes its own error code when it renders.
                    throw AccessDeniedException.withMessage(e.getDetailMessage(), resource, NAME);
                }
                return;
            }
            case RESOURCE:
                refuseUnless(auth.checkResourcePriv(currentUser,
                        ((AuthorizedResource.Named) resource).getName(), wanted), subject, resource, requirement);
                return;
            case WORKLOAD_GROUP:
                refuseUnless(auth.checkWorkloadGroupPriv(currentUser,
                        ((AuthorizedResource.Named) resource).getName(), wanted), subject, resource, requirement);
                return;
            case STORAGE_VAULT:
                refuseUnless(auth.checkStorageVaultPriv(currentUser,
                        ((AuthorizedResource.Named) resource).getName(), wanted), subject, resource, requirement);
                return;
            case CLOUD_GENERAL:
            case CLOUD_COMPUTE_GROUP:
            case CLOUD_STAGE:
            case CLOUD_STORAGE_VAULT:
                refuseUnless(auth.checkCloudPriv(currentUser, ((AuthorizedResource.Named) resource).getName(),
                                wanted, AccessTranslation.cloudTypeOf(resource.getKind())),
                        subject, resource, requirement);
                return;
            default:
                throw new IllegalStateException("the built-in privilege model has no answer for resource kind "
                        + resource.getKind());
        }
    }

    private void refuseUnless(boolean allowed, AuthorizedSubject subject, AuthorizedResource resource,
            AccessRequirement requirement) throws AccessDeniedException {
        if (!allowed) {
            throw AccessDeniedException.of(subject, resource, requirement, NAME);
        }
    }

    @Override
    public Map<String, DataMaskSpec> getDataMasks(AuthorizedSubject subject, AuthorizedResource.Table table,
            Set<String> columns, AccessContext context) {
        // The built-in privilege model has no column masking: there is no DDL to define one.
        return Collections.emptyMap();
    }

    @Override
    public List<RowFilterSpec> getRowFilters(AuthorizedSubject subject, AuthorizedResource.Table table,
            AccessContext context) {
        List<RowPolicy> policies = Env.getCurrentEnv().getPolicyMgr().getUserPolicies(table.getCatalog(),
                table.getDatabase(), table.getTable(), AccessTranslation.userIdentityOf(subject));
        ImmutableList.Builder<RowFilterSpec> specs = ImmutableList.builderWithExpectedSize(policies.size());
        for (RowPolicy policy : policies) {
            try {
                specs.add(new RowFilterSpec(policy.getPolicyIdent(), policy.getFilterSql(),
                        mergeTypeOf(policy.getFilterType())));
            } catch (AnalysisException e) {
                // A policy whose statement no longer parses cannot be turned into a filter, and dropping it
                // would silently widen access to the whole table. Fail the query with the same message the
                // planner used to raise when it asked the policy for its expression.
                throw new org.apache.doris.nereids.exceptions.AnalysisException(e.getMessage(), e);
            }
        }
        return specs.build();
    }

    private static RowFilterMergeType mergeTypeOf(FilterType filterType) {
        switch (filterType) {
            case PERMISSIVE:
                return RowFilterMergeType.PERMISSIVE;
            case RESTRICTIVE:
                return RowFilterMergeType.RESTRICTIVE;
            default:
                // Same shape as the planner's merge switch used to have: an unmapped filter type is a bug,
                // and guessing either way would change which rows the user sees.
                throw new IllegalStateException("Invalid operator");
        }
    }
}
