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

import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authorization.DataMaskSpec;
import org.apache.doris.authorization.RowFilterMergeType;
import org.apache.doris.authorization.RowFilterSpec;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.AuthorizationException;
import org.apache.doris.policy.FilterType;
import org.apache.doris.policy.RowPolicy;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The privilege model Doris ships with: users, roles and {@code GRANT} statements.
 *
 * <p>Every scoped check answers "globally, then at this scope". That order is this implementation's own
 * decision, not something the engine arranges for it, and it is not redundant with the checks {@link Auth}
 * runs per role: {@link Auth} refuses NODE privileges below global level, so a caller holding only global
 * NODE_PRIV is granted by the global check here and by nothing else.
 */
public class InternalAccessController implements CatalogAccessController {
    private Auth auth;

    public InternalAccessController(Auth auth) {
        this.auth = auth;
    }

    @Override
    public boolean checkGlobalPriv(UserIdentity currentUser, PrivPredicate wanted) {
        return auth.checkGlobalPriv(currentUser, wanted);
    }

    @Override
    public boolean checkCtlPriv(UserIdentity currentUser, String ctl, PrivPredicate wanted) {
        return checkGlobalPriv(currentUser, wanted) || auth.checkCtlPriv(currentUser, ctl, wanted);
    }

    @Override
    public boolean checkDbPriv(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted) {
        return checkGlobalPriv(currentUser, wanted) || auth.checkDbPriv(currentUser, ctl, db, wanted);
    }

    @Override
    public boolean checkTblPriv(UserIdentity currentUser, String ctl, String db, String tbl, PrivPredicate wanted) {
        return checkGlobalPriv(currentUser, wanted) || auth.checkTblPriv(currentUser, ctl, db, tbl, wanted);
    }

    @Override
    public void checkColsPriv(UserIdentity currentUser, String ctl, String db, String tbl, Set<String> cols,
            PrivPredicate wanted) throws AuthorizationException {
        if (checkGlobalPriv(currentUser, wanted)) {
            return;
        }
        auth.checkColsPriv(currentUser, ctl, db, tbl, cols, wanted);
    }

    @Override
    public boolean checkResourcePriv(UserIdentity currentUser, String resourceName, PrivPredicate wanted) {
        return auth.checkResourcePriv(currentUser, resourceName, wanted);
    }

    @Override
    public boolean checkWorkloadGroupPriv(UserIdentity currentUser, String workloadGroupName, PrivPredicate wanted) {
        return auth.checkWorkloadGroupPriv(currentUser, workloadGroupName, wanted);
    }

    @Override
    public boolean checkCloudPriv(UserIdentity currentUser, String cloudName,
            PrivPredicate wanted, ResourceTypeEnum type) {
        return auth.checkCloudPriv(currentUser, cloudName, wanted, type);
    }

    @Override
    public boolean checkStorageVaultPriv(UserIdentity currentUser, String storageVaultName, PrivPredicate wanted) {
        return auth.checkStorageVaultPriv(currentUser, storageVaultName, wanted);
    }

    @Override
    public Optional<DataMaskSpec> evalDataMaskPolicy(UserIdentity currentUser, String ctl, String db, String tbl,
            String col) {
        // The built-in privilege model has no column masking: there is no DDL to define one.
        return Optional.empty();
    }

    @Override
    public List<RowFilterSpec> evalRowFilterPolicies(UserIdentity currentUser, String ctl, String db,
            String tbl) {
        List<RowPolicy> policies = Env.getCurrentEnv().getPolicyMgr().getUserPolicies(ctl, db, tbl, currentUser);
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
