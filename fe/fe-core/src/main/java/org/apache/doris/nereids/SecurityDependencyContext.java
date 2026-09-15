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

package org.apache.doris.nereids;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authorization.DataMaskSpec;
import org.apache.doris.authorization.RowFilterSpec;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.InternalAuthorizationPlugin;
import org.apache.doris.policy.PolicyMgr;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Security state which a reusable prepared point-query plan depends on.
 *
 * <p>Row policies are deliberately not copied or compared here. A plan with a row policy is not eligible for
 * short-circuit execution, while the process-local policy version invalidates a previously cached no-policy plan
 * when a policy is later added. This keeps the common validation path to a few identity and volatile-version reads.
 * Authorization sources without reliable local versions are never reused without replanning.
 */
public class SecurityDependencyContext {
    private static final long UNKNOWN_VERSION = -1;

    private final UserIdentity planningUserIdentity;
    private final Set<String> planningAuthenticatedRoles;
    private final Env planningEnv;
    private final long authorizationVersion;
    private final long rowPolicyVersion;
    private final boolean versionValidationEligible;
    private boolean privilegeChecked;
    private boolean internalCatalogOnly = true;
    private boolean olapTableOnly = true;
    private boolean hasRowPolicy;
    private boolean hasDataMask;
    private boolean useVersionValidation;
    private boolean complete = true;

    /** Create an incomplete context for tests and callers without a connection. */
    public SecurityDependencyContext() {
        this(null, ImmutableSet.of(), null, UNKNOWN_VERSION, UNKNOWN_VERSION, false);
    }

    /** Capture the effective authorization subject and security versions before analysis starts. */
    public SecurityDependencyContext(ConnectContext connectContext) {
        this(connectContext == null ? null : connectContext.getCurrentUserIdentity(),
                authenticatedRoles(connectContext),
                connectContext == null ? null : connectContext.getEnv(),
                usesAuthorizationChecks(connectContext));
    }

    private SecurityDependencyContext(UserIdentity planningUserIdentity, Set<String> planningAuthenticatedRoles,
            Env planningEnv, boolean versionValidationEligible) {
        this(planningUserIdentity, planningAuthenticatedRoles, planningEnv,
                currentAuthorizationVersion(planningEnv), currentRowPolicyVersion(planningEnv),
                versionValidationEligible);
    }

    private SecurityDependencyContext(UserIdentity planningUserIdentity, Set<String> planningAuthenticatedRoles,
            Env planningEnv, long authorizationVersion, long rowPolicyVersion,
            boolean versionValidationEligible) {
        this.planningUserIdentity = planningUserIdentity;
        this.planningAuthenticatedRoles = planningAuthenticatedRoles;
        this.planningEnv = planningEnv;
        this.authorizationVersion = authorizationVersion;
        this.rowPolicyVersion = rowPolicyVersion;
        this.versionValidationEligible = versionValidationEligible;
    }

    /** Record that SELECT privileges were checked and whether the relation supports version-only validation. */
    public synchronized void addCheckedPrivilege(TableIf table, Set<String> usedColumns) {
        if (table == null) {
            complete = false;
            return;
        }
        DatabaseIf<?> database = table.getDatabase();
        CatalogIf<?> catalog = database == null ? null : database.getCatalog();
        if (catalog == null) {
            complete = false;
            return;
        }
        privilegeChecked = true;
        internalCatalogOnly &= InternalCatalog.INTERNAL_CATALOG_NAME.equals(catalog.getName());
        olapTableOnly &= table instanceof OlapTable;
    }

    /** Record only whether a row policy exists; policy objects never enter the point-query cache. */
    public synchronized void setRowPolicies(
            String catalog, String database, String table, List<RowFilterSpec> policies) {
        hasRowPolicy |= policies != null && !policies.isEmpty();
    }

    /** A row policy makes the statement ineligible for short-circuit execution. */
    public synchronized boolean hasRowPolicy() {
        return hasRowPolicy;
    }

    /** Record mask presence so a masked plan is never accepted by the version-only cache path. */
    public synchronized void addDataMask(
            String catalog, String database, String table, String column, Optional<DataMaskSpec> mask) {
        hasDataMask |= mask.isPresent();
    }

    /** Freeze the decisions used by a completed plan before storing them in a reusable context. */
    public synchronized SecurityDependencyContext snapshot() {
        SecurityDependencyContext snapshot = new SecurityDependencyContext(
                planningUserIdentity, planningAuthenticatedRoles, planningEnv,
                authorizationVersion, rowPolicyVersion, versionValidationEligible);
        snapshot.privilegeChecked = privilegeChecked;
        snapshot.internalCatalogOnly = internalCatalogOnly;
        snapshot.olapTableOnly = olapTableOnly;
        snapshot.hasRowPolicy = hasRowPolicy;
        snapshot.hasDataMask = hasDataMask;
        snapshot.complete = complete;
        snapshot.useVersionValidation = snapshot.canUseVersionValidation();
        return snapshot;
    }

    /** Freeze a prepared short-circuit dependency set, failing closed if its proof is incomplete. */
    public synchronized SecurityDependencyContext snapshotForShortCircuit() {
        SecurityDependencyContext snapshot = snapshot();
        if (!privilegeChecked || hasRowPolicy) {
            snapshot.complete = false;
            snapshot.useVersionValidation = false;
        }
        return snapshot;
    }

    /**
     * Check whether an analyzed point-query plan can bypass planning again.
     *
     * <p>A false result rejects only cached reuse. The prepared statement is reparsed and analyzed normally, so
     * authorization failures retain their standard user-facing error. The authorization subject is checked before
     * the version shortcut because COM_CHANGE_USER keeps the connection's prepared statements alive.
     */
    public boolean isValid(ConnectContext connectContext) {
        if (!complete || !useVersionValidation || connectContext == null
                || !Objects.equals(planningUserIdentity, connectContext.getCurrentUserIdentity())
                || !planningAuthenticatedRoles.equals(authenticatedRoles(connectContext))) {
            return false;
        }
        try {
            return usesAuthorizationChecks(connectContext) && versionsAreCurrent(connectContext.getEnv());
        } catch (RuntimeException e) {
            return false;
        }
    }

    private boolean canUseVersionValidation() {
        return complete && versionValidationEligible && privilegeChecked && internalCatalogOnly && olapTableOnly
                && !hasRowPolicy && !hasDataMask
                && authorizationVersion != UNKNOWN_VERSION && rowPolicyVersion != UNKNOWN_VERSION
                && usesVersionedBuiltInAuthorization(planningEnv);
    }

    private boolean versionsAreCurrent(Env env) {
        if (env == null || env != planningEnv) {
            return false;
        }
        Auth auth = env.getAuth();
        PolicyMgr policyMgr = env.getPolicyMgr();
        return auth != null && policyMgr != null
                && auth.isAuthorizationVersionReliable()
                && env.getAccessManager().getAccessControllerOrDefault(InternalCatalog.INTERNAL_CATALOG_NAME)
                        instanceof InternalAuthorizationPlugin
                && auth.getAuthorizationVersion() == authorizationVersion
                && policyMgr.getRowPolicyVersion() == rowPolicyVersion;
    }

    private static boolean usesVersionedBuiltInAuthorization(Env env) {
        return env != null && env.getAuth() != null && env.getPolicyMgr() != null
                && env.getAuth().isAuthorizationVersionReliable()
                && env.getAccessManager().getAccessControllerOrDefault(InternalCatalog.INTERNAL_CATALOG_NAME)
                        instanceof InternalAuthorizationPlugin;
    }

    private static long currentAuthorizationVersion(Env env) {
        Auth auth = env == null ? null : env.getAuth();
        return auth == null ? UNKNOWN_VERSION : auth.getAuthorizationVersion();
    }

    private static long currentRowPolicyVersion(Env env) {
        PolicyMgr policyMgr = env == null ? null : env.getPolicyMgr();
        return policyMgr == null ? UNKNOWN_VERSION : policyMgr.getRowPolicyVersion();
    }

    private static boolean usesAuthorizationChecks(ConnectContext connectContext) {
        if (connectContext == null || connectContext.isSkipAuth()) {
            return false;
        }
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        return sessionVariable != null && !sessionVariable.isPlayNereidsDump();
    }

    private static Set<String> authenticatedRoles(ConnectContext connectContext) {
        if (connectContext == null) {
            return ImmutableSet.of();
        }
        Set<String> roles = connectContext.getAuthenticatedRoles();
        return roles == null || roles.isEmpty() ? ImmutableSet.of() : ImmutableSet.copyOf(roles);
    }
}
