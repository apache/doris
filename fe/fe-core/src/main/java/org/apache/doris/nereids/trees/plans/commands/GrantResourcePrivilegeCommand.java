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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.ResourcePattern;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.analysis.WorkloadGroupPattern;
import org.apache.doris.catalog.AccessPrivilegeWithCols;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.ColPrivilegeKey;
import org.apache.doris.mysql.privilege.PrivBitSet;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.Privilege;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * GRANT privilege [, privilege] ON RESOURCE 'resource' TO user_identity [ROLE 'role'];
 */
public class GrantResourcePrivilegeCommand extends Command implements ForwardWithSync {
    private final Optional<UserIdentity> userIdentity;
    private final Optional<ResourcePattern> resourcePattern;
    private final Optional<WorkloadGroupPattern> workloadGroupPattern;
    private final Optional<String> role;
    // AccessPrivileges will be parsed into two parts,
    // with the column permissions section placed in "colPrivileges" and the others in "privileges"
    private final List<AccessPrivilegeWithCols> accessPrivileges;

    private Set<Privilege> privileges = Sets.newHashSet();
    // Privilege,ctl,db,table -> cols
    private Map<ColPrivilegeKey, Set<String>> colPrivileges = Maps.newHashMap();

    /**
     * GrantResourcePrivilegeCommand
     */
    public GrantResourcePrivilegeCommand(List<AccessPrivilegeWithCols> accessPrivileges,
            Optional<ResourcePattern> resourcePattern, Optional<WorkloadGroupPattern> workloadGroupPattern,
            Optional<String> role, Optional<UserIdentity> userIdentity) {
        super(PlanType.GRANT_RESOURCE_PRIVILEGE_COMMAND);
        this.accessPrivileges = Objects.requireNonNull(accessPrivileges, "accessPrivileges is null");
        this.resourcePattern = Objects.requireNonNull(resourcePattern, "resourcePattern is null");
        this.workloadGroupPattern = Objects.requireNonNull(workloadGroupPattern, "workloadGroupPattern is null");
        this.role = Objects.requireNonNull(role, "role is null");
        this.userIdentity = Objects.requireNonNull(userIdentity, "userIdentity is null");
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate();
        Env.getCurrentEnv().getAuth().grantResourcePrivilegeCommand(this);
    }

    /**
     * validate
     */
    public void validate() throws AnalysisException {
        if (Config.access_controller_type.equalsIgnoreCase("ranger-doris")) {
            throw new AnalysisException("Grant is prohibited when Ranger is enabled.");
        }

        if (userIdentity.isPresent()) {
            userIdentity.get().analyze();
        } else {
            FeNameFormat.checkRoleName(role.get(), false /* can not be admin */, "Can not grant to role");
        }

        if (resourcePattern.isPresent()) {
            resourcePattern.get().analyze();
        } else if (workloadGroupPattern.isPresent()) {
            workloadGroupPattern.get().analyze();
        }

        if (!CollectionUtils.isEmpty(accessPrivileges)) {
            checkAccessPrivileges(accessPrivileges);

            for (AccessPrivilegeWithCols accessPrivilegeWithCols : accessPrivileges) {
                accessPrivilegeWithCols.transferAccessPrivilegeToDoris(privileges, colPrivileges, null);
            }
        }

        if (CollectionUtils.isEmpty(privileges) && !role.isPresent() && MapUtils.isEmpty(colPrivileges)) {
            throw new AnalysisException("No privileges or roles in grant statement.");
        }

        if (resourcePattern.isPresent()) {
            PrivBitSet.convertResourcePrivToCloudPriv(resourcePattern.get(), privileges);
            checkResourcePrivileges(privileges, resourcePattern.get());
        } else if (workloadGroupPattern.isPresent()) {
            checkWorkloadGroupPrivileges(privileges, workloadGroupPattern.get());
        }
    }

    /**
     * checkAccessPrivileges
     */
    public static void checkAccessPrivileges(
            List<AccessPrivilegeWithCols> accessPrivileges) throws AnalysisException {
        for (AccessPrivilegeWithCols access : accessPrivileges) {
            if ((!access.getAccessPrivilege().canHasColPriv()) && !CollectionUtils
                    .isEmpty(access.getCols())) {
                throw new AnalysisException(
                    String.format("%s do not support col auth.", access.getAccessPrivilege().name()));
            }
        }
    }

    /**
     * checkWorkloadGroupPrivileges
     */
    public static void checkWorkloadGroupPrivileges(Collection<Privilege> privileges,
            WorkloadGroupPattern workloadGroupPattern) throws AnalysisException {
        Privilege.checkIncorrectPrivilege(Privilege.notBelongToWorkloadGroupPrivileges, privileges);

        PrivPredicate predicate = getPrivPredicate(privileges);
        AccessControllerManager accessManager = Env.getCurrentEnv().getAccessManager();
        if (!accessManager.checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)
                && !accessManager.checkWorkloadGroupPriv(ConnectContext.get(),
                workloadGroupPattern.getworkloadGroupName(), predicate)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ALL_ACCESS_DENIED_ERROR,
                    predicate.getPrivs().toPrivilegeList());
        }
    }

    /**
     * checkResourcePrivileges
     */
    public static void checkResourcePrivileges(Collection<Privilege> privileges,
            ResourcePattern resourcePattern) throws AnalysisException {
        Privilege.checkIncorrectPrivilege(Privilege.notBelongToResourcePrivileges, privileges);

        PrivPredicate predicate = getPrivPredicate(privileges);
        AccessControllerManager accessManager = Env.getCurrentEnv().getAccessManager();
        if (!accessManager.checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)
                && !checkResourcePriv(ConnectContext.get(), resourcePattern, predicate)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ALL_ACCESS_DENIED_ERROR,
                    predicate.getPrivs().toPrivilegeList());
        }
    }

    private static PrivPredicate getPrivPredicate(Collection<Privilege> privileges) {
        ArrayList<Privilege> privs = Lists.newArrayList(privileges);
        privs.add(Privilege.GRANT_PRIV);
        return PrivPredicate.of(PrivBitSet.of(privs), CompoundPredicate.Operator.AND);
    }

    private static boolean checkResourcePriv(ConnectContext ctx, ResourcePattern resourcePattern,
            PrivPredicate privPredicate) {
        if (resourcePattern.getPrivLevel() == Auth.PrivLevel.GLOBAL) {
            return Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, privPredicate);
        }

        switch (resourcePattern.getResourceType()) {
            case GENERAL:
            case STORAGE_VAULT:
                return Env.getCurrentEnv().getAccessManager()
                    .checkResourcePriv(ctx, resourcePattern.getResourceName(), privPredicate);
            case CLUSTER:
            case STAGE:
                return Env.getCurrentEnv().getAccessManager()
                    .checkCloudPriv(ctx, resourcePattern.getResourceName(), privPredicate,
                        resourcePattern.getResourceType());
            default:
                return true;
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitGrantResourcePrivilegeCommand(this, context);
    }

    public Optional<UserIdentity> getUserIdentity() {
        return userIdentity;
    }

    public Optional<ResourcePattern> getResourcePattern() {
        return resourcePattern;
    }

    public Optional<WorkloadGroupPattern> getWorkloadGroupPattern() {
        return workloadGroupPattern;
    }

    public Optional<String> getRole() {
        return role;
    }

    public List<AccessPrivilegeWithCols> getAccessPrivileges() {
        return accessPrivileges;
    }

    public Map<ColPrivilegeKey, Set<String>> getColPrivileges() {
        return colPrivileges;
    }

    public Set<Privilege> getPrivileges() {
        return privileges;
    }
}
