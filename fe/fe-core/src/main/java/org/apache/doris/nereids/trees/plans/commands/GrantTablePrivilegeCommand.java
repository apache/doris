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
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserIdentity;
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
 * GRANT privilege[(col1,col2...)] [, privilege] ON db.tbl TO user_identity [ROLE 'role'];
 */
public class GrantTablePrivilegeCommand extends Command implements ForwardWithSync {
    private final Optional<UserIdentity> userIdentity;
    private final TablePattern tablePattern;
    private final Optional<String> role;
    // AccessPrivileges will be parsed into two parts,
    // with the column permissions section placed in "colPrivileges" and the others in "privileges"
    private final List<AccessPrivilegeWithCols> accessPrivileges;

    private Set<Privilege> privileges = Sets.newHashSet();
    // Privilege,ctl,db,table -> cols
    private Map<ColPrivilegeKey, Set<String>> colPrivileges = Maps.newHashMap();

    public GrantTablePrivilegeCommand(List<AccessPrivilegeWithCols> accessPrivileges, TablePattern tablePattern,
                            Optional<UserIdentity> userIdentity, Optional<String> role) {
        super(PlanType.GRANT_TABLE_PRIVILEGE_COMMAND);
        this.accessPrivileges = Objects.requireNonNull(accessPrivileges, "accessPrivileges is null");
        this.tablePattern = Objects.requireNonNull(tablePattern, "tablePattern is null");
        this.userIdentity = Objects.requireNonNull(userIdentity, "userIdentity is null");
        this.role = Objects.requireNonNull(role, "role is null");
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate();
        Env.getCurrentEnv().getAuth().grantTablePrivilegeCommand(this);
    }

    /**
     * validate
     */
    public void validate() throws AnalysisException {
        if (Config.access_controller_type.equalsIgnoreCase("ranger-doris")) {
            throw new AnalysisException("Grant is prohibited when Ranger is enabled.");
        }

        tablePattern.analyze();

        if (userIdentity.isPresent()) {
            userIdentity.get().analyze();
        } else {
            FeNameFormat.checkRoleName(role.get(), false /* can not be admin */, "Can not grant to role");
        }

        if (!CollectionUtils.isEmpty(accessPrivileges)) {
            checkAccessPrivileges(accessPrivileges);

            for (AccessPrivilegeWithCols accessPrivilegeWithCols : accessPrivileges) {
                accessPrivilegeWithCols.transferAccessPrivilegeToDoris(privileges, colPrivileges, tablePattern);
            }
        }

        if (CollectionUtils.isEmpty(privileges) && !role.isPresent() && MapUtils.isEmpty(colPrivileges)) {
            throw new AnalysisException("No privileges or roles in grant statement.");
        }

        checkTablePrivileges(privileges, tablePattern, colPrivileges);
    }

    /**
     * checkAccessPrivileges
     */
    public static void checkAccessPrivileges(List<AccessPrivilegeWithCols> accessPrivileges) throws AnalysisException {
        for (AccessPrivilegeWithCols access : accessPrivileges) {
            if ((!access.getAccessPrivilege().canHasColPriv()) && !CollectionUtils
                    .isEmpty(access.getCols())) {
                throw new AnalysisException(
                    String.format("%s do not support col auth.", access.getAccessPrivilege().name()));
            }
        }
    }

    /**
     * Rules:
     * 1. some privs in Privilege.notBelongToTablePrivileges can not granted/revoked on table
     * 2. ADMIN_PRIV and NODE_PRIV can only be granted/revoked on GLOBAL level
     * 3. Only the user with NODE_PRIV can grant NODE_PRIV to other user
     * 4. Check that the current user has both grant_priv and the permissions to be assigned to others
     * 5. col priv must assign to specific table
     */
    public static void checkTablePrivileges(Collection<Privilege> privileges, TablePattern tblPattern,
            Map<ColPrivilegeKey, Set<String>> colPrivileges) throws AnalysisException {
        // Rule 1
        Privilege.checkIncorrectPrivilege(Privilege.notBelongToTablePrivileges, privileges);
        // Rule 2
        if (tblPattern.getPrivLevel() != Auth.PrivLevel.GLOBAL && (privileges.contains(Privilege.ADMIN_PRIV)
                || privileges.contains(Privilege.NODE_PRIV))) {
            throw new AnalysisException("ADMIN_PRIV and NODE_PRIV can only be granted/revoke on/from *.*.*");
        }

        // Rule 3
        if (privileges.contains(Privilege.NODE_PRIV) && !Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(ConnectContext.get(), PrivPredicate.OPERATOR)) {
            throw new AnalysisException("Only user with NODE_PRIV can grant/revoke NODE_PRIV to other user");
        }

        // Rule 4
        PrivPredicate predicate = getPrivPredicate(privileges);
        AccessControllerManager accessManager = Env.getCurrentEnv().getAccessManager();
        if (!accessManager.checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)
                && !checkTablePriv(ConnectContext.get(), predicate, tblPattern)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ALL_ACCESS_DENIED_ERROR,
                    predicate.getPrivs().toPrivilegeList());
        }

        // Rule 5
        if (!MapUtils.isEmpty(colPrivileges) && "*".equals(tblPattern.getTbl())) {
            throw new AnalysisException("Col auth must specify specific table");
        }
    }

    private static PrivPredicate getPrivPredicate(Collection<Privilege> privileges) {
        ArrayList<Privilege> privs = Lists.newArrayList(privileges);
        privs.add(Privilege.GRANT_PRIV);
        return PrivPredicate.of(PrivBitSet.of(privs), CompoundPredicate.Operator.AND);
    }

    private static boolean checkTablePriv(ConnectContext ctx, PrivPredicate wanted,
            TablePattern tblPattern) {
        AccessControllerManager accessManager = Env.getCurrentEnv().getAccessManager();
        switch (tblPattern.getPrivLevel()) {
            case GLOBAL:
                return accessManager.checkGlobalPriv(ctx, wanted);
            case CATALOG:
                return accessManager.checkCtlPriv(ConnectContext.get(),
                    tblPattern.getQualifiedCtl(), wanted);
            case DATABASE:
                return accessManager.checkDbPriv(ConnectContext.get(),
                    tblPattern.getQualifiedCtl(), tblPattern.getQualifiedDb(), wanted);
            default:
                return accessManager.checkTblPriv(ConnectContext.get(), tblPattern.getQualifiedCtl(),
                    tblPattern.getQualifiedDb(), tblPattern.getTbl(), wanted);

        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitGrantTablePrivilegeCommand(this, context);
    }

    public Optional<UserIdentity> getUserIdentity() {
        return userIdentity;
    }

    public Optional<String> getRole() {
        return role;
    }

    public List<AccessPrivilegeWithCols> getAccessPrivileges() {
        return accessPrivileges;
    }

    public Set<Privilege> getPrivileges() {
        return privileges;
    }

    public Map<ColPrivilegeKey, Set<String>> getColPrivileges() {
        return colPrivileges;
    }

    public TablePattern getTablePattern() {
        return tablePattern;
    }
}
