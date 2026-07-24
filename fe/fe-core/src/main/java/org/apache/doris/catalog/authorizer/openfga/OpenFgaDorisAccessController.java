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

package org.apache.doris.catalog.authorizer.openfga;

import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.authorizer.ranger.doris.DorisAccessType;
import org.apache.doris.catalog.authorizer.ranger.doris.DorisObjectType;
import org.apache.doris.common.AuthorizationException;
import org.apache.doris.mysql.privilege.CatalogAccessController;
import org.apache.doris.mysql.privilege.DataMaskPolicy;
import org.apache.doris.mysql.privilege.PrivBitSet;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.Privilege;
import org.apache.doris.mysql.privilege.RowFilterPolicy;
import org.apache.doris.resource.workloadgroup.WorkloadGroupMgr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * A {@link CatalogAccessController} backed by OpenFGA (an open source Zanzibar implementation).
 *
 * <p>Each {@code check*Priv} call maps the wanted {@link PrivPredicate} onto one or more OpenFGA
 * relationship checks over a hierarchical model (catalog -> database -> table -> column, plus
 * global, resource, workload_group, storage_vault, compute_group). This mirrors how
 * {@code RangerDorisAccessController} maps the same calls onto Ranger policy checks: it walks the
 * global -> catalog -> database -> table cascade, accumulates already-satisfied privileges in a
 * {@link PrivBitSet} to avoid rechecking across levels, and returns as soon as the wanted predicate
 * is satisfied.
 *
 * <p>The parent cascade (a grant at the catalog level applying to its databases and tables) is
 * expressed in the OpenFGA authorization model itself through parent relations, so a single Check
 * against a table also sees privileges inherited from its database, catalog, and the global object.
 *
 * <p>MVP scope: OpenFGA is relationship based authorization, not attribute based masking, so
 * {@link #evalDataMaskPolicy} returns empty and {@link #evalRowFilterPolicies} returns an empty
 * list, the same as {@code InternalAccessController} does when no native policy applies. Column
 * masking and row filtering are a later phase.
 */
public class OpenFgaDorisAccessController implements CatalogAccessController {
    private static final Logger LOG = LogManager.getLogger(OpenFgaDorisAccessController.class);

    private final OpenFgaChecker checker;
    private final OpenFgaConfig config;

    public OpenFgaDorisAccessController(OpenFgaChecker checker, OpenFgaConfig config) {
        this.checker = checker;
        this.config = config;
    }

    private String toOpenFgaUser(UserIdentity currentUser) {
        return config.getUserType() + ":" + currentUser.getQualifiedUser();
    }

    private boolean checkPrivilegeByOpenFga(UserIdentity currentUser, DorisAccessType accessType,
            OpenFgaResource resource) {
        String relation = OpenFgaRelation.relationOf(accessType);
        if (relation == null) {
            // No OpenFGA relation for this access type (for example NONE); treat as not allowed.
            return false;
        }
        String object = resource.getObjectString();
        if (object == null) {
            return false;
        }
        OpenFgaCheckRequest request = new OpenFgaCheckRequest(toOpenFgaUser(currentUser), relation, object);
        if (LOG.isDebugEnabled()) {
            LOG.debug("openfga request: {}", request);
        }
        return checker.check(request);
    }

    private boolean checkPrivilege(UserIdentity currentUser, PrivPredicate wanted, OpenFgaResource resource,
            PrivBitSet checkedPrivs) {
        PrivBitSet copy = wanted.getPrivs().copy();
        // avoid duplicate check auth at different levels
        copy.remove(checkedPrivs);
        for (Privilege privilege : copy.toPrivilegeList()) {
            boolean res = checkPrivilegeByOpenFga(currentUser, DorisAccessType.toAccessType(privilege), resource);
            if (res) {
                checkedPrivs.set(privilege.getIdx());
            }
            if (Privilege.satisfy(checkedPrivs, wanted)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean checkGlobalPriv(UserIdentity currentUser, PrivPredicate wanted) {
        PrivBitSet checkedPrivs = PrivBitSet.of();
        return checkGlobalPrivInternal(currentUser, wanted, checkedPrivs);
    }

    private boolean checkGlobalPrivInternal(UserIdentity currentUser, PrivPredicate wanted, PrivBitSet checkedPrivs) {
        OpenFgaResource resource = new OpenFgaResource(DorisObjectType.GLOBAL, config.getObjectSeparator(),
                config.getGlobalObjectId());
        return checkPrivilege(currentUser, wanted, resource, checkedPrivs);
    }

    @Override
    public boolean checkCtlPriv(UserIdentity currentUser, String ctl, PrivPredicate wanted) {
        PrivBitSet checkedPrivs = PrivBitSet.of();
        // The parent cascade is modeled in OpenFGA, but the global object is a separate object type,
        // so it is checked explicitly here just like the Ranger controller does.
        return checkGlobalPrivInternal(currentUser, wanted, checkedPrivs)
                || checkCtlPrivInternal(currentUser, ctl, wanted, checkedPrivs);
    }

    private boolean checkCtlPrivInternal(UserIdentity currentUser, String ctl, PrivPredicate wanted,
            PrivBitSet checkedPrivs) {
        OpenFgaResource resource = new OpenFgaResource(DorisObjectType.CATALOG, config.getObjectSeparator(), ctl);
        return checkPrivilege(currentUser, wanted, resource, checkedPrivs);
    }

    @Override
    public boolean checkDbPriv(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted) {
        PrivBitSet checkedPrivs = PrivBitSet.of();
        return checkGlobalPrivInternal(currentUser, wanted, checkedPrivs)
                || checkCtlPrivInternal(currentUser, ctl, wanted, checkedPrivs)
                || checkDbPrivInternal(currentUser, ctl, db, wanted, checkedPrivs);
    }

    private boolean checkDbPrivInternal(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted,
            PrivBitSet checkedPrivs) {
        OpenFgaResource resource = new OpenFgaResource(DorisObjectType.DATABASE, config.getObjectSeparator(), ctl, db);
        return checkPrivilege(currentUser, wanted, resource, checkedPrivs);
    }

    @Override
    public boolean checkTblPriv(UserIdentity currentUser, String ctl, String db, String tbl, PrivPredicate wanted) {
        PrivBitSet checkedPrivs = PrivBitSet.of();
        return checkGlobalPrivInternal(currentUser, wanted, checkedPrivs)
                || checkCtlPrivInternal(currentUser, ctl, wanted, checkedPrivs)
                || checkDbPrivInternal(currentUser, ctl, db, wanted, checkedPrivs)
                || checkTblPrivInternal(currentUser, ctl, db, tbl, wanted, checkedPrivs);
    }

    private boolean checkTblPrivInternal(UserIdentity currentUser, String ctl, String db, String tbl,
            PrivPredicate wanted, PrivBitSet checkedPrivs) {
        OpenFgaResource resource = new OpenFgaResource(DorisObjectType.TABLE, config.getObjectSeparator(),
                ctl, db, tbl);
        return checkPrivilege(currentUser, wanted, resource, checkedPrivs);
    }

    @Override
    public void checkColsPriv(UserIdentity currentUser, String ctl, String db, String tbl, Set<String> cols,
            PrivPredicate wanted) throws AuthorizationException {
        PrivBitSet checkedPrivs = PrivBitSet.of();
        boolean hasTablePriv = checkGlobalPrivInternal(currentUser, wanted, checkedPrivs)
                || checkCtlPrivInternal(currentUser, ctl, wanted, checkedPrivs)
                || checkDbPrivInternal(currentUser, ctl, db, wanted, checkedPrivs)
                || checkTblPrivInternal(currentUser, ctl, db, tbl, wanted, checkedPrivs);
        if (hasTablePriv) {
            return;
        }

        for (String col : cols) {
            if (!checkColPrivInternal(currentUser, ctl, db, tbl, col, wanted, checkedPrivs.copy())) {
                throw new AuthorizationException(String.format(
                        "Permission denied: user [%s] does not have privilege for [%s] command on [%s].[%s].[%s].[%s]",
                        currentUser, wanted, ctl, db, tbl, col));
            }
        }
    }

    private boolean checkColPrivInternal(UserIdentity currentUser, String ctl, String db, String tbl, String col,
            PrivPredicate wanted, PrivBitSet checkedPrivs) {
        OpenFgaResource resource = new OpenFgaResource(DorisObjectType.COLUMN, config.getObjectSeparator(),
                ctl, db, tbl, col);
        return checkPrivilege(currentUser, wanted, resource, checkedPrivs);
    }

    @Override
    public boolean checkResourcePriv(UserIdentity currentUser, String resourceName, PrivPredicate wanted) {
        PrivBitSet checkedPrivs = PrivBitSet.of();
        return checkGlobalPrivInternal(currentUser, wanted, checkedPrivs)
                || checkResourcePrivInternal(currentUser, resourceName, wanted, checkedPrivs);
    }

    private boolean checkResourcePrivInternal(UserIdentity currentUser, String resourceName, PrivPredicate wanted,
            PrivBitSet checkedPrivs) {
        OpenFgaResource resource = new OpenFgaResource(DorisObjectType.RESOURCE, config.getObjectSeparator(),
                resourceName);
        return checkPrivilege(currentUser, wanted, resource, checkedPrivs);
    }

    @Override
    public boolean checkWorkloadGroupPriv(UserIdentity currentUser, String workloadGroupName, PrivPredicate wanted) {
        // For compatibility with older versions, it is not needed to check the privileges of the default group.
        if (WorkloadGroupMgr.DEFAULT_GROUP_NAME.equals(workloadGroupName)) {
            return true;
        }
        PrivBitSet checkedPrivs = PrivBitSet.of();
        return checkGlobalPrivInternal(currentUser, wanted, checkedPrivs)
                || checkWorkloadGroupInternal(currentUser, workloadGroupName, wanted, checkedPrivs);
    }

    private boolean checkWorkloadGroupInternal(UserIdentity currentUser, String workloadGroupName,
            PrivPredicate wanted, PrivBitSet checkedPrivs) {
        OpenFgaResource resource = new OpenFgaResource(DorisObjectType.WORKLOAD_GROUP, config.getObjectSeparator(),
                workloadGroupName);
        return checkPrivilege(currentUser, wanted, resource, checkedPrivs);
    }

    @Override
    public boolean checkCloudPriv(UserIdentity currentUser, String cloudName, PrivPredicate wanted,
            ResourceTypeEnum type) {
        // Only CLUSTER (compute group) is checked here, matching the Ranger controller.
        // STORAGE_VAULT goes through checkStorageVaultPriv and GENERAL through checkResourcePriv.
        if (!ResourceTypeEnum.CLUSTER.equals(type)) {
            return false;
        }
        PrivBitSet checkedPrivs = PrivBitSet.of();
        return checkGlobalPrivInternal(currentUser, wanted, checkedPrivs)
                || checkComputeGroupPrivInternal(currentUser, cloudName, wanted, checkedPrivs);
    }

    private boolean checkComputeGroupPrivInternal(UserIdentity currentUser, String computeGroupName,
            PrivPredicate wanted, PrivBitSet checkedPrivs) {
        OpenFgaResource resource = new OpenFgaResource(DorisObjectType.COMPUTE_GROUP, config.getObjectSeparator(),
                computeGroupName);
        return checkPrivilege(currentUser, wanted, resource, checkedPrivs);
    }

    @Override
    public boolean checkStorageVaultPriv(UserIdentity currentUser, String storageVaultName, PrivPredicate wanted) {
        PrivBitSet checkedPrivs = PrivBitSet.of();
        return checkGlobalPrivInternal(currentUser, wanted, checkedPrivs)
                || checkStorageVaultPrivInternal(currentUser, storageVaultName, wanted, checkedPrivs);
    }

    private boolean checkStorageVaultPrivInternal(UserIdentity currentUser, String storageVaultName,
            PrivPredicate wanted, PrivBitSet checkedPrivs) {
        OpenFgaResource resource = new OpenFgaResource(DorisObjectType.STORAGE_VAULT, config.getObjectSeparator(),
                storageVaultName);
        return checkPrivilege(currentUser, wanted, resource, checkedPrivs);
    }

    @Override
    public Optional<DataMaskPolicy> evalDataMaskPolicy(UserIdentity currentUser, String ctl, String db, String tbl,
            String col) {
        // OpenFGA is relationship authorization, not attribute masking. No masking in this MVP.
        return Optional.empty();
    }

    @Override
    public List<? extends RowFilterPolicy> evalRowFilterPolicies(UserIdentity currentUser, String ctl, String db,
            String tbl) {
        // OpenFGA is relationship authorization, not row filtering. No row filter in this MVP.
        return Collections.<RowFilterPolicy>emptyList();
    }
}
