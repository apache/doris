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
import org.apache.doris.authorization.RowFilterSpec;
import org.apache.doris.common.AuthorizationException;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Decides access to the resources of one catalog, one kind of object at a time.
 *
 * <p>A controller is asked only about the resources it governs. The engine no longer establishes a global
 * privilege before routing, but an implementation of this interface is not thereby deprived of the one it
 * used to be handed: its scoped methods came in pairs, {@code checkDbPriv(boolean hasGlobal, ...)} in front
 * of {@code checkDbPriv(...)}, and {@link LegacyAccessControllerPlugin} still asks whoever governs instance
 * scope and grants on a yes, exactly as those default methods did. An implementation of the current contract
 * decides that exemption for itself instead - as the Ranger sources do, deferring to whichever source owns
 * global scope ({@code org.apache.doris.catalog.authorizer.ranger.RangerAccessController}, in the ranger
 * plugin and so not on this module's class path).
 *
 * <p>This is the older shape of that contract, kept because a catalog's {@code access_controller.class}
 * names an implementation of it and such implementations exist outside this repository. The engine reaches
 * one through {@link LegacyAccessControllerPlugin}; a source written today implements
 * {@link org.apache.doris.authorization.spi.AuthorizationPlugin} instead, which asks a single question about
 * a typed resource and answers by refusing rather than by returning false.
 *
 * <p><b>An implementation with data policies must be recompiled.</b> {@link #evalDataMaskPolicy} and
 * {@link #evalRowFilterPolicies} used to answer with {@code org.apache.doris.mysql.privilege.DataMaskPolicy}
 * and {@code org.apache.doris.mysql.privilege.RowFilterPolicy}; both types are gone, replaced by
 * {@link org.apache.doris.authorization.DataMaskSpec} and {@link org.apache.doris.authorization.RowFilterSpec}.
 * Both signatures erase to {@code Optional} and {@code List}, so a controller compiled against the old types
 * still loads and still answers every {@code check*Priv} - and then fails the first time a query reaches a
 * table it holds a policy for. How it fails depends on what the method body touches: one that names a deleted
 * type fails with {@code NoClassDefFoundError}, while one that only passes objects through - which is what
 * the reference implementation in this repository did, returning {@code PolicyMgr.getUserPolicies(...)}
 * verbatim - runs to completion and is refused by {@link LegacyAccessControllerPlugin} on the way out, with
 * an {@code IllegalStateException} naming this source and this notice. Both methods have always been abstract
 * here, so every implementation has one of each; what is unaffected is a controller whose two answers are
 * always empty, since nothing it returns names either type.
 *
 * <p><b>Comparing a {@link PrivPredicate} with {@code ==} holds only for the questions the engine asks by
 * name.</b> Those are handed over as the very constant the caller named - including {@code SHOW_RESOURCES}
 * and {@code SHOW_WORKLOAD_GROUP}, which name the same actions and so are one value, told apart by the
 * requirement object the engine derived from each. A check built for a single statement, on the other hand -
 * granting a privilege requires holding both it and the right to grant it - translates back to a predicate
 * built to match, equal to no constant and identical to none. Read {@link PrivPredicate#getPrivs()} and
 * {@link PrivPredicate#getOp()} to recognise those.
 *
 * @deprecated implement {@link org.apache.doris.authorization.spi.AuthorizationPlugin} instead. This
 *         interface still works and is still what {@code access_controller.class} may name, but it is not
 *         held to a plugin API version, so nothing detects when a Doris upgrade changes what it means.
 */
@Deprecated
public interface CatalogAccessController {
    default void close() {
    }

    // ==== Global ====
    boolean checkGlobalPriv(UserIdentity currentUser, PrivPredicate wanted);

    // ==== Catalog ====
    boolean checkCtlPriv(UserIdentity currentUser, String ctl, PrivPredicate wanted);

    // ==== Database ====
    boolean checkDbPriv(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted);

    // ==== Table ====
    boolean checkTblPriv(UserIdentity currentUser, String ctl, String db, String tbl, PrivPredicate wanted);

    // ==== Resource ====
    boolean checkResourcePriv(UserIdentity currentUser, String resourceName, PrivPredicate wanted);

    // ==== Workload Group ====
    boolean checkWorkloadGroupPriv(UserIdentity currentUser, String workloadGroupName, PrivPredicate wanted);

    void checkColsPriv(UserIdentity currentUser, String ctl, String db, String tbl,
            Set<String> cols, PrivPredicate wanted) throws AuthorizationException;

    // ==== Cloud ====
    boolean checkCloudPriv(UserIdentity currentUser, String cloudName, PrivPredicate wanted, ResourceTypeEnum type);

    boolean checkStorageVaultPriv(UserIdentity currentUser, String storageVaultName, PrivPredicate wanted);

    /**
     * How {@code col} must be rewritten before {@code currentUser} may read it, or empty when it is not masked.
     * The returned payload carries a SQL expression, never a parsed one: see {@link DataMaskSpec}.
     */
    Optional<DataMaskSpec> evalDataMaskPolicy(UserIdentity currentUser, String ctl, String db, String tbl,
            String col);

    /**
     * The row-level filters that apply to {@code tbl} for {@code currentUser}, empty when there are none.
     * The engine combines them per {@link org.apache.doris.authorization.RowFilterMergeType}.
     */
    List<RowFilterSpec> evalRowFilterPolicies(UserIdentity currentUser, String ctl, String db, String tbl);
}
