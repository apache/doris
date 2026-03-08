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
import org.apache.doris.common.AuthorizationException;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface CatalogAccessController {
    // ==== Catalog ====
    default boolean checkCtlPriv(boolean hasGlobal, PrivilegeContext context, String ctl, PrivPredicate wanted) {
        boolean res = checkCtlPriv(context, ctl, wanted);
        return hasGlobal || res;
    }

    // ==== Global ====
    boolean checkGlobalPriv(PrivilegeContext context, PrivPredicate wanted);

    // ==== Catalog ====
    boolean checkCtlPriv(PrivilegeContext context, String ctl, PrivPredicate wanted);

    // ==== Database ====
    default boolean checkDbPriv(boolean hasGlobal, PrivilegeContext context, String ctl, String db,
                                PrivPredicate wanted) {
        boolean res = checkDbPriv(context, ctl, db, wanted);
        return hasGlobal || res;
    }

    boolean checkDbPriv(PrivilegeContext context, String ctl, String db, PrivPredicate wanted);

    // ==== Table ====
    default boolean checkTblPriv(boolean hasGlobal, PrivilegeContext context, String ctl, String db, String tbl,
                                 PrivPredicate wanted) {
        boolean res = checkTblPriv(context, ctl, db, tbl, wanted);
        return hasGlobal || res;
    }

    boolean checkTblPriv(PrivilegeContext context, String ctl, String db, String tbl, PrivPredicate wanted);

    // ==== Column ====
    default void checkColsPriv(boolean hasGlobal, PrivilegeContext context, String ctl, String db, String tbl,
                               Set<String> cols, PrivPredicate wanted) throws AuthorizationException {
        try {
            checkColsPriv(context, ctl, db, tbl, cols, wanted);
        } catch (AuthorizationException e) {
            if (!hasGlobal) {
                throw e;
            }
        }
    }

    // ==== Resource ====
    boolean checkResourcePriv(PrivilegeContext context, String resourceName, PrivPredicate wanted);

    // ==== Workload Group ====
    boolean checkWorkloadGroupPriv(PrivilegeContext context, String workloadGroupName, PrivPredicate wanted);

    void checkColsPriv(PrivilegeContext context, String ctl, String db, String tbl,
                       Set<String> cols, PrivPredicate wanted) throws AuthorizationException;

    // ==== Cloud ====
    boolean checkCloudPriv(PrivilegeContext context, String cloudName, PrivPredicate wanted, ResourceTypeEnum type);

    boolean checkStorageVaultPriv(PrivilegeContext context, String storageVaultName, PrivPredicate wanted);

    Optional<DataMaskPolicy> evalDataMaskPolicy(PrivilegeContext context, String ctl, String db, String tbl,
                                                String col);

    List<? extends RowFilterPolicy> evalRowFilterPolicies(PrivilegeContext context, String ctl, String db,
                                                          String tbl);
}
