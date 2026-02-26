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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AuthorizationException;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class InternalAccessController implements CatalogAccessController {
    private Auth auth;

    public InternalAccessController(Auth auth) {
        this.auth = auth;
    }

    @Override
    public boolean checkGlobalPriv(PrivilegeContext context, PrivPredicate wanted) {
        return auth.checkGlobalPriv(context, wanted);
    }

    @Override
    public boolean checkCtlPriv(PrivilegeContext context, String ctl, PrivPredicate wanted) {
        return auth.checkCtlPriv(context, ctl, wanted);
    }

    @Override
    public boolean checkDbPriv(PrivilegeContext context, String ctl, String db, PrivPredicate wanted) {
        return auth.checkDbPriv(context, ctl, db, wanted);
    }

    @Override
    public boolean checkTblPriv(PrivilegeContext context, String ctl, String db, String tbl,
                                PrivPredicate wanted) {
        return auth.checkTblPriv(context, ctl, db, tbl, wanted);
    }

    @Override
    public void checkColsPriv(PrivilegeContext context, String ctl, String db, String tbl, Set<String> cols,
                              PrivPredicate wanted) throws AuthorizationException {
        auth.checkColsPriv(context, ctl, db, tbl, cols, wanted);
    }

    @Override
    public boolean checkResourcePriv(PrivilegeContext context, String resourceName, PrivPredicate wanted) {
        return auth.checkResourcePriv(context, resourceName, wanted);
    }

    @Override
    public boolean checkWorkloadGroupPriv(PrivilegeContext context, String workloadGroupName,
                                          PrivPredicate wanted) {
        return auth.checkWorkloadGroupPriv(context, workloadGroupName, wanted);
    }

    @Override
    public boolean checkCloudPriv(PrivilegeContext context, String cloudName,
                                  PrivPredicate wanted, ResourceTypeEnum type) {
        return auth.checkCloudPriv(context, cloudName, wanted, type);
    }

    @Override
    public boolean checkStorageVaultPriv(PrivilegeContext context, String storageVaultName, PrivPredicate wanted) {
        return auth.checkStorageVaultPriv(context, storageVaultName, wanted);
    }

    @Override
    public Optional<DataMaskPolicy> evalDataMaskPolicy(PrivilegeContext context, String ctl, String db,
                                                       String tbl, String col) {
        return Optional.empty();
    }

    @Override
    public List<? extends RowFilterPolicy> evalRowFilterPolicies(PrivilegeContext context, String ctl, String db,
                                                                 String tbl) {
        return Env.getCurrentEnv().getPolicyMgr().getUserPolicies(ctl, db, tbl, context.getCurrentUser());
    }
}
