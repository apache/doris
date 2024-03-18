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
    public boolean checkGlobalPriv(UserIdentity currentUser, PrivPredicate wanted) {
        return auth.checkGlobalPriv(currentUser, wanted);
    }

    @Override
    public boolean checkCtlPriv(UserIdentity currentUser, String ctl, PrivPredicate wanted) {
        return auth.checkCtlPriv(currentUser, ctl, wanted);
    }

    @Override
    public boolean checkDbPriv(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted) {
        return auth.checkDbPriv(currentUser, ctl, db, wanted);
    }

    @Override
    public boolean checkTblPriv(UserIdentity currentUser, String ctl, String db, String tbl, PrivPredicate wanted) {
        return auth.checkTblPriv(currentUser, ctl, db, tbl, wanted);
    }

    @Override
    public void checkColsPriv(UserIdentity currentUser, String ctl, String db, String tbl, Set<String> cols,
            PrivPredicate wanted) throws AuthorizationException {
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
    public boolean checkCloudPriv(UserIdentity currentUser, String resourceName,
            PrivPredicate wanted, ResourceTypeEnum type) {
        return auth.checkResourcePriv(currentUser, resourceName, wanted);
    }

    @Override
    public Optional<DataMaskPolicy> evalDataMaskPolicy(UserIdentity currentUser, String ctl, String db, String tbl,
            String col) {
        return Optional.empty();
    }

    @Override
    public List<? extends RowFilterPolicy> evalRowFilterPolicies(UserIdentity currentUser, String ctl, String db,
            String tbl) {
        return Env.getCurrentEnv().getPolicyMgr().getUserPolicies(ctl, db, tbl, currentUser);
    }
}
