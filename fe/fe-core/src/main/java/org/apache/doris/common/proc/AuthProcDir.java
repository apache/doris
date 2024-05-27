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

package org.apache.doris.common.proc;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.privilege.Auth;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

/*
 * It describes the information about the authorization(privilege) and the authentication(user)
 * SHOW PROC /auth/
 */
public class AuthProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("UserIdentity").add("Comment").add("Password").add("Roles").add("GlobalPrivs").add("CatalogPrivs")
            .add("DatabasePrivs").add("TablePrivs").add("ColPrivs").add("ResourcePrivs").add("CloudClusterPrivs")
            .add("CloudStagePrivs").add("StorageVaultPrivs").add("WorkloadGroupPrivs")
            .build();

    private Auth auth;

    public AuthProcDir(Auth auth) {
        this.auth = auth;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String userIdent) throws AnalysisException {
        if (Strings.isNullOrEmpty(userIdent)) {
            throw new AnalysisException("User is not specified");
        }

        UserIdentity userIdentity = UserIdentity.fromString(userIdent);
        if (userIdentity == null) {
            throw new AnalysisException("Invalid user ident: " + userIdent);
        }

        return new UserPropertyProcNode(auth, userIdentity);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        result.setRows(Env.getCurrentEnv().getAuth().getAuthInfo(null /* get all user */));
        return result;
    }
}
