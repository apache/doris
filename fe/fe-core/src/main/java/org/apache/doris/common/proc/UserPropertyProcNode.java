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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.privilege.Auth;

import com.google.common.collect.ImmutableList;

/*
 * SHOW PROC '/auth/user'
 */
public class UserPropertyProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Key").add("Value")
            .build();

    public static final ImmutableList<String> ALL_USER_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("User").add("Properties")
            .build();

    private Auth auth;
    private UserIdentity userIdent;

    public UserPropertyProcNode(Auth auth, UserIdentity userIdent) {
        this.auth = auth;
        this.userIdent = userIdent;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        result.setRows(auth.getUserProperties(userIdent.getQualifiedUser()));
        result.addRows(auth.getPasswdPolicyInfo(userIdent));
        return result;
    }
}
