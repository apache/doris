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

package org.apache.doris.utframe;

import org.apache.doris.analysis.CreateUserStmt;
import org.apache.doris.analysis.GrantStmt;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AccessPrivilegeWithCols;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class MockUsers {
    public static UserIdentity getUserWithNoAllPrivilege() throws UserException {
        UserIdentity user = new UserIdentity("user_with_no_all_privilege", "%");
        TablePattern tablePattern = new TablePattern("*", "*", "*");
        List<AccessPrivilegeWithCols> privileges = ImmutableList.of();
        createUser(user, tablePattern, privileges);
        return user;
    }

    private static void createUser(UserIdentity userIdentity, TablePattern tablePattern,
            List<AccessPrivilegeWithCols> privileges) throws UserException {
        userIdentity.analyze();
        tablePattern.analyze();
        CreateUserStmt createUserStmt = new CreateUserStmt(new UserDesc(userIdentity));
        Env.getCurrentEnv().getAuth().createUser(createUserStmt);
        GrantStmt grantStmt = new GrantStmt(userIdentity, null, tablePattern, privileges);
        Env.getCurrentEnv().getAuth().grant(grantStmt);
    }

    public static void changeUser(UserIdentity user) {
        ConnectContext.get().setCurrentUserIdentity(user);
    }
}
