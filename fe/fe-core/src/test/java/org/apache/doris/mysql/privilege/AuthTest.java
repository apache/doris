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

import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

public class AuthTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
    }

    @Test
    public void testMergeRolePriv() throws Exception {
        addUser("u1", true);
        createRole("role1");
        createRole("role2");
        grantPriv("GRANT LOAD_PRIV ON internal.test.* TO ROLE 'role1';");
        grantPriv("GRANT GRANT_PRIV ON internal.test.* TO ROLE 'role2';");

        grantRole("GRANT 'role1','role2' TO 'u1'@'%'");
        Env.getCurrentEnv().getAuth().checkDbPriv(UserIdentity.createAnalyzedUserIdentWithIp("u1", "%"),
                InternalCatalog.INTERNAL_CATALOG_NAME, "test",
                PrivPredicate.of(PrivBitSet.of(Privilege.GRANT_PRIV, Privilege.LOAD_PRIV), Operator.AND));
    }

}
