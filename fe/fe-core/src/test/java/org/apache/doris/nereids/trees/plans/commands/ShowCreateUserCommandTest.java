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

import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.DomainResolver;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.nereids.trees.plans.commands.info.CreateUserInfo;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class ShowCreateUserCommandTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
    }

    private static final class MockDomainResolver extends DomainResolver {
        public MockDomainResolver(Auth auth) {
            super(auth);
        }

        @Override
        public boolean resolveWithBNS(String domainName, Set<String> resolvedIPs) {
            switch (domainName) {
                case "palo.domain1":
                    resolvedIPs.add("10.1.1.1");
                    resolvedIPs.add("10.1.1.2");
                    resolvedIPs.add("10.1.1.3");
                    break;
                case "palo.domain2":
                    resolvedIPs.add("20.1.1.1");
                    resolvedIPs.add("20.1.1.2");
                    resolvedIPs.add("20.1.1.3");
                    break;
                default:
                    break;
            }
            return true;
        }
    }

    @Test
    void testHandleShowCreateUser() throws Exception {
        UserIdentity user = new UserIdentity("test", "127.0.0.1");
        ShowCreateUserCommand sc = new ShowCreateUserCommand(user);
        ShowCreateUserCommand finalSc = sc;
        Assertions.assertThrows(AnalysisException.class, () -> finalSc.handleShowCreateUser(connectContext, null));

        sc = new ShowCreateUserCommand(user);
        ShowCreateUserCommand finalSc1 = sc;
        Assertions.assertThrows(AnalysisException.class, () -> finalSc1.handleShowCreateUser(connectContext, null));

        // test domain user
        UserIdentity userIdentity = new UserIdentity("zhangsan", "palo.domain1", true);
        UserDesc userDesc = new UserDesc(userIdentity, "12345", true);
        CreateUserInfo info = new CreateUserInfo(true, userDesc, null, null, "");
        CreateUserCommand create = new CreateUserCommand(info);
        try {
            create.run(connectContext, null);
        } catch (UserException e) {
            e.printStackTrace();
        }
        sc = new ShowCreateUserCommand(userIdentity);
        sc.handleShowCreateUser(connectContext, null);
        userIdentity = new UserIdentity("zhangsan", "10.1.1.1", false);
        sc = new ShowCreateUserCommand(userIdentity);
        ShowCreateUserCommand finalSc2 = sc;
        Assertions.assertThrows(AnalysisException.class, () -> finalSc2.handleShowCreateUser(connectContext, null));
    }
}
