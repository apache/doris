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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ShowCreateUserCommandTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
    }

    @Test
    void testHandleShowCreateUser() throws Exception {
        ShowCreateUserCommand sc = new ShowCreateUserCommand(null, true);
        sc.handleShowCreateUser(connectContext, null);

        UserIdentity user = new UserIdentity("test", "127.0.0.1");
        sc = new ShowCreateUserCommand(user, false);
        ShowCreateUserCommand finalSc = sc;
        Assertions.assertThrows(AnalysisException.class, () -> finalSc.handleShowCreateUser(connectContext, null));

        sc = new ShowCreateUserCommand(user, true);
        ShowCreateUserCommand finalSc1 = sc;
        Assertions.assertThrows(AnalysisException.class, () -> finalSc1.handleShowCreateUser(connectContext, null));

        sc = new ShowCreateUserCommand(null, false);
        sc.handleShowCreateUser(connectContext, null);
    }
}
