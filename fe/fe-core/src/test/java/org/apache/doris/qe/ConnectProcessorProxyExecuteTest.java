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

package org.apache.doris.qe;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class ConnectProcessorProxyExecuteTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createRole("role1");
    }

    @Test
    public void testProxyExecuteApplyCurrentRoles() throws Exception {
        ConnectContext proxyContext = new ConnectContext(null, true, connectContext.getSessionId());
        TestableConnectProcessor processor = new TestableConnectProcessor(proxyContext);
        Set<String> roles = Sets.newHashSet("role1");

        TMasterOpRequest request = new TMasterOpRequest();
        request.setUser("root");
        request.setDb("");
        request.setSql("show databases");
        request.setCurrentUserIdent(UserIdentity.ROOT.toThrift());
        request.setCurrentRoles(roles);

        processor.proxyExecute(request);
        Assertions.assertEquals(roles, proxyContext.getCurrentRoles());

        // Restore thread local context for subsequent tests.
        connectContext.setThreadLocalInfo();
    }

    private static class TestableConnectProcessor extends ConnectProcessor {
        TestableConnectProcessor(ConnectContext context) {
            super(context);
            connectType = ConnectType.MYSQL;
        }

        @Override
        public void processOnce() {
            // No-op.
        }
    }
}
