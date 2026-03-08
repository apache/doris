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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FEOpExecutorTest extends TestWithFeService {

    @Test
    public void testBuildStmtForwardParamsWithCurrentRoles() throws Exception {
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        connectContext.setCurrentRoles(Sets.newHashSet("role1", "role2"));

        TestableFEOpExecutor executor = new TestableFEOpExecutor(
                new OriginStatement("select 1", 0), connectContext);
        TMasterOpRequest request = executor.buildForwardParams();

        Assertions.assertTrue(request.isSetCurrentRoles());
        Assertions.assertEquals(Sets.newHashSet("role1", "role2"), request.getCurrentRoles());
    }

    @Test
    public void testBuildStmtForwardParamsWithoutCurrentRoles() throws Exception {
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        connectContext.clearCurrentRoles();

        TestableFEOpExecutor executor = new TestableFEOpExecutor(
                new OriginStatement("select 1", 0), connectContext);
        TMasterOpRequest request = executor.buildForwardParams();

        Assertions.assertFalse(request.isSetCurrentRoles());
    }

    private static class TestableFEOpExecutor extends FEOpExecutor {
        TestableFEOpExecutor(OriginStatement originStmt, ConnectContext ctx) {
            super(new TNetworkAddress("127.0.0.1", 9020), originStmt, ctx, false);
        }

        TMasterOpRequest buildForwardParams() throws AnalysisException {
            return super.buildStmtForwardParams();
        }
    }
}
