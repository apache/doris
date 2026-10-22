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

package org.apache.doris.service;

import org.apache.doris.catalog.Env;
import org.apache.doris.thrift.TLoadTxn2PCRequest;
import org.apache.doris.thrift.TLoadTxnBeginRequest;
import org.apache.doris.thrift.TLoadTxnCommitRequest;
import org.apache.doris.thrift.TLoadTxnRollbackRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class GracefulMasterRedirectTest {
    @Test
    public void testTxnRpcRedirectsBeforeTransactionWork() throws Exception {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.isMaster()).thenReturn(false);
        Mockito.when(env.getMasterHost()).thenReturn("new-master");
        Mockito.when(env.getMasterRpcPort()).thenReturn(9020);
        try (MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            FrontendServiceImpl service = new FrontendServiceImpl(null);
            TNetworkAddress expected = new TNetworkAddress("new-master", 9020);
            var begin = service.loadTxnBegin(new TLoadTxnBeginRequest());
            Assertions.assertEquals(TStatusCode.NOT_MASTER, begin.getStatus().getStatusCode());
            Assertions.assertEquals(expected, begin.getMasterAddress());
            Assertions.assertEquals(expected,
                    service.loadTxnPreCommit(new TLoadTxnCommitRequest()).getMasterAddress());
            Assertions.assertEquals(expected,
                    service.loadTxnCommit(new TLoadTxnCommitRequest()).getMasterAddress());
            Assertions.assertEquals(expected,
                    service.loadTxn2PC(new TLoadTxn2PCRequest()).getMasterAddress());
            Assertions.assertEquals(expected,
                    service.loadTxnRollback(new TLoadTxnRollbackRequest()).getMasterAddress());
        }
    }
}
