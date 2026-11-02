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

package org.apache.doris.master;

import org.apache.doris.catalog.Env;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TMasterResult;
import org.apache.doris.thrift.TReportRequest;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class RemoteSpillReportHandlerTest {
    private MockedStatic<Env> mockedEnvStatic;
    private Backend backend;

    @BeforeEach
    public void setUp() {
        backend = new Backend(10001L, "127.0.0.1", 9050);
        SystemInfoService systemInfo = Mockito.mock(SystemInfoService.class);
        Mockito.when(systemInfo.getBackendWithBePort("127.0.0.1", 9060)).thenReturn(backend);
        Mockito.when(systemInfo.getAllBackendIds()).thenReturn(ImmutableList.of(backend.getId()));
        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
    }

    @AfterEach
    public void tearDown() {
        mockedEnvStatic.close();
    }

    private static TReportRequest diskReport() {
        TReportRequest request = new TReportRequest();
        request.setBackend(new TBackend("127.0.0.1", 9060, 8040));
        request.setDisks(Maps.newHashMap());
        return request;
    }

    // The remote spill size sent with a disk report is kept on the backend right away (master FE
    // memory only); a report without it leaves the value unchanged.
    @Test
    public void testDiskReportUpdatesRemoteSpillBytes() throws Exception {
        ReportHandler handler = new ReportHandler();

        TReportRequest request = diskReport();
        request.setRemoteSpillBytes(4096L);
        TMasterResult result = handler.handleReport(request);
        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
        Assertions.assertEquals(4096L, backend.getRemoteSpillBytes());

        request = diskReport();
        request.setRemoteSpillBytes(0L);
        handler.handleReport(request);
        Assertions.assertEquals(0L, backend.getRemoteSpillBytes());

        backend.setRemoteSpillBytes(128L);
        handler.handleReport(diskReport());
        Assertions.assertEquals(128L, backend.getRemoteSpillBytes());
    }
}
