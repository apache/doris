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

package org.apache.doris.httpv2.rest;

import org.apache.doris.catalog.Env;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.tso.TSOService;
import org.apache.doris.tso.TSOTimestamp;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.http.ResponseEntity;

import java.util.Map;

public class TSOActionTest {
    @Test
    public void testDefaultRequestForwardsToMaster() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        TSOAction action = Mockito.spy(new TSOAction());
        Object forwardedResult = new Object();

        Mockito.doReturn(null).when(action).executeCheckPassword(request, response);
        Mockito.doReturn(true).when(action).checkForwardToMaster(request);
        Mockito.doReturn(forwardedResult).when(action).forwardToMaster(request);

        Assertions.assertSame(forwardedResult, action.getTSO(request, response));
        Mockito.verify(action).forwardToMaster(request);
    }

    @Test
    public void testLocalRequestReadsLocalSnapshot() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        TSOAction action = Mockito.spy(new TSOAction());
        Env env = Mockito.mock(Env.class);
        TSOService tsoService = Mockito.mock(TSOService.class);
        long physicalTime = 1_725_000_000_000L;
        long logicalCounter = 17L;
        long currentTso = TSOTimestamp.composeTimestamp(physicalTime, logicalCounter);
        long windowEndPhysicalTime = physicalTime + 5_000L;

        Mockito.when(request.getParameter("local")).thenReturn("true");
        Mockito.doReturn(null).when(action).executeCheckPassword(request, response);
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.when(env.getTSOService()).thenReturn(tsoService);
        Mockito.when(tsoService.getStatusSnapshot()).thenReturn(
                new TSOService.TSOStatusSnapshot(true, currentTso, windowEndPhysicalTime));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            ResponseEntity<?> responseEntity = (ResponseEntity<?>) action.getTSO(request, response);
            ResponseBody<?> responseBody = (ResponseBody<?>) responseEntity.getBody();
            Map<?, ?> data = (Map<?, ?>) responseBody.getData();

            Assertions.assertEquals(windowEndPhysicalTime, data.get("window_end_physical_time"));
            Assertions.assertEquals(currentTso, data.get("current_tso"));
            Assertions.assertEquals(physicalTime, data.get("current_tso_physical_time"));
            Assertions.assertEquals(logicalCounter, data.get("current_tso_logical_counter"));
        }

        Mockito.verify(action, Mockito.never()).checkForwardToMaster(request);
        Mockito.verify(action, Mockito.never()).forwardToMaster(request);
        Mockito.verify(tsoService).getStatusSnapshot();
        Mockito.verify(tsoService, Mockito.never()).getTSO();
        Mockito.verify(tsoService, Mockito.never()).getCurrentTSO();
        Mockito.verify(tsoService, Mockito.never()).getWindowEndTSO();
    }
}
