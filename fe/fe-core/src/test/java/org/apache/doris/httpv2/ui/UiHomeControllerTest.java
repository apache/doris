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

package org.apache.doris.httpv2.ui;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.httpv2.HttpAuthManager.SessionValue;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;

public class UiHomeControllerTest {
    private HttpServletRequest request;
    private SessionValue session;

    @BeforeEach
    void setUp() {
        session = new SessionValue();
        session.currentUser = UserIdentity.createAnalyzedUserIdentWithIp("operator", "%");
        request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getAttribute(UiRequestContext.SESSION_ATTRIBUTE)).thenReturn(session);
        Mockito.when(request.getAttribute(UiRequestContext.REQUEST_ID_ATTRIBUTE)).thenReturn("req-home");
    }

    @Test
    void versionContainsOnlyTheExistingVersionFields() {
        UiApiResponse<UiVersionInfo> response = new TestController(true, null).version(request);

        Assertions.assertEquals("req-home", response.getRequestId());
        Assertions.assertNotNull(response.getData().getVersion());
        Assertions.assertNotNull(response.getData().getGit());
        Assertions.assertNotNull(response.getData().getBuildInfo());
        Assertions.assertNotNull(response.getData().getBuildTime());
        Assertions.assertNotNull(response.getData().getFeatures());
    }

    @Test
    void nodeFacadePreservesUnknownColumnsAndMissingFields() throws Exception {
        ProcResult procResult = Mockito.mock(ProcResult.class);
        Mockito.when(procResult.getColumnNames()).thenReturn(Arrays.asList("Host", "Alive", "FutureColumn"));
        Mockito.when(procResult.getRows()).thenReturn(Collections.singletonList(Arrays.asList("127.0.0.1", "true")));

        UiApiResponse<UiNodeTable> response = new TestController(true, procResult).frontends(request);

        Assertions.assertEquals(Arrays.asList("Host", "Alive", "FutureColumn"),
                response.getData().getColumnNames());
        Assertions.assertEquals(Arrays.asList("127.0.0.1", "true"), response.getData().getRows().get(0));
    }

    @Test
    void nodeFacadeRequiresNodeStatusCapability() {
        UiApiException exception = Assertions.assertThrows(
                UiApiException.class, () -> new TestController(false, null).backends(request));

        Assertions.assertEquals(403, exception.getStatus().value());
        Assertions.assertEquals("UI_FORBIDDEN", exception.getCode());
        UiCapabilityRequirement details = (UiCapabilityRequirement) exception.getDetails();
        Assertions.assertEquals(UiCapability.NODE_STATUS_VIEW, details.getRequiredCapability());
    }

    private static class TestController extends UiHomeController {
        private final boolean canView;
        private final ProcResult result;

        TestController(boolean canView, ProcResult result) {
            this.canView = canView;
            this.result = result;
        }

        @Override
        protected boolean canViewNodeStatus(SessionValue ignoredSession) {
            return canView;
        }

        @Override
        protected ProcResult fetchNodeResult(String ignoredPath) {
            return result;
        }
    }
}
