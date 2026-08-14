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

package org.apache.doris.httpv2.rest.manager;

import org.apache.doris.httpv2.controller.BaseController.ActionAuthorizationInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

class NodeActionTest {
    @Test
    void keepsAnExistingBasicAuthorizationHeader() {
        Assertions.assertEquals("Basic existing",
                NodeAction.resolveAuthorization("Basic existing", new ActionAuthorizationInfo()));
    }

    @Test
    void createsAnInternalBasicHeaderForCookieAuthentication() {
        ActionAuthorizationInfo authInfo = new ActionAuthorizationInfo();
        authInfo.fullUserName = "admin_user";
        authInfo.password = "pass:word";

        String authorization = NodeAction.resolveAuthorization(null, authInfo);
        String decoded = new String(Base64.getDecoder().decode(authorization.substring("Basic ".length())),
                StandardCharsets.UTF_8);

        Assertions.assertEquals("admin_user:pass:word", decoded);
    }

    @Test
    void encodesConfigurationNamesAndValuesForDownstreamRequests() {
        Assertions.assertEquals("name+with+space", NodeAction.encodeQueryParameter("name with space"));
        Assertions.assertEquals("8+%26+4%3D2", NodeAction.encodeQueryParameter("8 & 4=2"));
    }
}
