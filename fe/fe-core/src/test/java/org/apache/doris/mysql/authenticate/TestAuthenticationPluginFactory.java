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

package org.apache.doris.mysql.authenticate;

import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.AuthenticationResult;
import org.apache.doris.authentication.BasicPrincipal;
import org.apache.doris.authentication.CredentialType;
import org.apache.doris.authentication.spi.AuthenticationPlugin;
import org.apache.doris.authentication.spi.AuthenticationPluginFactory;

public class TestAuthenticationPluginFactory implements AuthenticationPluginFactory {
    @Override
    public String name() {
        return "test_plugin";
    }

    @Override
    public AuthenticationPlugin create() {
        return new TestAuthenticationPlugin();
    }

    private static class TestAuthenticationPlugin implements AuthenticationPlugin {
        @Override
        public String name() {
            return "test_plugin";
        }

        @Override
        public boolean supports(AuthenticationRequest request) {
            return CredentialType.CLEAR_TEXT_PASSWORD.equalsIgnoreCase(request.getCredentialType());
        }

        @Override
        public boolean requiresClearPassword() {
            return true;
        }

        @Override
        public AuthenticationResult authenticate(AuthenticationRequest request, AuthenticationIntegration integration) {
            return AuthenticationResult.success(BasicPrincipal.builder()
                    .name(request.getUsername())
                    .authenticator(integration.getType())
                    .build());
        }
    }
}
