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

package org.apache.doris.authentication.plugins;

import org.apache.doris.authentication.AuthenticationPluginType;
import org.apache.doris.authentication.AuthenticationProfile;
import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.CredentialType;
import org.apache.doris.authentication.spi.AuthenticationException;
import org.apache.doris.authentication.spi.AuthenticationPlugin;
import org.apache.doris.authentication.spi.AuthenticationResult;

/**
 * Password authentication plugin.
 *
 * <p>Supports MySQL native password and clear text password.
 */
public class PasswordPlugin implements AuthenticationPlugin {

    @Override
    public String name() {
        return "password";
    }

    @Override
    public AuthenticationPluginType type() {
        return AuthenticationPluginType.PASSWORD;
    }

    @Override
    public boolean supports(AuthenticationRequest request) {
        CredentialType type = request.getCredentialType();
        return type == CredentialType.MYSQL_NATIVE_PASSWORD
            || type == CredentialType.CLEAR_TEXT_PASSWORD;
    }

    @Override
    public AuthenticationResult authenticate(AuthenticationRequest request, AuthenticationProfile profile)
            throws AuthenticationException {
        // Implementation will be provided when integrating with existing password authentication
        // This will use existing password validation logic from fe-core
        throw new AuthenticationException("PasswordPlugin is not wired to fe-core yet");
    }
}
