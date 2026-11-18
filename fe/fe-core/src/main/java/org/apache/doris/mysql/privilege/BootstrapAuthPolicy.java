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

package org.apache.doris.mysql.privilege;

import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;

import com.google.common.net.InetAddresses;

import java.net.InetAddress;

/** Client authentication rules; internal execution identities never pass through this policy. */
public final class BootstrapAuthPolicy {
    private static final InetAddress IPV4_LOOPBACK = InetAddresses.forString("127.0.0.1");
    private static final InetAddress IPV6_LOOPBACK = InetAddresses.forString("::1");

    private BootstrapAuthPolicy() {
    }

    public static boolean isBuiltinUser(String userName) {
        return Auth.ROOT_USER.equals(userName) || Auth.ADMIN_USER.equals(userName);
    }

    public static boolean isLocalAddress(String host) {
        if (host == null || !InetAddresses.isInetAddress(host)) {
            return false;
        }
        InetAddress address = InetAddresses.forString(host);
        return IPV4_LOOPBACK.equals(address) || IPV6_LOOPBACK.equals(address);
    }

    public static boolean isRecoveryLogin(String userName, String host, AuthenticationProtocol protocol,
            boolean secureInitialization) {
        if (!Config.skip_localhost_auth_check || !isBuiltinUser(userName)) {
            return false;
        }
        if (!secureInitialization) {
            // Preserve the exact pre-existing behavior on upgraded clusters.
            return "127.0.0.1".equals(host);
        }
        return protocol == AuthenticationProtocol.MYSQL && isLocalAddress(host);
    }

    public static AuthenticationException accessDenied(String userName, String host, boolean suppliedPassword) {
        ErrorCode code = isBuiltinUser(userName) && !suppliedPassword
                ? ErrorCode.ERR_ACCESS_DENIED_EMPTY_PASSWORD : ErrorCode.ERR_ACCESS_DENIED_ERROR;
        return new AuthenticationException(code, userName + "@" + host, suppliedPassword ? "YES" : "NO");
    }

    /** Returns whether the authenticated connection must set its own password before normal SQL. */
    public static boolean check(User user, boolean secureInitialization, String host,
            AuthenticationProtocol protocol, boolean suppliedPassword) throws AuthenticationException {
        String userName = user.getUserIdentity().getQualifiedUser();
        if (!secureInitialization || Config.allow_empty_password_for_root_admin || !isBuiltinUser(userName)
                || user.getPassword().getPassword().length != 0) {
            return false;
        }
        if (Auth.ROOT_USER.equals(userName) && protocol == AuthenticationProtocol.MYSQL && isLocalAddress(host)) {
            return true;
        }
        throw accessDenied(userName, host, suppliedPassword);
    }
}
