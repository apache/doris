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

package org.apache.doris.authentication;

/**
 * Authentication plugin type enumeration.
 */
public enum AuthenticationPluginType {
    /** 内置密码认证（MySQL native password） */
    PASSWORD("password", false),

    /** 明文密码认证 */
    CLEAR_PASSWORD("clear_password", true),

    /** LDAP / Active Directory */
    LDAP("ldap", true),

    /** OpenID Connect / OAuth 2.0 */
    OIDC("oidc", false),

    /** Kerberos / GSSAPI */
    KERBEROS("kerberos", false),

    /** X.509 证书认证（mTLS） */
    X509("x509", false),

    /** JWT Token 认证 */
    JWT("jwt", false);

    private final String identifier;
    private final boolean requiresClearPassword;

    AuthenticationPluginType(String identifier, boolean requiresClearPassword) {
        this.identifier = identifier;
        this.requiresClearPassword = requiresClearPassword;
    }

    public String getIdentifier() {
        return identifier;
    }

    public boolean requiresClearPassword() {
        return requiresClearPassword;
    }
}
