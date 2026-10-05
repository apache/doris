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
 * Credential type constants (string-based for extensibility).
 */
public final class CredentialType {

    public static final String MYSQL_NATIVE_PASSWORD = "MYSQL_NATIVE_PASSWORD";
    public static final String CLEAR_TEXT_PASSWORD = "CLEAR_TEXT_PASSWORD";
    public static final String KERBEROS_TOKEN = "KERBEROS_TOKEN";
    public static final String OAUTH_TOKEN = "OAUTH_TOKEN";
    public static final String OIDC_ID_TOKEN = "OIDC_ID_TOKEN";
    public static final String X509_CERTIFICATE = "X509_CERTIFICATE";
    public static final String JWT_TOKEN = "JWT_TOKEN";
    public static final String SAML_ASSERTION = "SAML_ASSERTION";

    private CredentialType() {
    }
}
