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
 * Credential type enumeration.
 */
public enum CredentialType {
    MYSQL_NATIVE_PASSWORD,   // MySQL 原生密码（scrambled）
    CLEAR_TEXT_PASSWORD,     // 明文密码
    KERBEROS_TOKEN,          // Kerberos 票据
    OAUTH_TOKEN,             // OAuth/OIDC Access Token
    OIDC_ID_TOKEN,           // OIDC ID Token
    X509_CERTIFICATE,        // X.509 客户端证书
    JWT_TOKEN,               // JWT Token
    SAML_ASSERTION           // SAML 断言
}
