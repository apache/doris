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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.datasource.DelegatedCredential;

import org.apache.iceberg.rest.auth.OAuth2Properties;

public final class IcebergDelegatedCredentialUtils {

    private IcebergDelegatedCredentialUtils() {
    }

    public static String credentialKey(DelegatedCredential.Type type) {
        switch (type) {
            case ACCESS_TOKEN:
                return OAuth2Properties.TOKEN;
            case ID_TOKEN:
                return OAuth2Properties.ID_TOKEN_TYPE;
            case JWT:
                return OAuth2Properties.JWT_TOKEN_TYPE;
            case SAML:
                return OAuth2Properties.SAML2_TOKEN_TYPE;
            default:
                throw new IllegalArgumentException("Unsupported delegated credential type: " + type);
        }
    }
}
