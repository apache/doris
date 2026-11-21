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

package org.apache.doris.filesystem.gcs.auth;



/** Validated GCP identity used by the GCS authentication implementation. */
public final class GcpCredential {
    public static final String CREDENTIAL_PROVIDER_TYPE = "gs.credential_provider_type";
    public static final String IMPERSONATION_SERVICE_ACCOUNT = "gs.impersonation_service_account";

    public static final String OAUTH2_TOKEN = "gs.oauth2.token";
    public static final String OAUTH2_TOKEN_EXPIRES_AT = "gs.oauth2.token-expires-at";
    public static final String ICEBERG_OAUTH2_TOKEN = "gcs.oauth2.token";
    public static final String ICEBERG_OAUTH2_TOKEN_EXPIRES_AT = "gcs.oauth2.token-expires-at";

    private static final String SERVICE_ACCOUNT_PATTERN =
            "^[^@\\s]+@(?:[^@\\s]+\\.iam\\.gserviceaccount\\.com|"
                    + "developer\\.gserviceaccount\\.com|appspot\\.gserviceaccount\\.com)$";

    private final GcpCredentialProviderType providerType;
    private final String impersonationServiceAccount;
    private final String accessToken;
    private final long expiresAtMillis;

    public GcpCredential(GcpCredentialProviderType providerType, String impersonationServiceAccount) {
        this(providerType, impersonationServiceAccount, "", 0);
    }

    public GcpCredential(GcpCredentialProviderType providerType, String impersonationServiceAccount,
            String accessToken, long expiresAtMillis) {
        this.accessToken = accessToken == null ? "" : accessToken;
        this.expiresAtMillis = expiresAtMillis;
        this.providerType = providerType;
        this.impersonationServiceAccount = impersonationServiceAccount == null ? "" : impersonationServiceAccount;
        validate();
    }

    public GcpCredentialProviderType getCredentialProviderType() {
        return providerType;
    }

    public String getImpersonationServiceAccount() {
        return impersonationServiceAccount;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public long getExpiresAtMillis() {
        return expiresAtMillis;
    }

    private void validate() {
        if (providerType == GcpCredentialProviderType.ACCESS_TOKEN) {
            if (accessToken.isEmpty() || accessToken.chars().anyMatch(Character::isWhitespace)) {
                throw new IllegalArgumentException("GCS access token must be nonempty and contain no whitespace");
            }
            if (!impersonationServiceAccount.isEmpty()) {
                throw new IllegalArgumentException("GCS access token cannot be combined with impersonation");
            }
            if (expiresAtMillis < 0) {
                throw new IllegalArgumentException("GCS access token expiration must be positive epoch milliseconds");
            }
        } else if (!accessToken.isEmpty() || expiresAtMillis != 0) {
            throw new IllegalArgumentException("GCS access token requires ACCESS_TOKEN authentication");
        }
        if (providerType == GcpCredentialProviderType.ANONYMOUS) {
            throw new IllegalArgumentException("Anonymous access does not use a native GCP credential");
        }
        if (impersonationServiceAccount.isEmpty()) {
            return;
        }
        if (!impersonationServiceAccount.matches(SERVICE_ACCOUNT_PATTERN)) {
            throw new IllegalArgumentException("Invalid GCP service account email: "
                    + impersonationServiceAccount);
        }
    }

}
