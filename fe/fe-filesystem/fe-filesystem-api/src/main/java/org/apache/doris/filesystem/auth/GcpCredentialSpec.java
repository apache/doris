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

package org.apache.doris.filesystem.auth;

import java.util.Objects;
import java.util.Optional;

/**
 * GCP identity description shared by the GCS plugin and the host protocol adapter.
 * Validation of user properties and credential loading belong to the GCS plugin.
 * Null fields mean absent; an empty impersonation account is an explicit ALTER clear.
 */
public final class GcpCredentialSpec implements NativeObjectStorageCredential {
    public enum ProviderType {
        DEFAULT, COMPUTE_ENGINE
    }

    private final ProviderType providerType;
    private final String impersonationServiceAccount;

    public GcpCredentialSpec(ProviderType providerType, String impersonationServiceAccount) {
        this.providerType = providerType;
        this.impersonationServiceAccount = impersonationServiceAccount;
    }

    @Override
    public String getProvider() {
        return "GCP";
    }

    public Optional<ProviderType> getProviderType() {
        return Optional.ofNullable(providerType);
    }

    public Optional<String> getImpersonationServiceAccount() {
        return Optional.ofNullable(impersonationServiceAccount);
    }

    @Override
    public boolean isEmpty() {
        return providerType == null && impersonationServiceAccount == null;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof GcpCredentialSpec)) {
            return false;
        }
        GcpCredentialSpec that = (GcpCredentialSpec) other;
        return providerType == that.providerType
                && Objects.equals(impersonationServiceAccount, that.impersonationServiceAccount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(providerType, impersonationServiceAccount);
    }
}
