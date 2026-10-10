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

import java.util.Map;

/**
 * Provider-resolved authentication, with no property parsing or SDK dependencies.
 *
 * <p>The provider identifies the wire credential variant (for example, GCP). Credential map keys
 * are fields of that variant, not user-facing configuration keys. Only protocol adapters interpret
 * these fields; callers use the authentication mode and pass the maps through unchanged.</p>
 */
public final class ObjectStorageAuthentication {
    private final String provider;
    private final boolean anonymous;
    private final Map<String, String> credential;
    private final Map<String, String> credentialUpdates;

    public ObjectStorageAuthentication(String provider, boolean anonymous, Map<String, String> credential,
            Map<String, String> credentialUpdates) {
        this.provider = provider;
        this.anonymous = anonymous;
        this.credential = Map.copyOf(credential);
        this.credentialUpdates = Map.copyOf(credentialUpdates);
    }

    public String getProvider() {
        return provider;
    }

    public boolean isAnonymous() {
        return anonymous;
    }

    public boolean isNative() {
        return !credential.isEmpty();
    }

    public Map<String, String> getCredential() {
        return credential;
    }

    /** Only explicitly supplied native fields, including empty values that clear persisted fields. */
    public Map<String, String> getCredentialUpdates() {
        return credentialUpdates;
    }
}
