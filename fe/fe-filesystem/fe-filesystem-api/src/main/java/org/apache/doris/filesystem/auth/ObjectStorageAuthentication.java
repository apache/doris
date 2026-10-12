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

/** Provider-resolved authentication, independent of user property aliases, SDKs and wire formats. */
public final class ObjectStorageAuthentication {
    private final String provider;
    private final boolean anonymous;
    private final NativeObjectStorageCredential credential;
    private final NativeObjectStorageCredential credentialUpdates;

    private ObjectStorageAuthentication(String provider, boolean anonymous,
            NativeObjectStorageCredential credential, NativeObjectStorageCredential credentialUpdates) {
        this.provider = Objects.requireNonNull(provider, "provider");
        this.anonymous = anonymous;
        this.credential = credential;
        this.credentialUpdates = credentialUpdates;
    }

    public static ObjectStorageAuthentication nativeAuthentication(NativeObjectStorageCredential credential,
            NativeObjectStorageCredential updates) {
        Objects.requireNonNull(credential, "credential");
        Objects.requireNonNull(updates, "updates");
        if (credential.isEmpty()) {
            throw new IllegalArgumentException("Resolved native credentials must not be empty");
        }
        if (!credential.getProvider().equals(updates.getProvider())) {
            throw new IllegalArgumentException("Credential and ALTER patch must identify the same provider");
        }
        return new ObjectStorageAuthentication(credential.getProvider(), false, credential,
                updates.isEmpty() ? null : updates);
    }

    public static ObjectStorageAuthentication hmac(String provider) {
        return new ObjectStorageAuthentication(provider, false, null, null);
    }

    public static ObjectStorageAuthentication anonymous(String provider) {
        return new ObjectStorageAuthentication(provider, true, null, null);
    }

    public String getProvider() {
        return provider;
    }

    public boolean isAnonymous() {
        return anonymous;
    }

    public boolean isNative() {
        return credential != null;
    }

    public Optional<NativeObjectStorageCredential> getCredential() {
        return Optional.ofNullable(credential);
    }

    /** Only explicitly supplied native fields, including empty values that clear persisted fields. */
    public Optional<NativeObjectStorageCredential> getCredentialUpdates() {
        return Optional.ofNullable(credentialUpdates);
    }
}
