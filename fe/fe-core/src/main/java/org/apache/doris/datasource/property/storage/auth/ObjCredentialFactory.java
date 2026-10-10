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

package org.apache.doris.datasource.property.storage.auth;

import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.filesystem.auth.ObjectStorageAuthentication;

import java.util.Map;
import java.util.Optional;

/** Converts plugin-resolved native credentials to the core wire protocols. */
public final class ObjCredentialFactory {
    private ObjCredentialFactory() {
    }

    public static Optional<ObjCredential> fromProperties(Map<String, String> properties) {
        return fromAuthentication(StorageAdapter.resolveAuthentication(properties));
    }

    public static Optional<ObjCredential> fromAuthentication(Optional<ObjectStorageAuthentication> authentication) {
        return authentication.filter(ObjectStorageAuthentication::isNative)
                .map(auth -> fromCredential(auth.getProvider(), auth.getCredential()));
    }

    public static ObjCredential fromCredential(String provider, Map<String, String> credential) {
        if ("GCP".equals(provider)) {
            return new GcpCredentialAdapter(credential);
        }
        throw new IllegalArgumentException("Unsupported native credential wire format: " + provider);
    }
}
