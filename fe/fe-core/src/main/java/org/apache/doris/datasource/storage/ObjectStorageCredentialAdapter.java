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

package org.apache.doris.datasource.storage;

import org.apache.doris.cloud.proto.Cloud.ObjectStoreCredentialPB;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB;
import org.apache.doris.filesystem.auth.NativeObjectStorageCredential;
import org.apache.doris.filesystem.auth.ObjectStorageAuthentication;
import org.apache.doris.storage.protocol.ObjectStorageCredentialTranslator;
import org.apache.doris.thrift.TCredential;
import org.apache.doris.thrift.TObjStorageType;
import org.apache.doris.thrift.TS3StorageParam;

import java.util.Map;
import java.util.Optional;

/** Connects provider-resolved identities to the host protocol adapter without provider-specific logic. */
public final class ObjectStorageCredentialAdapter {
    private final NativeObjectStorageCredential credential;

    private ObjectStorageCredentialAdapter(NativeObjectStorageCredential credential) {
        this.credential = credential;
    }

    public static Optional<ObjectStorageCredentialAdapter> fromAuthentication(
            Optional<ObjectStorageAuthentication> authentication) {
        return authentication.flatMap(ObjectStorageAuthentication::getCredential)
                .map(ObjectStorageCredentialAdapter::fromCredential);
    }

    public static ObjectStorageCredentialAdapter fromCredential(NativeObjectStorageCredential credential) {
        return new ObjectStorageCredentialAdapter(credential);
    }

    public static Map<String, String> toProperties(String provider, ObjectStoreCredentialPB credential) {
        return StorageAdapter.credentialToProperties(ObjectStorageCredentialTranslator.fromCloud(provider, credential));
    }

    public void applyTo(ObjectStoreInfoPB.Builder builder) {
        ObjectStoreCredentialPB encoded = ObjectStorageCredentialTranslator.toCloud(credential);
        if (!builder.hasProvider()) {
            builder.setProvider(ObjectStoreInfoPB.Provider.valueOf(credential.getProvider()));
        }
        builder.setCredential(encoded);
    }

    public void applyTo(TS3StorageParam param) {
        TCredential encoded = ObjectStorageCredentialTranslator.toThrift(credential);
        if (!param.isSetProvider()) {
            param.setProvider(TObjStorageType.valueOf(credential.getProvider()));
        }
        param.setCredential(encoded);
    }
}
