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
import org.apache.doris.filesystem.auth.ObjectStorageAuthentication;
import org.apache.doris.filesystem.spi.ObjectStorageCredentialCodec;
import org.apache.doris.thrift.TCredential;
import org.apache.doris.thrift.TObjStorageType;
import org.apache.doris.thrift.TS3StorageParam;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/** Transfers plugin-encoded credentials into core wire envelopes without interpreting provider fields. */
public final class ObjectStorageCredentialAdapter {
    private final ObjectStorageCredentialCodec codec;
    private final Map<String, String> fields;

    private ObjectStorageCredentialAdapter(ObjectStorageCredentialCodec codec, Map<String, String> fields) {
        this.codec = codec;
        this.fields = Map.copyOf(fields);
    }

    public static Optional<ObjectStorageCredentialAdapter> fromAuthentication(
            Optional<ObjectStorageAuthentication> authentication) {
        return authentication.filter(ObjectStorageAuthentication::isNative)
                .map(auth -> fromCredential(auth.getProvider(), auth.getCredential()));
    }

    public static ObjectStorageCredentialAdapter fromCredential(String provider, Map<String, String> fields) {
        return new ObjectStorageCredentialAdapter(StorageAdapter.credentialCodec(provider), fields);
    }

    public static Map<String, String> toProperties(String provider, ObjectStoreCredentialPB credential) {
        try {
            return StorageAdapter.credentialCodec(provider).toProperties(credential.toByteArray());
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot decode native object-storage credentials", e);
        }
    }

    public void applyTo(ObjectStoreInfoPB.Builder builder) {
        try {
            ObjectStoreCredentialPB credential = ObjectStoreCredentialPB.parseFrom(codec.toCloudCredential(fields));
            if (!builder.hasProvider()) {
                builder.setProvider(ObjectStoreInfoPB.Provider.valueOf(codec.provider()));
            }
            builder.setCredential(credential);
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot encode native object-storage credentials for Cloud", e);
        }
    }

    public void applyTo(TS3StorageParam param) {
        try {
            TCredential credential = new TCredential();
            new TDeserializer(new TBinaryProtocol.Factory()).deserialize(credential, codec.toThriftCredential(fields));
            if (!param.isSetProvider()) {
                param.setProvider(TObjStorageType.valueOf(codec.provider()));
            }
            param.setCredential(credential);
        } catch (IOException | TException e) {
            throw new IllegalArgumentException("Cannot encode native object-storage credentials for BE", e);
        }
    }
}
