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

package org.apache.doris.storage.protocol;

import org.apache.doris.cloud.proto.Cloud.ObjectStoreCredentialPB;
import org.apache.doris.filesystem.auth.NativeObjectStorageCredential;
import org.apache.doris.thrift.TCredential;

import java.util.Locale;
import java.util.Map;

/**
 * Host-side boundary between identity descriptions and the wire variants understood by Doris.
 * Adding a wire variant requires a codec here and matching receiver support, not a core factory branch
 * or a serialization hook in the filesystem SPI. No user properties or SDK objects enter this module.
 */
public final class ObjectStorageCredentialTranslator {
    private static final Map<String, ObjectStorageCredentialCodec<?>> CODECS = Map.of("GCP", new GcpCredentialCodec());

    private ObjectStorageCredentialTranslator() {
    }

    public static ObjectStoreCredentialPB toCloud(NativeObjectStorageCredential credential) {
        return toCloud(codec(credential.getProvider()), credential);
    }

    public static TCredential toThrift(NativeObjectStorageCredential credential) {
        return toThrift(codec(credential.getProvider()), credential);
    }

    public static NativeObjectStorageCredential fromCloud(String provider, ObjectStoreCredentialPB credential) {
        return codec(provider).fromCloud(credential);
    }

    private static ObjectStorageCredentialCodec<?> codec(String provider) {
        ObjectStorageCredentialCodec<?> codec = CODECS.get(provider.toUpperCase(Locale.ROOT));
        if (codec == null) {
            throw new IllegalArgumentException("Unsupported native credential protocol for provider: " + provider);
        }
        return codec;
    }

    private static <C extends NativeObjectStorageCredential> C checkedCredential(
            ObjectStorageCredentialCodec<C> codec, NativeObjectStorageCredential credential) {
        if (!codec.credentialType().isInstance(credential)) {
            throw new IllegalArgumentException("Unexpected credential description for provider: "
                    + credential.getProvider());
        }
        return codec.credentialType().cast(credential);
    }

    private static <C extends NativeObjectStorageCredential> ObjectStoreCredentialPB toCloud(
            ObjectStorageCredentialCodec<C> codec, NativeObjectStorageCredential credential) {
        return codec.toCloud(checkedCredential(codec, credential));
    }

    private static <C extends NativeObjectStorageCredential> TCredential toThrift(
            ObjectStorageCredentialCodec<C> codec, NativeObjectStorageCredential credential) {
        return codec.toThrift(checkedCredential(codec, credential));
    }
}
