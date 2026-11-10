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

package org.apache.doris.connector.iceberg;

import org.apache.doris.thrift.TFileEncryptionInfo;

import org.apache.iceberg.encryption.DorisKeyMetadata;
import org.apache.iceberg.encryption.NativeEncryptionKeyMetadata;
import org.apache.iceberg.util.ByteBuffers;

import java.io.Serializable;
import java.nio.ByteBuffer;

/** Immutable, scan-local file key shared only by the byte-slices of that file. */
final class IcebergFileEncryption implements Serializable {
    private static final long serialVersionUID = 1L;
    private final byte[] key;
    private final byte[] aad;

    private IcebergFileEncryption(NativeEncryptionKeyMetadata metadata) {
        key = ByteBuffers.toByteArray(metadata.encryptionKey());
        aad = ByteBuffers.toByteArray(metadata.aadPrefix());
    }

    static IcebergFileEncryption fromMetadata(ByteBuffer metadata) {
        // A null per-file descriptor denotes plaintext, including files in mixed snapshots.
        return metadata == null ? null : new IcebergFileEncryption(DorisKeyMetadata.parse(metadata));
    }

    TFileEncryptionInfo toThrift() {
        TFileEncryptionInfo info = new TFileEncryptionInfo();
        info.setVersion(1);
        // Thrift binary fields are mutable; one split's serialization must not mutate another's key.
        info.setKey(key.clone());
        info.setAadPrefix(aad.clone());
        return info;
    }
}
