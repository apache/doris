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

package org.apache.doris.rpc;

import org.apache.doris.thrift.TFileEncryptionInfo;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

/** Wire validation shared by scan planning and dispatch; never formats secret payloads. */
public final class FileScanEncryption {
    public static final int VERSION = 1;

    private FileScanEncryption() {
    }

    public static void validateInfo(TFileEncryptionInfo info) {
        if (!info.isSetVersion()) {
            throw new IllegalArgumentException("File encryption [INVALID_DESCRIPTOR]");
        }
        if (info.getVersion() != VERSION) {
            throw new IllegalArgumentException("File encryption [UNSUPPORTED_VERSION]");
        }
        // Generated binary accessors copy key bytes; validation only needs the existing buffer length.
        int keyLength = info.isSetKey() ? info.key.remaining() : 0;
        if ((keyLength != 16 && keyLength != 24 && keyLength != 32) || !info.isSetAadPrefix()) {
            throw new IllegalArgumentException("File encryption [INVALID_DESCRIPTOR]");
        }
    }

    /** Returns true for data encryption or any independently encrypted delete file. */
    public static boolean validate(TTableFormatFileDesc table) {
        if (!table.isSetIcebergParams()) {
            return false;
        }
        TIcebergFileDesc iceberg = table.getIcebergParams();
        boolean encrypted = iceberg.isSetEncryptionInfo();
        if (encrypted) {
            validateInfo(iceberg.getEncryptionInfo());
        }
        if (iceberg.isSetDeleteFiles()) {
            for (TIcebergDeleteFileDesc delete : iceberg.getDeleteFiles()) {
                if (delete.isSetEncryptionInfo()) {
                    validateInfo(delete.getEncryptionInfo());
                    encrypted = true;
                }
            }
        }
        return encrypted;
    }
}
