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

package org.apache.doris.encryption;

import org.apache.doris.thrift.TEncryptionAlgorithm;
import org.apache.doris.thrift.TEncryptionKey;
import org.apache.doris.thrift.TEncryptionKeyType;

import com.google.gson.annotations.SerializedName;

public class EncryptionKey {
    public enum Algorithm {
        AES256, SM4;
    }

    public enum KeyType {
        MASTER_KEY, DATA_KEY;
    }

    @SerializedName(value = "id")
    public String id;

    @SerializedName(value = "version")
    public int version;

    @SerializedName(value = "parentId")
    public String parentId;
    @SerializedName(value = "parentVersion")
    public int parentVersion;

    @SerializedName(value = "type")
    public KeyType type;

    @SerializedName(value = "algorithm")
    public Algorithm algorithm;
    @SerializedName(value = "ciphertext")
    public String ciphertext;
    // Plaintext cannot stored persistently
    public byte[] plaintext;

    @SerializedName(value = "iv")
    public String iv;

    @SerializedName(value = "crc")
    public long crc;

    @SerializedName(value = "ctime")
    public long ctime;

    @SerializedName(value = "mtime")
    public long mtime;

    @Override
    public String toString() {
        return "EncryptionKey{"
            + "id='" + id + '\'' + ", version=" + version + ", parentId='" + parentId + '\''
            + ", parentVersion=" + parentVersion
            + ", type=" + type + ", algorithm=" + algorithm
            + ", ciphertext(Base64)=" + (ciphertext != null ? ciphertext : "null")
            + ", iv(Base64)=" + (iv != null ? iv : "null")
            + ", crc=" + crc
            + ", ctime=" + ctime
            + ", mtime=" + mtime + '}';
    }

    public TEncryptionKey toThrift() {
        TEncryptionKey tKey = new TEncryptionKey();
        tKey.setId(this.id);
        tKey.setVersion(this.version);
        tKey.setParentId(this.parentId);
        tKey.setParentVersion(this.parentVersion);

        // Convert algorithm enum
        if (this.algorithm == EncryptionKey.Algorithm.AES256) {
            tKey.setAlgorithm(TEncryptionAlgorithm.AES256);
        } else if (this.algorithm == EncryptionKey.Algorithm.SM4) {
            tKey.setAlgorithm(TEncryptionAlgorithm.SM4);
        } else {
            throw new IllegalArgumentException("Unknown algorithm: " + this.algorithm);
        }

        if (this.type == KeyType.MASTER_KEY) {
            tKey.setType(TEncryptionKeyType.MASTER_KEY);
        } else if (this.type == KeyType.DATA_KEY) {
            tKey.setType(TEncryptionKeyType.DATA_KEY);
        } else {
            throw new IllegalArgumentException("Unknown key type: " + this.type);
        }

        tKey.setCiphertext(this.ciphertext);
        tKey.setPlaintext(this.plaintext);
        tKey.setIv(this.iv);
        tKey.setCrc(this.crc);
        tKey.setCtime(this.ctime);
        tKey.setMtime(this.mtime);

        return tKey;
    }
}
