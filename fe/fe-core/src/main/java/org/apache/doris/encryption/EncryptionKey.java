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

import org.apache.doris.proto.OlapFile.EncryptionAlgorithmPB;
import org.apache.doris.proto.OlapFile.EncryptionKeyPB;
import org.apache.doris.proto.OlapFile.EncryptionKeyPB.Builder;
import org.apache.doris.proto.OlapFile.EncryptionKeyTypePB;
import org.apache.doris.thrift.TEncryptionAlgorithm;
import org.apache.doris.thrift.TEncryptionKey;

import com.google.gson.annotations.SerializedName;
import com.google.protobuf.ByteString;

public class EncryptionKey {
    public enum Algorithm {
        AES256, SM4;
        public TEncryptionAlgorithm toThrift() {
            switch (this) {
                case AES256:
                    return TEncryptionAlgorithm.AES256;
                case SM4:
                    return TEncryptionAlgorithm.SM4;
                default:
                    throw new RuntimeException("invalid algorithm: " + this);
            }
        }
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

    public boolean isDecrypted() {
        return plaintext != null && plaintext.length > 0;
    }

    public TEncryptionKey toThrift() {
        Builder builder = EncryptionKeyPB.newBuilder();
        builder.setId(id);
        builder.setVersion(version);
        builder.setParentId(parentId);
        builder.setParentVersion(parentVersion);
        switch (algorithm) {
            case AES256:
                builder.setAlgorithm(EncryptionAlgorithmPB.AES_256_CTR);
                break;
            case SM4:
                builder.setAlgorithm(EncryptionAlgorithmPB.SM4_128_CTR);
                break;
            default:
                // do nothing
        }
        switch (type) {
            case DATA_KEY:
                builder.setType(EncryptionKeyTypePB.DATA_KEY);
                break;
            case MASTER_KEY:
                builder.setType(EncryptionKeyTypePB.MASTER_KEY);
                break;
            default:
                // do nothing
        }
        builder.setCiphertextBase64(ciphertext);
        if (isDecrypted()) {
            builder.setPlaintext(ByteString.copyFrom(plaintext));
        }
        builder.setCrc32(crc);
        builder.setCtime(ctime);
        builder.setMtime(mtime);
        EncryptionKeyPB keyPB = builder.build();

        TEncryptionKey tk = new TEncryptionKey();
        tk.setKeyPb(keyPB.toByteArray());
        return tk;
    }

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
}
