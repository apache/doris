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

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public class KeyManagerStore implements Writable {
    private static final Logger LOG = LogManager.getLogger(KeyManagerStore.class);

    @Setter
    @Getter
    @SerializedName(value = "rootKeyInfo")
    private RootKeyInfo rootKeyInfo;

    @Setter
    @Getter
    @SerializedName(value = "masterKeyMap")
    private TreeMap<Integer, EncryptionKey> masterKeyMap = new TreeMap<>();

    public void writeRootKeyInfo(RootKeyInfo rootKeyInfo) {
    }

    public void setMasterKey(EncryptionKey masterKey) {
        masterKeyMap.put(masterKey.version, masterKey);
    }

    public EncryptionKey getMaxVersionMasterKey() {
        return masterKeyMap.lastEntry().getValue();
    }

    public List<EncryptionKey> getAllVersionMasterKey() {
        return new ArrayList<>(masterKeyMap.values());
    }

    public EncryptionKey getMasterKey(int version) {
        return masterKeyMap.get(version);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static KeyManagerStore read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), KeyManagerStore.class);
    }
}
