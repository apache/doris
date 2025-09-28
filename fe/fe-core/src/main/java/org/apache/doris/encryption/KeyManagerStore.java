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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KeyManagerStore implements Writable {
    private static final Logger LOG = LogManager.getLogger(KeyManagerStore.class);

    @SerializedName(value = "rootKeyInfo")
    private RootKeyInfo rootKeyInfo;

    @SerializedName(value = "masterKeys")
    private final List<EncryptionKey> masterKeys = new ArrayList<>();

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    public void addMasterKey(EncryptionKey masterKey) {
        writeLock();
        try {
            masterKeys.add(masterKey);
        } finally {
            writeUnlock();
        }
    }

    public List<EncryptionKey> getMasterKeys() {
        readLock();
        try {
            return masterKeys;
        } finally {
            readUnlock();
        }
    }

    public void setRootKeyInfo(RootKeyInfo info) {
        writeLock();
        try {
            this.rootKeyInfo = info;
        } finally {
            writeUnlock();
        }
    }

    public RootKeyInfo getRootKeyInfo() {
        readLock();
        try {
            return rootKeyInfo;
        } finally {
            readUnlock();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static KeyManagerStore read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), KeyManagerStore.class);
    }
}
