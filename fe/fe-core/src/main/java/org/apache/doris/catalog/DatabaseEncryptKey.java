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

package org.apache.doris.catalog;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * user define encryptKey in current db.
 */
public class DatabaseEncryptKey implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(DatabaseEncryptKey.class);
    // user define encryptKey
    // keyName -> encryptKey
    @SerializedName(value = "name2EncryptKey")
    private ConcurrentMap<String, EncryptKey> name2EncryptKey = Maps.newConcurrentMap();

    public ConcurrentMap<String, EncryptKey> getName2EncryptKey() {
        return name2EncryptKey;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static DatabaseEncryptKey read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, DatabaseEncryptKey.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        Preconditions.checkState(name2EncryptKey.getClass() == ConcurrentHashMap.class,
                "name2EncryptKey should be ConcurrentMap, but is " + name2EncryptKey.getClass());
    }
}
