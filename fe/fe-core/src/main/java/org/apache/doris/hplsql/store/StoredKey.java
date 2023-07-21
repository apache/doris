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

package org.apache.doris.hplsql.store;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TStoredKey;

import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class StoredKey implements Writable {
    private static final Logger LOG = LogManager.getLogger(StoredKey.class);

    @SerializedName(value = "name")
    private String name;

    @SerializedName(value = "catalogName")
    private String catalogName;

    @SerializedName(value = "dbName")
    private String dbName;

    public StoredKey(String name, String catalogName, String dbName) {
        this.name = name;
        this.catalogName = catalogName;
        this.dbName = dbName;
    }

    public TStoredKey toThrift() {
        return new TStoredKey().setName(name).setCatalogName(catalogName).setDbName(dbName);
    }

    public static StoredKey fromThrift(TStoredKey key) {
        return new StoredKey(key.getName(), key.getCatalogName(), key.getDbName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, catalogName, dbName);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof StoredKey)) {
            return false;
        }
        return Objects.equals(this.name, ((StoredKey) obj).name) && Objects.equals(this.catalogName,
                ((StoredKey) obj).catalogName)
                && Objects.equals(this.dbName, ((StoredKey) obj).dbName);
    }

    @Override
    public String toString() {
        return "name:" + name + ", catalogName:" + catalogName + ", dbName:" + dbName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static StoredKey read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, StoredKey.class);
    }
}
