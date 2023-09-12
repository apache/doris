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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.datasource.property.S3ClientBEProperties;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

// Broker descriptor
//
// Broker example:
// WITH S3/HDFS
// (
//   "username" = "user0",
//   "password" = "password0"
// )
public class BulkStorageDesc implements Writable {
    @SerializedName(value = "name")
    private String name;
    @SerializedName(value = "storageType")
    protected StorageType storageType;
    @SerializedName(value = "properties")
    protected Map<String, String> properties;

    public enum StorageType {
        BROKER,
        S3,
        HDFS,
        LOCAL;
    }

    public BulkStorageDesc(String name, Map<String, String> properties) {
        this.name = name;
        this.properties = properties;
        if (this.properties == null) {
            this.properties = Maps.newHashMap();
        }
        this.storageType = StorageType.BROKER;
        this.properties.putAll(S3ClientBEProperties.getBeFSProperties(this.properties));
    }

    public BulkStorageDesc(String name, StorageType type, Map<String, String> properties) {
        this.name = name;
        this.properties = properties;
        if (this.properties == null) {
            this.properties = Maps.newHashMap();
        }
        this.storageType = type;
        this.properties.putAll(S3ClientBEProperties.getBeFSProperties(this.properties));
    }

    public StorageType getStorageType() {
        return storageType;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static BulkStorageDesc read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, BulkStorageDesc.class);
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        if (storageType == StorageType.BROKER) {
            sb.append("WITH BROKER ").append(name);
        } else {
            sb.append("WITH ").append(storageType.name());
        }
        if (properties != null && !properties.isEmpty()) {
            PrintableMap<String, String> printableMap = new PrintableMap<>(properties, " = ", true, false, true);
            sb.append(" (").append(printableMap).append(")");
        }
        return sb.toString();
    }
}
