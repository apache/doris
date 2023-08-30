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

package org.apache.doris.analysis;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.datasource.property.S3ClientBEProperties;
import org.apache.doris.fs.PersistentFileSystem;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TFileType;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
public class BulkStorageDesc implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(BulkStorageDesc.class);
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

    public BulkStorageDesc(String name) {
        this.name = name;
        this.properties = Maps.newHashMap();
        this.storageType = StorageType.LOCAL;
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


    public TFileType getFileType() {
        switch (storageType) {
            case LOCAL:
                return TFileType.FILE_LOCAL;
            case S3:
                return TFileType.FILE_S3;
            case HDFS:
                return TFileType.FILE_HDFS;
            case BROKER:
            default:
                return TFileType.FILE_BROKER;
        }
    }

    public String getFileLocation(String location) {
        return location;
    }

    public StorageType getStorageType() {
        return storageType;
    }

    public void readFields(DataInput in) throws IOException {
        name = Text.readString(in);
        int size = in.readInt();
        properties = Maps.newHashMap();
        for (int i = 0; i < size; ++i) {
            final String key = Text.readString(in);
            final String val = Text.readString(in);
            properties.put(key, val);
        }
        StorageType st = StorageType.BROKER;
        String typeStr = properties.remove(PersistentFileSystem.STORAGE_TYPE);
        if (typeStr != null) {
            try {
                st = StorageType.valueOf(typeStr);
            }  catch (IllegalArgumentException e) {
                LOG.warn("set to BROKER, because of exception", e);
            }
        }
        storageType = st;
    }

    @Override
    public void gsonPostProcess() throws IOException {}

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
