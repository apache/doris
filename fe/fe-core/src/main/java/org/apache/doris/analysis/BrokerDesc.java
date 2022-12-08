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

import org.apache.doris.analysis.StorageBackend.StorageType;
import org.apache.doris.backup.BlobStorage;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.thrift.TFileType;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

// Broker descriptor
//
// Broker example:
// WITH BROKER "broker0"
// (
//   "username" = "user0",
//   "password" = "password0"
// )
public class BrokerDesc extends StorageDesc implements Writable {
    private static final Logger LOG = LogManager.getLogger(BrokerDesc.class);

    // just for multi load
    public static final String MULTI_LOAD_BROKER = "__DORIS_MULTI_LOAD_BROKER__";
    public static final String MULTI_LOAD_BROKER_BACKEND_KEY = "__DORIS_MULTI_LOAD_BROKER_BACKEND__";

    // Only used for recovery
    private BrokerDesc() {
        this.properties = Maps.newHashMap();
        this.storageType = StorageBackend.StorageType.BROKER;
    }

    // for empty broker desc
    public BrokerDesc(String name) {
        this.name = name;
        this.properties = Maps.newHashMap();
        this.storageType = StorageType.LOCAL;
    }

    public BrokerDesc(String name, Map<String, String> properties) {
        this.name = name;
        this.properties = properties;
        if (this.properties == null) {
            this.properties = Maps.newHashMap();
        }
        if (isMultiLoadBroker()) {
            this.storageType = StorageBackend.StorageType.LOCAL;
        } else {
            this.storageType = StorageBackend.StorageType.BROKER;
        }
        tryConvertToS3();
    }

    public BrokerDesc(String name, StorageBackend.StorageType storageType, Map<String, String> properties) {
        this.name = name;
        this.properties = properties;
        if (this.properties == null) {
            this.properties = Maps.newHashMap();
        }
        this.storageType = storageType;
        tryConvertToS3();
    }

    public static BrokerDesc createForStreamLoad() {
        BrokerDesc brokerDesc = new BrokerDesc("", StorageType.STREAM, null);
        return brokerDesc;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public StorageBackend.StorageType getStorageType() {
        return storageType;
    }

    public boolean isMultiLoadBroker() {
        return this.name.equalsIgnoreCase(MULTI_LOAD_BROKER);
    }

    public TFileType getFileType() {
        switch (storageType) {
            case LOCAL:
                return TFileType.FILE_LOCAL;
            case S3:
                return TFileType.FILE_S3;
            case HDFS:
                return TFileType.FILE_HDFS;
            case STREAM:
                return TFileType.FILE_STREAM;
            case BROKER:
            default:
                return TFileType.FILE_BROKER;
        }
    }

    public StorageBackend.StorageType storageType() {
        return storageType;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, name);
        properties.put(BlobStorage.STORAGE_TYPE, storageType.name());
        out.writeInt(properties.size());
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }
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
        StorageBackend.StorageType st = StorageBackend.StorageType.BROKER;
        String typeStr = properties.remove(BlobStorage.STORAGE_TYPE);
        if (typeStr != null) {
            try {
                st = StorageBackend.StorageType.valueOf(typeStr);
            }  catch (IllegalArgumentException e) {
                LOG.warn("set to BROKER, because of exception", e);
            }
        }
        storageType = st;
    }

    public static BrokerDesc read(DataInput in) throws IOException {
        BrokerDesc desc = new BrokerDesc();
        desc.readFields(in);
        return desc;
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        if (storageType == StorageBackend.StorageType.BROKER) {
            sb.append("WITH BROKER ").append(name);
        } else {
            sb.append("WITH ").append(storageType.name());
        }
        if (properties != null && !properties.isEmpty()) {
            PrintableMap<String, String> printableMap = new PrintableMap<>(properties, " = ", true, false, true);
            sb.append(" (").append(printableMap.toString()).append(")");
        }
        return sb.toString();
    }
}
