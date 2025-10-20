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
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.datasource.property.storage.BrokerProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.datasource.property.storage.exception.StoragePropertiesException;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TFileType;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
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
    @Deprecated
    @SerializedName("cts3")
    private boolean convertedToS3 = false;

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
        this.properties = Maps.newHashMap();
        if (properties != null) {
            this.properties.putAll(properties);
        }
        // Assume the storage type is BROKER by default
        // If it's a multi-load broker, override the storage type to LOCAL
        if (isMultiLoadBroker()) {
            this.storageType = StorageBackend.StorageType.LOCAL;
        } else {
            this.storageType = StorageBackend.StorageType.BROKER;
        }

        // Try to determine the actual storage type from properties if available
        if (MapUtils.isNotEmpty(this.properties)) {
            try {
                // Create primary storage properties from the given configuration
                this.storageProperties = StorageProperties.createPrimary(this.properties);
                // Override the storage type based on property configuration
                this.storageType = StorageBackend.StorageType.valueOf(storageProperties.getStorageName());
            } catch (StoragePropertiesException e) {
                // Currently ignored: these properties might be broker-specific.
                // Just keep the storage type as BROKER, and try to create BrokerProperties
                this.storageProperties = BrokerProperties.of(name, properties);
                this.storageType = StorageBackend.StorageType.BROKER;
            }
        }
        //only storage type is broker
        if (StringUtils.isBlank(this.name) && (this.getStorageType() != StorageType.BROKER)) {
            this.name = this.storageType().name();
        }
    }

    public BrokerDesc(String name, StorageBackend.StorageType storageType, Map<String, String> properties) {
        this.name = name;
        this.properties = Maps.newHashMap();
        this.storageType = storageType;
        if (properties != null) {
            this.properties.putAll(properties);
        }
        if (StorageType.BROKER.equals(storageType)) {
            this.storageProperties = BrokerProperties.of(name, properties);
            return;
        }
        if (MapUtils.isNotEmpty(this.properties) && StorageType.REFACTOR_STORAGE_TYPES.contains(storageType)) {
            this.storageProperties = StorageProperties.createPrimary(properties);
        }

    }

    public String getFileLocation(String location) throws UserException {
        return (null != storageProperties) ? storageProperties.validateAndNormalizeUri(location) : location;
    }

    public static BrokerDesc createForStreamLoad() {
        return new BrokerDesc("", StorageType.STREAM, null);
    }

    public boolean isMultiLoadBroker() {
        return StringUtils.isNotBlank(this.name) && this.name.equalsIgnoreCase(MULTI_LOAD_BROKER);
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
            case OFS:
            case JFS:
            default:
                return TFileType.FILE_BROKER;
        }
    }

    public StorageBackend.StorageType storageType() {
        return storageType;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static BrokerDesc read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), BrokerDesc.class);
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
