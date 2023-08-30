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
// WITH S3/HDFS
// (
//   "username" = "user0",
//   "password" = "password0"
// )
public class BulkDesc implements Writable {
    private static final Logger LOG = LogManager.getLogger(BulkDesc.class);
    private String name;
    protected BulkType type;
    protected Map<String, String> properties;
    private boolean convertedToS3 = false;

    public enum BulkType {
        BROKER,
        S3,
        HDFS,
        LOCAL
    }
    // Only used for recovery

    private BulkDesc() {
        this.properties = Maps.newHashMap();
        this.type = BulkType.BROKER;
    }
    // for empty broker desc

    public BulkDesc(String name) {
        this.name = name;
        this.properties = Maps.newHashMap();
        this.type = BulkType.LOCAL;
    }

    public BulkDesc(String name, Map<String, String> properties) {
        this.name = name;
        this.properties = properties;
        if (this.properties == null) {
            this.properties = Maps.newHashMap();
        }
        this.type = BulkType.BROKER;
        this.properties.putAll(S3ClientBEProperties.getBeFSProperties(this.properties));
    }

    public BulkDesc(String name, BulkType type, Map<String, String> properties) {
        this.name = name;
        this.properties = properties;
        if (this.properties == null) {
            this.properties = Maps.newHashMap();
        }
        this.type = type;
        this.properties.putAll(S3ClientBEProperties.getBeFSProperties(this.properties));
    }


    public TFileType getFileType() {
        switch (type) {
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

    public BulkType getType() {
        return type;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, name);
        properties.put(PersistentFileSystem.STORAGE_TYPE, type.name());
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
        BulkType st = BulkType.BROKER;
        String typeStr = properties.remove(PersistentFileSystem.STORAGE_TYPE);
        if (typeStr != null) {
            try {
                st = BulkType.valueOf(typeStr);
            }  catch (IllegalArgumentException e) {
                LOG.warn("set to BROKER, because of exception", e);
            }
        }
        type = st;
    }

    public static BulkDesc read(DataInput in) throws IOException {
        BulkDesc desc = new BulkDesc();
        desc.readFields(in);
        return desc;
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        if (type == BulkType.BROKER) {
            sb.append("WITH BROKER ").append(name);
        } else {
            sb.append("WITH ").append(type.name());
        }
        if (properties != null && !properties.isEmpty()) {
            PrintableMap<String, String> printableMap = new PrintableMap<>(properties, " = ", true, false, true);
            sb.append(" (").append(printableMap).append(")");
        }
        return sb.toString();
    }
}
