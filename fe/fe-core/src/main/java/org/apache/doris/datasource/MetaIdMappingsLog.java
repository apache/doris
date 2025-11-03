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

package org.apache.doris.datasource;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.Getter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@Data
public class MetaIdMappingsLog implements Writable {

    public static final short OPERATION_TYPE_IGNORE = 0;
    public static final short OPERATION_TYPE_ADD = 1;
    public static final short OPERATION_TYPE_DELETE = 2;

    public static final short META_OBJECT_TYPE_IGNORE = 0;
    public static final short META_OBJECT_TYPE_DATABASE = 1;
    public static final short META_OBJECT_TYPE_TABLE = 2;
    public static final short META_OBJECT_TYPE_PARTITION = 3;

    @SerializedName(value = "ctlId")
    private long catalogId = -1L;

    @SerializedName(value = "fromEvent")
    private boolean fromHmsEvent = false;

    // The synced event id of master
    @SerializedName(value = "lastEventId")
    private long lastSyncedEventId = -1L;

    @SerializedName(value = "metaIdMappings")
    private List<MetaIdMapping> metaIdMappings = Lists.newLinkedList();

    public MetaIdMappingsLog() {
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogId, lastSyncedEventId,
                    metaIdMappings == null ? 0 : Arrays.hashCode(metaIdMappings.toArray()));
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MetaIdMappingsLog)) {
            return false;
        }
        return Objects.equals(this.catalogId, ((MetaIdMappingsLog) obj).catalogId)
                    && Objects.equals(this.fromHmsEvent, ((MetaIdMappingsLog) obj).fromHmsEvent)
                    && Objects.equals(this.lastSyncedEventId, ((MetaIdMappingsLog) obj).lastSyncedEventId)
                    && Objects.equals(this.metaIdMappings, ((MetaIdMappingsLog) obj).metaIdMappings);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static MetaIdMappingsLog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, MetaIdMappingsLog.class);
    }

    public void addMetaIdMapping(MetaIdMapping metaIdMapping) {
        this.metaIdMappings.add(metaIdMapping);
    }

    public void addMetaIdMappings(List<MetaIdMapping> metaIdMappings) {
        this.metaIdMappings.addAll(metaIdMappings);
    }

    public static OperationType getOperationType(short opType) {
        switch (opType) {
            case OPERATION_TYPE_ADD:
                return OperationType.ADD;
            case OPERATION_TYPE_DELETE:
                return OperationType.DELETE;
            default:
                return OperationType.IGNORE;
        }
    }

    public static MetaObjectType getMetaObjectType(short metaObjType) {
        switch (metaObjType) {
            case META_OBJECT_TYPE_DATABASE:
                return MetaObjectType.DATABASE;
            case META_OBJECT_TYPE_TABLE:
                return MetaObjectType.TABLE;
            case META_OBJECT_TYPE_PARTITION:
                return MetaObjectType.PARTITION;
            default:
                return MetaObjectType.IGNORE;
        }
    }

    @Getter
    public static class MetaIdMapping {

        @SerializedName(value = "opType")
        private short opType;
        @SerializedName(value = "metaObjType")
        private short metaObjType;
        // name of Database
        @SerializedName(value = "dbName")
        private String dbName;
        // name of Table
        @SerializedName(value = "tblName")
        private String tblName;
        // name of Partition
        @SerializedName(value = "pName")
        private String partitionName;
        // id of Database/Table/Partition
        @SerializedName(value = "id")
        private long id;

        public MetaIdMapping() {}

        public MetaIdMapping(short opType,
                             short metaObjType,
                             String dbName,
                             String tblName,
                             String partitionName,
                             long id) {
            this.opType = opType;
            this.metaObjType = metaObjType;
            this.dbName = dbName;
            this.tblName = tblName;
            this.partitionName = partitionName;
            this.id = id;
        }

        public MetaIdMapping(short opType,
                             short metaObjType,
                             String dbName,
                             String tblName,
                             String partitionName) {
            this.opType = opType;
            this.metaObjType = metaObjType;
            this.dbName = dbName;
            this.tblName = tblName;
            this.partitionName = partitionName;
            this.id = -1L;
        }

        public MetaIdMapping(short opType,
                             short metaObjType,
                             String dbName,
                             String tblName,
                             long id) {
            this.opType = opType;
            this.metaObjType = metaObjType;
            this.dbName = dbName;
            this.tblName = tblName;
            this.partitionName = null;
            this.id = id;
        }

        public MetaIdMapping(short opType,
                             short metaObjType,
                             String dbName,
                             String tblName) {
            this.opType = opType;
            this.metaObjType = metaObjType;
            this.dbName = dbName;
            this.tblName = tblName;
            this.partitionName = null;
            this.id = -1L;
        }

        public MetaIdMapping(short opType,
                             short metaObjType,
                             String dbName,
                             long id) {
            this.opType = opType;
            this.metaObjType = metaObjType;
            this.dbName = dbName;
            this.tblName = null;
            this.partitionName = null;
            this.id = id;
        }

        public MetaIdMapping(short opType,
                             short metaObjType,
                             String dbName) {
            this.opType = opType;
            this.metaObjType = metaObjType;
            this.dbName = dbName;
            this.tblName = null;
            this.partitionName = null;
            this.id = -1L;
        }

        @Override
        public int hashCode() {
            return Objects.hash(opType, metaObjType, dbName, tblName, partitionName, id);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof MetaIdMapping)) {
                return false;
            }
            return Objects.equals(this.opType, ((MetaIdMapping) obj).opType)
                        && Objects.equals(this.metaObjType, ((MetaIdMapping) obj).metaObjType)
                        && Objects.equals(this.dbName, ((MetaIdMapping) obj).dbName)
                        && Objects.equals(this.tblName, ((MetaIdMapping) obj).tblName)
                        && Objects.equals(this.partitionName, ((MetaIdMapping) obj).partitionName)
                        && Objects.equals(this.id, ((MetaIdMapping) obj).id);
        }

    }

    public enum OperationType {
        IGNORE(OPERATION_TYPE_IGNORE),
        // Add a Database/Table/Partition
        ADD(OPERATION_TYPE_ADD),
        // Delete Database/Table/Partition
        DELETE(OPERATION_TYPE_DELETE);

        private final short opType;

        OperationType(short opType) {
            this.opType = opType;
        }

        public short getOperationType() {
            return opType;
        }
    }

    public enum MetaObjectType {
        IGNORE(META_OBJECT_TYPE_IGNORE),
        DATABASE(META_OBJECT_TYPE_DATABASE),
        TABLE(META_OBJECT_TYPE_TABLE),
        PARTITION(META_OBJECT_TYPE_PARTITION);

        private final short metaObjType;

        MetaObjectType(short metaObjType) {
            this.metaObjType = metaObjType;
        }

        public short getMetaObjectType() {
            return metaObjType;
        }
    }
}
