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

    @SerializedName(value = "catalogId")
    private long catalogId = -1L;

    @SerializedName(value = "fromHmsEvent")
    private boolean fromHmsEvent = false;

    // The synced event id of master
    @SerializedName(value = "lastSyncedEventId")
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

    public void addFromCreateDatabaseEvent(String databaseName) {
        MetaIdMapping mapping = new MetaIdMapping((short) 1, (short) 1, databaseName, null, null,
                    ExternalMetaIdMgr.nextMetaId());
        this.metaIdMappings.add(mapping);
    }

    public void addFromCreateTableEvent(String dbName, String tableName) {
        MetaIdMapping mapping = new MetaIdMapping((short) 1, (short) 2, dbName, tableName, null,
                    ExternalMetaIdMgr.nextMetaId());
        this.metaIdMappings.add(mapping);
    }

    public void addFromAddPartitionEvent(String dbName, String tblName, String partitionName) {
        MetaIdMapping mapping = new MetaIdMapping((short) 1, (short) 3, dbName, tblName, partitionName,
                    ExternalMetaIdMgr.nextMetaId());
        this.metaIdMappings.add(mapping);
    }

    public void addFromDropDatabaseEvent(String dbName) {
        MetaIdMapping mapping = new MetaIdMapping((short) 2, (short) 1, dbName, null, null, -1L);
        this.metaIdMappings.add(mapping);
    }

    public void addFromDropTableEvent(String dbName, String tblName) {
        MetaIdMapping mapping = new MetaIdMapping((short) 2, (short) 2, dbName, tblName, null, -1L);
        this.metaIdMappings.add(mapping);
    }

    public void addFromDropPartitionEvent(String dbName, String tblName, String partitionName) {
        MetaIdMapping mapping = new MetaIdMapping((short) 2, (short) 3, dbName, tblName, partitionName, -1L);
        this.metaIdMappings.add(mapping);
    }

    public static OperationType getOperationType(short opType) {
        switch (opType) {
            case 1:
                return OperationType.ADD;
            case 2:
                return OperationType.DELETE;
            default:
                return OperationType.IGNORE;
        }
    }

    public static MetaObjectType getMetaObjectType(short metaObjType) {
        switch (metaObjType) {
            case 1:
                return MetaObjectType.DATABASE;
            case 2:
                return MetaObjectType.TABLE;
            case 3:
                return MetaObjectType.PARTITION;
            default:
                return MetaObjectType.IGNORE;
        }
    }

    @Getter
    public static class MetaIdMapping implements Writable {

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
        @SerializedName(value = "partitionName")
        private String partitionName;
        // id of Database/Table/Partition
        @SerializedName(value = "id")
        private long id;

        public MetaIdMapping() {}

        private MetaIdMapping(short opType,
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

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, GsonUtils.GSON.toJson(this));
        }

        public static MetaIdMapping read(DataInput in) throws IOException {
            String json = Text.readString(in);
            return GsonUtils.GSON.fromJson(json, MetaIdMapping.class);
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
        IGNORE((short) 0),
        // Add a Database/Table/Partition
        ADD((short) 1),
        // Delete Database/Table/Partition
        DELETE((short) 2);

        private final short opType;

        OperationType(short opType) {
            this.opType = opType;
        }

        public short getOperationType() {
            return opType;
        }
    }

    public enum MetaObjectType {
        IGNORE((short) 0),
        DATABASE((short) 1),
        TABLE((short) 2),
        PARTITION((short) 3);

        private final short metaObjType;

        MetaObjectType(short metaObjType) {
            this.metaObjType = metaObjType;
        }

        public short getMetaObjectType() {
            return metaObjType;
        }
    }
}
