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

package org.apache.doris.cdcclient.source.deserialize;

import org.apache.doris.cdcclient.utils.SchemaChangeOperation;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Result of deserializing a SourceRecord. */
public class DeserializeResult {
    private static final Logger LOG = LoggerFactory.getLogger(DeserializeResult.class);

    public enum Type {
        DML,
        SCHEMA_CHANGE,
        EMPTY
    }

    private final Type type;
    private final List<String> records;
    private final List<SchemaChangeOperation> schemaChanges;
    private final Map<TableId, TableChanges.TableChange> updatedSchemas;
    private final String unsupportedReason;

    private DeserializeResult(
            Type type,
            List<String> records,
            List<SchemaChangeOperation> schemaChanges,
            Map<TableId, TableChanges.TableChange> updatedSchemas,
            String unsupportedReason) {
        this.type = type;
        this.records = records;
        this.schemaChanges = schemaChanges;
        this.updatedSchemas = updatedSchemas;
        this.unsupportedReason = unsupportedReason;
    }

    public static DeserializeResult dml(List<String> records) {
        return new DeserializeResult(Type.DML, records, null, null, null);
    }

    public static DeserializeResult schemaChange(
            List<SchemaChangeOperation> schemaChanges,
            Map<TableId, TableChanges.TableChange> updatedSchemas) {
        return new DeserializeResult(
                Type.SCHEMA_CHANGE, Collections.emptyList(), schemaChanges, updatedSchemas, null);
    }

    /**
     * Schema change result that also carries DML records from the triggering record. The
     * coordinator should execute DDLs first, then write the records.
     */
    public static DeserializeResult schemaChange(
            List<SchemaChangeOperation> schemaChanges,
            Map<TableId, TableChanges.TableChange> updatedSchemas,
            List<String> records) {
        return new DeserializeResult(
                Type.SCHEMA_CHANGE, records, schemaChanges, updatedSchemas, null);
    }

    /** Carry candidate schemas for target verification without applying them to the reader. */
    public static DeserializeResult unsupportedSchemaChange(
            String reason,
            Map<TableId, TableChanges.TableChange> previousSchemas,
            Map<TableId, TableChanges.TableChange> updatedSchemas) {
        StringBuilder message = new StringBuilder(reason);
        for (Map.Entry<TableId, TableChanges.TableChange> entry : updatedSchemas.entrySet()) {
            Table before = previousSchemas.get(entry.getKey()).getTable();
            Table after = entry.getValue().getTable();
            LOG.info(
                    "[SCHEMA-CHANGE-DETAIL] Table {}: {}. Before: {}. After: {}",
                    entry.getKey().identifier(),
                    reason,
                    before,
                    after);
            List<String> added =
                    after.columns().stream()
                            .map(Column::name)
                            .filter(name -> before.columnWithName(name) == null)
                            .collect(Collectors.toList());
            List<String> removed =
                    before.columns().stream()
                            .map(Column::name)
                            .filter(name -> after.columnWithName(name) == null)
                            .collect(Collectors.toList());
            message.append(". Table: ").append(entry.getKey().identifier());
            if (!added.isEmpty()) {
                message.append("; columns added=").append(added);
            }
            if (!removed.isEmpty()) {
                message.append("; removed=").append(removed);
            }
        }
        return new DeserializeResult(
                Type.SCHEMA_CHANGE,
                Collections.emptyList(),
                Collections.emptyList(),
                updatedSchemas,
                message.toString());
    }

    public String getUnsupportedReason() {
        return unsupportedReason;
    }

    public static DeserializeResult empty() {
        return new DeserializeResult(Type.EMPTY, Collections.emptyList(), null, null, null);
    }

    public Type getType() {
        return type;
    }

    public List<String> getRecords() {
        return records;
    }

    public List<SchemaChangeOperation> getSchemaChanges() {
        return schemaChanges;
    }

    public List<String> getDdls() {
        return schemaChanges == null
                ? null
                : schemaChanges.stream()
                        .map(SchemaChangeOperation::getSql)
                        .collect(Collectors.toList());
    }

    public Map<TableId, TableChanges.TableChange> getUpdatedSchemas() {
        return updatedSchemas;
    }
}
