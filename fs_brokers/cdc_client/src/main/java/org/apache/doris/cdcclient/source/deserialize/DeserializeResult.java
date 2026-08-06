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

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

/** Result of deserializing a SourceRecord. */
public class DeserializeResult {

    public enum Type {
        DML,
        SCHEMA_CHANGE,
        EMPTY
    }

    private final Type type;
    private final List<String> records;
    private final List<SchemaChangeOperation> schemaChanges;
    private final Map<TableId, TableChanges.TableChange> updatedSchemas;

    private DeserializeResult(
            Type type,
            List<String> records,
            List<SchemaChangeOperation> schemaChanges,
            Map<TableId, TableChanges.TableChange> updatedSchemas) {
        this.type = type;
        this.records = records;
        this.schemaChanges = schemaChanges;
        this.updatedSchemas = updatedSchemas;
    }

    public static DeserializeResult dml(List<String> records) {
        return new DeserializeResult(Type.DML, records, null, null);
    }

    public static DeserializeResult schemaChange(
            List<SchemaChangeOperation> schemaChanges,
            Map<TableId, TableChanges.TableChange> updatedSchemas) {
        return new DeserializeResult(
                Type.SCHEMA_CHANGE, Collections.emptyList(), schemaChanges, updatedSchemas);
    }

    /**
     * Schema change result that also carries DML records from the triggering record. The
     * coordinator should execute DDLs first, then write the records.
     */
    public static DeserializeResult schemaChange(
            List<SchemaChangeOperation> schemaChanges,
            Map<TableId, TableChanges.TableChange> updatedSchemas,
            List<String> records) {
        return new DeserializeResult(Type.SCHEMA_CHANGE, records, schemaChanges, updatedSchemas);
    }

    public static DeserializeResult empty() {
        return new DeserializeResult(Type.EMPTY, Collections.emptyList(), null, null);
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
