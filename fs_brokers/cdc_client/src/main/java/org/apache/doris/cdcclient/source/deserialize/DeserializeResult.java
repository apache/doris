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

import java.util.Collections;
import java.util.List;
import java.util.Map;

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
    private final List<String> ddls;
    private final Map<TableId, TableChanges.TableChange> updatedSchemas;

    private DeserializeResult(
            Type type,
            List<String> records,
            List<String> ddls,
            Map<TableId, TableChanges.TableChange> updatedSchemas) {
        this.type = type;
        this.records = records;
        this.ddls = ddls;
        this.updatedSchemas = updatedSchemas;
    }

    public static DeserializeResult dml(List<String> records) {
        return new DeserializeResult(Type.DML, records, null, null);
    }

    public static DeserializeResult schemaChange(
            List<String> ddls, Map<TableId, TableChanges.TableChange> updatedSchemas) {
        return new DeserializeResult(
                Type.SCHEMA_CHANGE, Collections.emptyList(), ddls, updatedSchemas);
    }

    /**
     * Schema change result that also carries DML records from the triggering record. The
     * coordinator should execute DDLs first, then write the records.
     */
    public static DeserializeResult schemaChange(
            List<String> ddls,
            Map<TableId, TableChanges.TableChange> updatedSchemas,
            List<String> records) {
        return new DeserializeResult(Type.SCHEMA_CHANGE, records, ddls, updatedSchemas);
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

    public List<String> getDdls() {
        return ddls;
    }

    public Map<TableId, TableChanges.TableChange> getUpdatedSchemas() {
        return updatedSchemas;
    }
}
