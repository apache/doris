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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.SchemaCacheKey;
import org.apache.doris.datasource.SchemaCacheValue;

import com.google.common.collect.ImmutableList;
import org.apache.iceberg.Table;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Internal-only logical Iceberg metadata planning table.
 *
 * <p>This table is not backed by an Iceberg native metadata table type. It uses
 * the system-table framework only as the entry point for Doris internal planning
 * queries, and exposes a file-level metadata row schema for later BE/JNI planning.
 */
public class IcebergMetadataPlanningExternalTable extends IcebergSysExternalTable {
    public static final String CONTROL_PREDICATE_COLUMN = "predicate";
    private static final List<Column> RESULT_SCHEMA = ImmutableList.of(
            new Column("content", Type.INT, true),
            new Column("file_path", ScalarType.createStringType(), true),
            new Column("file_format", ScalarType.createStringType(), true),
            new Column("spec_id", Type.INT, true),
            new Column("partition_data", ScalarType.createStringType(), true),
            new Column("record_count", Type.BIGINT, true),
            new Column("file_size_in_bytes", Type.BIGINT, true),
            new Column("split_offsets", ArrayType.create(Type.BIGINT, true), true),
            new Column("first_row_id", Type.BIGINT, true),
            new Column("file_sequence_number", Type.BIGINT, true)
    );
    private static final List<Column> FULL_SCHEMA = ImmutableList.<Column>builder()
            .addAll(RESULT_SCHEMA)
            .add(new Column(CONTROL_PREDICATE_COLUMN, ScalarType.createStringType(), true))
            .build();
    public static final List<String> OUTPUT_COLUMN_NAMES = RESULT_SCHEMA.stream()
            .map(Column::getName)
            .collect(Collectors.toList());

    private final SchemaCacheValue schemaCacheValue = new SchemaCacheValue(FULL_SCHEMA);

    public IcebergMetadataPlanningExternalTable(IcebergExternalTable sourceTable, String sysTableType) {
        super(sourceTable, sysTableType);
    }

    @Override
    public Table getSysIcebergTable() {
        return getSourceTable().getIcebergTable();
    }

    @Override
    public List<Column> getFullSchema() {
        return FULL_SCHEMA;
    }

    @Override
    public Optional<SchemaCacheValue> initSchema(SchemaCacheKey key) {
        return Optional.of(schemaCacheValue);
    }

    @Override
    public Optional<SchemaCacheValue> getSchemaCacheValue() {
        return Optional.of(schemaCacheValue);
    }

    @Override
    public String getComment() {
        return "Iceberg internal metadata planning table for " + getSourceTable().getName();
    }
}
