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

package org.apache.iceberg;

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Builds the single in-memory {@link DataTask} that carries a table's REST catalog labels as rows.
 *
 * <p>Labels (apache/iceberg#15750) are returned by the catalog with each table load and are not part of the
 * table metadata, so no {@link MetadataTableType} can produce them. This split-package helper builds an iceberg
 * {@link StaticDataTask} directly (its constructors are package-private) so the task serializes and is read by
 * BE's {@code IcebergSysTableJniScanner} exactly like the SDK's own static metadata tables. The metadata file is
 * synthesized rather than built from an {@code InputFile}, which would issue a storage HEAD for its length.
 */
public final class IcebergLabelsDataTask {
    public static final String OBJECT_SCOPE = "object";
    public static final String FIELD_SCOPE = "field";

    private IcebergLabelsDataTask() {
    }

    /**
     * Returns a task with one row per label: {@code (scope, field_id, field_name, key, value)}. Field labels
     * resolve {@code field_name} against the current table schema, so a label on a dropped field keeps its
     * {@code field_id} with a null {@code field_name}.
     */
    public static DataTask of(Table table, Schema labelsSchema, Schema projectedSchema, String path) {
        Preconditions.checkState(table instanceof SupportsLabels,
                "Iceberg table %s does not expose catalog labels", table.name());
        Labels labels = ((SupportsLabels) table).labels();
        Schema tableSchema = table.schema();
        List<StructLike> rows = new ArrayList<>();
        for (Map.Entry<String, String> label : labels.objectLabels().entrySet()) {
            rows.add(StaticDataTask.Row.of(OBJECT_SCOPE, null, null, label.getKey(), label.getValue()));
        }
        for (FieldLabel field : labels.fields()) {
            String fieldName = tableSchema.findColumnName(field.fieldId());
            for (Map.Entry<String, String> label : field.labels().entrySet()) {
                rows.add(StaticDataTask.Row.of(
                        FIELD_SCOPE, field.fieldId(), fieldName, label.getKey(), label.getValue()));
            }
        }
        DataFile metadataFile = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(path)
                .withFileSizeInBytes(0L)
                .withRecordCount(rows.size())
                .withFormat(FileFormat.METADATA)
                .build();
        return new StaticDataTask(metadataFile, labelsSchema, projectedSchema, rows.toArray(new StructLike[0]));
    }
}
