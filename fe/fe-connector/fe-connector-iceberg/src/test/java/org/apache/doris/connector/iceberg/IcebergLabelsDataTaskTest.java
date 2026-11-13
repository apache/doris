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

package org.apache.doris.connector.iceberg;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IcebergLabelsDataTask;
import org.apache.iceberg.ImmutableFieldLabel;
import org.apache.iceberg.ImmutableLabels;
import org.apache.iceberg.Labels;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests for the {@code t$labels} rows ({@link IcebergLabelsDataTask}): one row per object-level and field-level
 * label, projected in the requested order, and carried intact through the system-table serialization that BE's
 * JNI scanner reads.
 */
public class IcebergLabelsDataTaskTest {

    private static final Schema BASE_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "email", Types.StringType.get()));

    private static final Labels LABELS = ImmutableLabels.builder()
            .objectLabels(Collections.singletonMap("owner", "team-a"))
            .addFields(ImmutableFieldLabel.builder()
                    .fieldId(2)
                    .labels(Collections.singletonMap("classification", "pii"))
                    .build())
            // Field 7 is not in the current schema (e.g. a dropped column): keep its id, no name.
            .addFields(ImmutableFieldLabel.builder()
                    .fieldId(7)
                    .labels(Collections.singletonMap("deprecated", "true"))
                    .build())
            .build();

    private static Table labeledTable(Labels labels) {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        BaseTable table = (BaseTable) catalog.createTable(
                TableIdentifier.of("db1", "t1"), BASE_SCHEMA, PartitionSpec.unpartitioned());
        return new BaseTable(table.operations(), table.name(), table.reporter(), labels);
    }

    /** Reads every row eagerly: StaticDataTask re-wraps one projection per row, so rows cannot be kept. */
    private static List<List<Object>> read(DataTask task, int width) throws IOException {
        List<List<Object>> rows = new ArrayList<>();
        try (CloseableIterable<StructLike> iterable = task.rows()) {
            for (StructLike row : iterable) {
                List<Object> values = new ArrayList<>(width);
                for (int i = 0; i < width; i++) {
                    values.add(row.get(i, Object.class));
                }
                rows.add(values);
            }
        }
        return rows;
    }

    @Test
    public void emitsObjectAndFieldLabelRows() throws IOException {
        DataTask task = IcebergLabelsDataTask.of(labeledTable(LABELS), IcebergLabelsSysTable.SCHEMA,
                IcebergLabelsSysTable.SCHEMA, "/dummyPath");

        Assertions.assertEquals(Arrays.asList(
                Arrays.asList("object", null, null, "owner", "team-a"),
                Arrays.asList("field", 2, "email", "classification", "pii"),
                Arrays.asList("field", 7, null, "deprecated", "true")),
                read(task, 5));
        Assertions.assertEquals(3L, task.file().recordCount());
    }

    @Test
    public void projectsInRequestedOrder() throws IOException {
        // BE reads the projected row positionally in required_fields order, so the task must project to exactly
        // the requested schema. MUTATION: building the task over the full schema -> (scope, field_id) -> red.
        Schema projected = new Schema(
                IcebergLabelsSysTable.SCHEMA.findField("value"),
                IcebergLabelsSysTable.SCHEMA.findField("scope"));
        DataTask task = IcebergLabelsDataTask.of(labeledTable(LABELS), IcebergLabelsSysTable.SCHEMA,
                projected, "/dummyPath");

        Assertions.assertEquals(Arrays.asList(
                Arrays.asList("team-a", "object"),
                Arrays.asList("pii", "field"),
                Arrays.asList("true", "field")),
                read(task, 2));
    }

    @Test
    public void tableWithoutLabelsHasNoRows() throws IOException {
        DataTask task = IcebergLabelsDataTask.of(labeledTable(Labels.EMPTY), IcebergLabelsSysTable.SCHEMA,
                IcebergLabelsSysTable.SCHEMA, "/dummyPath");

        Assertions.assertTrue(read(task, 5).isEmpty());
    }

    @Test
    public void rowsSurviveSystemTableSerialization() throws IOException {
        // The task is shipped to BE as a serialized FileScanTask; labels must be materialized in it, since the
        // BE side has no catalog to ask. MUTATION: keeping a lazy reference to the table's labels -> red.
        DataTask task = IcebergLabelsDataTask.of(labeledTable(LABELS), IcebergLabelsSysTable.SCHEMA,
                IcebergLabelsSysTable.SCHEMA, "/dummyPath");

        FileScanTask decoded = IcebergSystemTableSerialization.deserializeFromBase64(
                IcebergSystemTableSerialization.serializeToBase64(task));

        Assertions.assertEquals(read(task, 5), read(decoded.asDataTask(), 5));
    }
}
