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

package org.apache.doris.cdcclient.service;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

class PipelineCoordinatorTest {
    @Test
    void deserializationFailureOmitsOffsetButRetainsTableAndCause() {
        String message = PipelineCoordinator.formatSourceRecordFailure(
                "Failed to deserialize source record",
                record(Collections.singletonMap("pos", 123L)),
                new IOException("Cannot deserialize row", new IllegalArgumentException("Invalid column")),
                false);

        assertThat(message)
                .contains("Failed to deserialize source record. Reason: IOException: Cannot deserialize row")
                .contains("caused by: IllegalArgumentException: Invalid column")
                .contains("Source table: orders")
                .doesNotContain("Source offset:", "123");
    }

    @Test
    void ddlFailureIncludesOffsetAfterTable() {
        String message = PipelineCoordinator.formatSourceRecordFailure(
                "Failed to execute Doris DDL",
                record(Collections.singletonMap("pos", 123L)),
                new IOException("ALTER TABLE denied"),
                true);

        assertThat(message).isEqualTo(
                "Failed to execute Doris DDL. Reason: IOException: ALTER TABLE denied"
                        + ". Source table: orders. Source offset: {\"pos\":123}");
    }

    @Test
    void ddlFailureWithoutOffsetDoesNotFabricateOne() {
        String message = PipelineCoordinator.formatSourceRecordFailure(
                "Failed to execute Doris DDL",
                record(null),
                new IOException("ALTER TABLE denied"),
                true);

        assertThat(message)
                .contains("ALTER TABLE denied", "Source table: orders")
                .doesNotContain("Source offset:");
    }

    private SourceRecord record(Map<String, ?> offset) {
        Schema sourceSchema = SchemaBuilder.struct().field("table", Schema.STRING_SCHEMA).build();
        Schema schema = SchemaBuilder.struct().field("source", sourceSchema).build();
        Struct value = new Struct(schema).put("source", new Struct(sourceSchema).put("table", "orders"));
        return new SourceRecord(Collections.emptyMap(), offset, "orders", schema, value);
    }
}
