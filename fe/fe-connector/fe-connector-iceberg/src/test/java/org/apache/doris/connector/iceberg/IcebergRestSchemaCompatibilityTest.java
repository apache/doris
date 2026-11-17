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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.UpdateRequirements;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CommitTransactionRequestParser;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequestParser;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class IcebergRestSchemaCompatibilityTest {
    private final ObjectMapper mapper = IcebergRestSchemaCompatibility.objectMapper();

    @Test
    void testDropHighestOfThreeFields() throws Exception {
        assertDropHighestField(3);
    }

    @Test
    void testDropHighestOfTwoFields() throws Exception {
        assertDropHighestField(2);
    }

    private void assertDropHighestField(int count) throws Exception {
        TableMetadata base = metadata(count);
        UpdateTableRequest request = update(base, schema(count - 1));
        String original = UpdateTableRequestParser.toJson(request);
        JsonNode expected = mapper.readTree(original);
        // This is the only wire change: preserve the historical high-water mark, not the surviving maximum ID.
        ((ObjectNode) expected.get("updates").get(0)).put("last-column-id", count);
        JsonNode actual = wire(request);
        Assertions.assertEquals(expected, actual);
        Assertions.assertEquals(original, UpdateTableRequestParser.toJson(request));
        Assertions.assertEquals(count - 1, actual.at("/updates/0/schema/fields").size());
        for (int index = 0; index < count - 1; index++) {
            Assertions.assertEquals(index + 1, actual.at("/updates/0/schema/fields/" + index + "/id").asInt());
        }
    }

    @Test
    void testRetainHistoricalIdAfterEarlierDrop() throws Exception {
        TableMetadata base = TableMetadata.buildFrom(metadata(3)).addSchema(schema(2)).build();
        Assertions.assertEquals(3, base.lastColumnId());
        Assertions.assertEquals(3, wire(update(base, schema(1))).at("/updates/0/last-column-id").asInt());
    }

    @Test
    void testAddThenDropWithinOneCommitDoesNotLowerId() throws Exception {
        JsonNode json = wire(update(metadata(3), schema(4), schema(2)));
        Assertions.assertEquals(4, json.at("/updates/0/last-column-id").asInt());
        Assertions.assertEquals(4, json.at("/updates/1/last-column-id").asInt());
    }

    @Test
    void testCreateTracksIdsAcrossSchemaUpdates() throws Exception {
        List<MetadataUpdate> updates = Arrays.asList(new MetadataUpdate.AddSchema(schema(4)),
                new MetadataUpdate.AddSchema(schema(2)));
        JsonNode json = wire(new UpdateTableRequest(UpdateRequirements.forCreateTable(updates), updates));
        Assertions.assertEquals(4, json.at("/updates/0/last-column-id").asInt());
        Assertions.assertEquals(4, json.at("/updates/1/last-column-id").asInt());
    }

    @Test
    void testReplaceRetainsBaseAssignedId() throws Exception {
        TableMetadata base = metadata(3);
        List<MetadataUpdate> updates = Collections.singletonList(new MetadataUpdate.AddSchema(schema(2)));
        UpdateTableRequest request = new UpdateTableRequest(UpdateRequirements.forReplaceTable(base, updates), updates);
        Assertions.assertEquals(3, wire(request).at("/updates/0/last-column-id").asInt());
    }

    @Test
    void testMultiTableCommitKeepsIndependentHighWaterMarks() throws Exception {
        UpdateTableRequest first = update(metadata(3), schema(4), schema(2));
        UpdateTableRequest second = update(metadata(2), schema(1));
        second = UpdateTableRequest.create(TableIdentifier.of("db", "other"), second.requirements(), second.updates());
        CommitTransactionRequest request = new CommitTransactionRequest(Arrays.asList(first, second));
        JsonNode expected = mapper.readTree(CommitTransactionRequestParser.toJson(request));
        ((ObjectNode) expected.at("/table-changes/0/updates/0")).put("last-column-id", 4);
        ((ObjectNode) expected.at("/table-changes/0/updates/1")).put("last-column-id", 4);
        ((ObjectNode) expected.at("/table-changes/1/updates/0")).put("last-column-id", 2);
        Assertions.assertEquals(expected, mapper.readTree(mapper.writeValueAsString(request)));
    }

    @Test
    void testNonSchemaCommitIsUnchanged() throws Exception {
        UpdateTableRequest request = UpdateTableRequest.create(TableIdentifier.of("db", "table"),
                Collections.singletonList(new UpdateRequirement.AssertTableUUID(metadata(3).uuid())),
                Collections.singletonList(new MetadataUpdate.SetProperties(Collections.singletonMap("key", "value"))));
        Assertions.assertEquals(mapper.readTree(UpdateTableRequestParser.toJson(request)), wire(request));
    }

    @Test
    void testConcurrentSchemaChangeStillFailsRequirements() throws Exception {
        UpdateTableRequest request = update(metadata(3), schema(2));
        UpdateTableRequest decoded = UpdateTableRequestParser.fromJson(mapper.writeValueAsString(request));
        TableMetadata concurrent = TableMetadata.buildFrom(metadata(3)).addSchema(schema(4)).build();
        UpdateRequirement fieldIdRequirement = decoded.requirements().stream()
                .filter(requirement -> requirement instanceof UpdateRequirement.AssertLastAssignedFieldId)
                .findFirst().orElseThrow(AssertionError::new);
        Assertions.assertThrows(CommitFailedException.class, () -> fieldIdRequirement.validate(concurrent));
    }

    @Test
    void testOtherRestResponsesKeepSdkCodecsAndUnknownFieldTolerance() throws Exception {
        ConfigResponse config = mapper.readValue("{\"defaults\":{\"key\":\"value\"},"
                + "\"overrides\":{},\"future-field\":true}", ConfigResponse.class);
        Assertions.assertEquals("value", config.defaults().get("key"));
    }

    private JsonNode wire(UpdateTableRequest request) throws Exception {
        return mapper.readTree(mapper.writeValueAsString(request));
    }

    private static UpdateTableRequest update(TableMetadata base, Schema... schemas) {
        List<MetadataUpdate> updates = new ArrayList<>();
        for (Schema schema : schemas) {
            updates.add(new MetadataUpdate.AddSchema(schema));
        }
        return UpdateTableRequest.create(TableIdentifier.of("db", "table"),
                UpdateRequirements.forUpdateTable(base, updates), updates);
    }

    private static TableMetadata metadata(int count) {
        return TableMetadata.newTableMetadata(schema(count), PartitionSpec.unpartitioned(), "s3://bucket/table",
                Collections.emptyMap());
    }

    private static Schema schema(int count) {
        List<Types.NestedField> fields = new ArrayList<>();
        for (int id = 1; id <= count; id++) {
            fields.add(Types.NestedField.optional(id, "c" + id, Types.IntegerType.get()));
        }
        return new Schema(fields);
    }
}
