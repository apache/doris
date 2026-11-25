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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.rest.RESTSerializers;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequestParser;
import org.apache.iceberg.util.JsonUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Preserves metadata invariants across Iceberg REST client/server versions. */
final class IcebergRestSchemaCompatibility {
    private static final ThreadLocal<WriteFormat> WRITE_FORMAT = new ThreadLocal<>();

    private IcebergRestSchemaCompatibility() {
    }

    static ObjectMapper objectMapper() {
        // Match the SDK's package-private RESTObjectMapper, including all request/response codecs.
        ObjectMapper mapper = new ObjectMapper(new JsonFactoryBuilder()
                .configure(JsonFactory.Feature.INTERN_FIELD_NAMES, false)
                .configure(JsonFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false).build());
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setPropertyNamingStrategy(new PropertyNamingStrategies.KebabCaseStrategy());
        RESTSerializers.registerAll(mapper);
        SimpleModule module = new SimpleModule("doris-iceberg-rest-schema-compatibility");
        module.addSerializer(UpdateTableRequest.class, new UpdateTableSerializer());
        // The SDK transaction serializer invokes UpdateTableRequestParser directly, bypassing Jackson codecs.
        module.addSerializer(CommitTransactionRequest.class, new CommitTransactionSerializer());
        mapper.registerModule(module);
        return mapper;
    }

    static void commitWithFormatFence(TableOperations operations, TableMetadata base, TableMetadata updated,
            int admittedFormat) {
        WriteFormat previous = WRITE_FORMAT.get();
        // The SDK serializes REST requests synchronously on this committing thread.
        WRITE_FORMAT.set(new WriteFormat(base.uuid(), admittedFormat));
        try {
            operations.commit(base, updated);
        } finally {
            if (previous == null) {
                WRITE_FORMAT.remove();
            } else {
                WRITE_FORMAT.set(previous);
            }
        }
    }

    private static UpdateTableRequest withWriteFormat(UpdateTableRequest request) {
        WriteFormat format = WRITE_FORMAT.get();
        if (format == null || request.requirements().stream().noneMatch(requirement ->
                requirement instanceof UpdateRequirement.AssertTableUUID
                        && ((UpdateRequirement.AssertTableUUID) requirement).uuid().equalsIgnoreCase(format.uuid))) {
            return request;
        }
        List<MetadataUpdate> updates = new ArrayList<>(request.updates().size() + 1);
        // REST requirements do not fence format. An explicit same-version upgrade is a no-op on that version,
        // but atomically rejects a server-side upgrade because the format cannot be downgraded.
        updates.add(new MetadataUpdate.UpgradeFormatVersion(format.version));
        updates.addAll(request.updates());
        return UpdateTableRequest.create(request.identifier(), request.requirements(), updates);
    }

    private static final class WriteFormat {
        private final String uuid;
        private final int version;

        private WriteFormat(String uuid, int version) {
            this.uuid = uuid;
            this.version = version;
        }
    }

    private static void writeUpdate(UpdateTableRequest request, JsonGenerator generator) throws IOException {
        request = withWriteFormat(request);
        List<MetadataUpdate> updates = request.updates();
        boolean addsSchema = updates.stream().anyMatch(update -> update instanceof MetadataUpdate.AddSchema);
        if (!addsSchema) {
            UpdateTableRequestParser.toJson(request, generator);
            return;
        }

        int lastColumnId = 0;
        for (UpdateRequirement requirement : request.requirements()) {
            if (requirement instanceof UpdateRequirement.AssertLastAssignedFieldId) {
                lastColumnId = ((UpdateRequirement.AssertLastAssignedFieldId) requirement).lastAssignedFieldId();
            }
        }
        JsonNode json = JsonUtil.mapper().readTree(UpdateTableRequestParser.toJson(request));
        for (int index = 0; index < updates.size(); index++) {
            MetadataUpdate update = updates.get(index);
            if (update instanceof MetadataUpdate.AddSchema) {
                // SDK 1.9+ writes schema.highestFieldId(), which can decrease after DROP COLUMN.
                // REST 1.6 validates this deprecated field against the table's historical last-column-id.
                // Keep it monotonic across every schema change in a single commit as well.
                lastColumnId = Math.max(lastColumnId, ((MetadataUpdate.AddSchema) update).schema().highestFieldId());
                ((ObjectNode) json.get("updates").get(index)).put("last-column-id", lastColumnId);
            }
        }
        generator.writeTree(json);
    }

    private static class UpdateTableSerializer extends JsonSerializer<UpdateTableRequest> {
        @Override
        public void serialize(UpdateTableRequest request, JsonGenerator generator, SerializerProvider provider)
                throws IOException {
            writeUpdate(request, generator);
        }
    }

    private static class CommitTransactionSerializer extends JsonSerializer<CommitTransactionRequest> {
        @Override
        public void serialize(CommitTransactionRequest request, JsonGenerator generator, SerializerProvider provider)
                throws IOException {
            generator.writeStartObject();
            generator.writeArrayFieldStart("table-changes");
            for (UpdateTableRequest tableChange : request.tableChanges()) {
                writeUpdate(tableChange, generator);
            }
            generator.writeEndArray();
            generator.writeEndObject();
        }
    }
}
