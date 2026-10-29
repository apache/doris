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
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.rest.RESTSerializers;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequestParser;
import org.apache.iceberg.util.JsonUtil;

import java.io.IOException;
import java.util.List;

/** Preserves the assigned field ID high-water mark for Iceberg 1.6 REST servers. */
final class IcebergRestSchemaCompatibility {
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

    private static void writeUpdate(UpdateTableRequest request, JsonGenerator generator) throws IOException {
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
