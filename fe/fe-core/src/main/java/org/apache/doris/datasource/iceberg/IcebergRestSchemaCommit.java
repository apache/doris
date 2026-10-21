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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Preconditions;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTSerializers;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequestParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Adds a partition-counter precondition to a DROP request without persisting a partition spec. */
public final class IcebergRestSchemaCommit {
    private static final ThreadLocal<TableMetadata> DROP_BASE = new ThreadLocal<>();

    private IcebergRestSchemaCommit() {
    }

    public static RESTSessionCatalog newSessionCatalog() {
        return new GuardedSessionCatalog();
    }

    static boolean supports(RESTSessionCatalog catalog) {
        return catalog instanceof GuardedSessionCatalog;
    }

    static void commit(TableOperations operations, TableMetadata base, TableMetadata update) {
        Preconditions.checkState(DROP_BASE.get() == null, "Nested Iceberg REST schema DROP commit");
        DROP_BASE.set(base);
        try {
            // The synchronous SDK commit keeps its endpoint, table auth session, mutation headers,
            // response handling and cached metadata. Only the serialized request gains a requirement.
            operations.commit(base, update);
        } finally {
            DROP_BASE.remove();
        }
    }

    static ObjectMapper newMapper() {
        // Match Iceberg 1.11.0 RESTObjectMapper using public APIs, then specialize only table updates.
        JsonFactory factory = new JsonFactoryBuilder()
                .configure(JsonFactory.Feature.INTERN_FIELD_NAMES, false)
                .configure(JsonFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false).build();
        ObjectMapper mapper = new ObjectMapper(factory);
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setPropertyNamingStrategy(new PropertyNamingStrategies.KebabCaseStrategy());
        RESTSerializers.registerAll(mapper);
        SimpleModule module = new SimpleModule("doris-iceberg-schema-drop");
        module.addSerializer(UpdateTableRequest.class, new DropRequestSerializer());
        mapper.registerModule(module);
        return mapper;
    }

    private static final class GuardedSessionCatalog extends RESTSessionCatalog {
        private GuardedSessionCatalog() {
            super(properties -> HTTPClient.builder(properties).uri(properties.get(CatalogProperties.URI))
                    .withHeaders(RESTUtil.configHeaders(properties)).withObjectMapper(newMapper()).build(), null);
        }
    }

    private static final class DropRequestSerializer extends JsonSerializer<UpdateTableRequest> {
        @Override
        public void serialize(UpdateTableRequest request, JsonGenerator generator, SerializerProvider provider)
                throws IOException {
            TableMetadata base = DROP_BASE.get();
            UpdateTableRequest serialized = request;
            if (base != null) {
                // A scoped commit may serialize only the table whose validated metadata it pinned.
                Preconditions.checkState(request.requirements().stream().anyMatch(requirement ->
                        requirement instanceof UpdateRequirement.AssertTableUUID
                                && Objects.equals(((UpdateRequirement.AssertTableUUID) requirement).uuid(),
                                        base.uuid())),
                        "Iceberg REST schema DROP request does not match its validated table");
                List<UpdateRequirement> requirements = new ArrayList<>(request.requirements());
                requirements.add(new UpdateRequirement.AssertLastAssignedPartitionId(base.lastAssignedPartitionId()));
                serialized = UpdateTableRequest.create(request.identifier(), requirements, request.updates());
            }
            UpdateTableRequestParser.toJson(serialized, generator);
        }
    }
}
