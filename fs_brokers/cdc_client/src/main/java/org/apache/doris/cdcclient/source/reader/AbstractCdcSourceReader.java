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

package org.apache.doris.cdcclient.source.reader;

import org.apache.doris.cdcclient.source.deserialize.DeserializeResult;
import org.apache.doris.cdcclient.source.deserialize.SourceRecordDeserializer;
import org.apache.doris.cdcclient.utils.SchemaChangeManager;
import org.apache.doris.job.cdc.request.JobBaseRecordRequest;

import org.apache.flink.cdc.connectors.base.utils.SerializerUtils;
import org.apache.flink.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class providing common schema-tracking functionality for CDC source readers.
 *
 * <p>Handles serialization/deserialization of {@code tableSchemas} between FE and cdc_client, and
 * provides a helper to load schemas from the incoming {@link JobBaseRecordRequest}.
 */
@Getter
@Setter
public abstract class AbstractCdcSourceReader implements SourceReader {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractCdcSourceReader.class);

    protected static final FlinkJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new FlinkJsonTableChangeSerializer();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    protected SourceRecordDeserializer<SourceRecord, DeserializeResult> serializer;
    protected Map<TableId, TableChanges.TableChange> tableSchemas;

    /**
     * Load tableSchemas from a JSON string (produced by {@link #serializeTableSchemas()}). Used
     * when a binlog/stream split is resumed from FE-persisted state.
     *
     * <p>Format: {@code [{"i":"\"schema\".\"table\"","uc":false,"c":{...debeziumDoc...}},...]}.
     */
    public void loadTableSchemasFromJson(String json) throws IOException {
        if (json == null || json.isEmpty()) {
            return;
        }
        JsonNode root = OBJECT_MAPPER.readTree(json);
        Map<TableId, TableChanges.TableChange> map = new ConcurrentHashMap<>();
        DocumentReader docReader = DocumentReader.defaultReader();
        for (JsonNode entry : root) {
            boolean uc = entry.path("uc").asBoolean(false);
            TableId tableId = TableId.parse(entry.get("i").asText(), uc);
            Document doc = docReader.read(OBJECT_MAPPER.writeValueAsString(entry.get("c")));
            TableChanges.TableChange change = FlinkJsonTableChangeSerializer.fromDocument(doc, uc);
            map.put(tableId, change);
        }
        this.tableSchemas = map;
        this.serializer.setTableSchemas(map);
        LOG.info("Loaded {} table schemas from JSON", map.size());
    }

    /**
     * Serialize current tableSchemas to a compact JSON string for FE persistence.
     *
     * <p>Stores the Debezium document as a nested JSON object (not a string) to avoid redundant
     * escaping. Format: {@code
     * [{"i":"\"schema\".\"table\"","uc":false,"c":{...debeziumDoc...}},...]}.
     */
    @Override
    public String serializeTableSchemas() {
        if (tableSchemas == null || tableSchemas.isEmpty()) {
            return null;
        }
        try {
            DocumentWriter docWriter = DocumentWriter.defaultWriter();
            ArrayNode result = OBJECT_MAPPER.createArrayNode();
            for (Map.Entry<TableId, TableChanges.TableChange> e : tableSchemas.entrySet()) {
                TableId tableId = e.getKey();
                // useCatalogBeforeSchema: false when catalog is null but schema is set (e.g. PG)
                boolean uc = SerializerUtils.shouldUseCatalogBeforeSchema(tableId);
                ObjectNode entry = OBJECT_MAPPER.createObjectNode();
                entry.put("i", tableId.toDoubleQuotedString());
                entry.put("uc", uc);
                // parse compact doc JSON into a JsonNode so "c" is a nested object, not a string
                entry.set(
                        "c",
                        OBJECT_MAPPER.readTree(
                                docWriter.write(TABLE_CHANGE_SERIALIZER.toDocument(e.getValue()))));
                result.add(entry);
            }
            return OBJECT_MAPPER.writeValueAsString(result);
        } catch (Exception e) {
            LOG.warn("Failed to serialize tableSchemas: {}", e.getMessage());
            return null;
        }
    }

    /** Apply schema changes to in-memory tableSchemas and notify the serializer. */
    @Override
    public void applySchemaChange(Map<TableId, TableChanges.TableChange> updatedSchemas) {
        if (updatedSchemas == null || updatedSchemas.isEmpty()) {
            return;
        }
        if (tableSchemas == null) {
            tableSchemas = new ConcurrentHashMap<>(updatedSchemas);
        } else {
            tableSchemas.putAll(updatedSchemas);
        }
        serializer.setTableSchemas(tableSchemas);
    }

    /**
     * Load FE-persisted tableSchemas into memory from the incoming request.
     *
     * <p>FE's schema and offset are always committed together, so FE's schema always corresponds to
     * the starting offset of the current batch. Loading it unconditionally ensures the deserializer
     * uses the correct baseline — particularly critical for MySQL: Flink CDC only retains the
     * latest schema in memory, so if a previous batch executed a DDL but failed to commit the
     * offset, retrying from the pre-DDL offset with a stale post-DDL cache would cause
     * schema-mismatch errors on every retry (see flink-cdc#732). PostgreSQL is unaffected by this
     * because WAL records carry the schema at the time they were written, but loading FE's schema
     * unconditionally is still correct: any re-detected DDL will be handled idempotently by {@link
     * SchemaChangeManager}.
     *
     * <p>Call this at the start of preparing a binlog/stream split.
     */
    protected void tryLoadTableSchemasFromRequest(JobBaseRecordRequest baseReq) throws IOException {
        loadTableSchemasFromJson(baseReq.getTableSchemas());
    }
}
