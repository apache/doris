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
import org.apache.doris.job.cdc.request.JobBaseConfig;
import org.apache.doris.job.cdc.request.JobBaseRecordRequest;

import org.apache.flink.cdc.connectors.base.utils.SerializerUtils;
import org.apache.flink.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.Column;
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

    private final Map<String, Class<?>> splitKeyClassCache = new ConcurrentHashMap<>();

    @Override
    public synchronized void release(JobBaseConfig jobConfig) {
        // Stop the engine but keep source-side state (e.g. the PG replication slot) for another
        // backend to take over.
        LOG.info("Release source reader for job {}", jobConfig.getJobId());
        finishSplitRecords();
        shutdownSnapshotPollExecutor();
    }

    /** Stop the snapshot-phase poll thread pool; called when this reader instance is discarded. */
    protected void shutdownSnapshotPollExecutor() {}

    /** Drop source-side owned resources. Returns false if cleanup is incomplete (retry later). */
    public boolean releaseSourceResources(JobBaseConfig jobConfig) {
        return true;
    }

    protected abstract Class<?> probeSplitKeyClass(
            TableId tableId, Column splitColumn, JobBaseConfig jobConfig);

    protected Class<?> resolveSplitKeyClass(
            TableId tableId, Column splitColumn, JobBaseConfig jobConfig) {
        String key = tableId.identifier() + "." + splitColumn.name();
        return splitKeyClassCache.computeIfAbsent(
                key, k -> probeSplitKeyClass(tableId, splitColumn, jobConfig));
    }

    protected static Object[] convertBounds(Object[] raw, Class<?> target, ObjectMapper mapper) {
        if (raw == null) {
            return null;
        }
        Object[] out = new Object[raw.length];
        for (int i = 0; i < raw.length; i++) {
            out[i] = convertBound(raw[i], target, mapper);
        }
        return out;
    }

    private static final Map<Class<?>, Function<String, Object>> BOUND_PARSERS =
            Map.of(
                    java.sql.Date.class, java.sql.Date::valueOf,
                    java.sql.Timestamp.class, AbstractCdcSourceReader::parseTimestampBound,
                    java.sql.Time.class, java.sql.Time::valueOf,
                    java.time.LocalDateTime.class, java.time.LocalDateTime::parse,
                    java.time.LocalDate.class, java.time.LocalDate::parse,
                    java.time.LocalTime.class, java.time.LocalTime::parse,
                    java.time.OffsetDateTime.class, java.time.OffsetDateTime::parse);

    // Offset suffix like "+08:00" / "-05:00" ("Z" is handled separately).
    private static final java.util.regex.Pattern OFFSET_SUFFIX =
            java.util.regex.Pattern.compile("[+-]\\d{2}:\\d{2}$");

    /**
     * A java.sql.Timestamp bound is serialized to FE by Jackson as ISO-8601 (with a zone offset
     * when WRITE_DATES_AS_TIMESTAMPS is off), which java.sql.Timestamp#valueOf cannot parse.
     * Reconstruct instant-faithfully so the rebuilt bound binds back to the same value flink-cdc
     * read.
     */
    private static java.sql.Timestamp parseTimestampBound(String s) {
        try {
            return java.sql.Timestamp.valueOf(s); // legacy SQL form "yyyy-MM-dd HH:mm:ss[.f]"
        } catch (IllegalArgumentException notSqlForm) {
            if (s.endsWith("Z") || OFFSET_SUFFIX.matcher(s).find()) {
                return java.sql.Timestamp.from(java.time.OffsetDateTime.parse(s).toInstant());
            }
            return java.sql.Timestamp.valueOf(java.time.LocalDateTime.parse(s)); // ISO local ('T')
        }
    }

    private static Object convertBound(Object v, Class<?> target, ObjectMapper mapper) {
        if (v == null) {
            return null;
        }
        if (target.isInstance(v)) {
            return v;
        }
        Function<String, Object> parser = BOUND_PARSERS.get(target);
        if (parser != null) {
            return parser.apply(v.toString());
        }
        return mapper.convertValue(v, target);
    }

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
            // Return null so the current batch is not failed — data keeps flowing and
            // schema persistence will be retried on the next DDL or feHadNoSchema batch.
            // For PostgreSQL this is safe: WAL records carry afterSchema so the next DML
            // will re-trigger schema-change detection and self-heal.
            // WARNING: for MySQL (schema change not yet implemented), returning null here
            // is dangerous — MySQL binlog has no inline schema, so loading a stale
            // pre-DDL schema from FE on the next task would cause column mismatches
            // (flink-cdc#732). When MySQL schema change is implemented, this must throw
            // instead of returning null to prevent committing the offset with a stale schema.
            LOG.error(
                    "Failed to serialize tableSchemas, schema will not be persisted to FE"
                            + " in this cycle. Will retry on next DDL or batch.",
                    e);
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
