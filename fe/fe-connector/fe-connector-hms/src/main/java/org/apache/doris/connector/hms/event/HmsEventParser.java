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

package org.apache.doris.connector.hms.event;

import org.apache.doris.connector.api.event.MetastoreChangeDescriptor;
import org.apache.doris.connector.api.event.MetastoreChangeDescriptor.Op;
import org.apache.doris.connector.hms.HmsNotificationEvent;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.AddPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateTableMessage;
import org.apache.hadoop.hive.metastore.messaging.DropPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAlterDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONDropTableMessage;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageDeserializer;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

/**
 * Parses one HMS {@link HmsNotificationEvent} into connector-neutral {@link MetastoreChangeDescriptor}s.
 *
 * <p>This is the plugin-side half of the metastore-event relocation: it replaces the fe-core
 * {@code datasource.hive.event.*} classes' {@code process()} bodies, but instead of mutating the
 * engine's object graph it emits neutral descriptors the engine applies. It preserves the legacy
 * semantics faithfully (table-name lowercasing, rename vs view-recreate, alter-partition rename =
 * drop+add, canonical partition names), so a flipped catalog behaves as the legacy poller did.</p>
 *
 * <p>The GZIP message format (some HDP/CDH Hive versions) base64+gzip-wraps the JSON payload; we
 * decompress it before handing the plain {@link JSONMessageDeserializer} the body, avoiding a bespoke
 * deserializer subclass.</p>
 */
public final class HmsEventParser {

    private static final JSONMessageDeserializer JSON = new JSONMessageDeserializer();
    private static final String GZIP_FORMAT_PREFIX = "gzip";

    private HmsEventParser() {
    }

    /**
     * Map one notification event to zero or more descriptors (empty for unsupported / no-op events,
     * e.g. a properties-only ALTER DATABASE or a transaction event).
     */
    public static List<MetastoreChangeDescriptor> parse(HmsNotificationEvent event) {
        try {
            return doParse(event);
        } catch (Exception e) {
            throw new RuntimeException("failed to parse HMS notification event " + event, e);
        }
    }

    private static List<MetastoreChangeDescriptor> doParse(HmsNotificationEvent event) throws Exception {
        String type = event.getEventType();
        if (type == null) {
            return Collections.emptyList();
        }
        String dbName = lower(event.getDbName());
        String tblName = event.getTableName();
        long eventId = event.getEventId();
        // Hive records event time in seconds; the engine tracks freshness in millis.
        long updateTime = event.getEventTime() * 1000L;
        String body = prepareBody(event.getMessageFormat(), event.getMessage());

        switch (type) {
            case "CREATE_TABLE": {
                CreateTableMessage message = JSON.getCreateTableMessage(body);
                String table = message.getTableObj().getTableName().toLowerCase(Locale.ROOT);
                return one(MetastoreChangeDescriptor.forTable(
                        Op.REGISTER_TABLE, dbName, table, null, eventId, updateTime));
            }
            case "DROP_TABLE": {
                JSONDropTableMessage message = (JSONDropTableMessage) JSON.getDropTableMessage(body);
                return one(MetastoreChangeDescriptor.forTable(
                        Op.UNREGISTER_TABLE, dbName, message.getTable(), null, eventId, updateTime));
            }
            case "ALTER_TABLE": {
                JSONAlterTableMessage message = (JSONAlterTableMessage) JSON.getAlterTableMessage(body);
                Table after = message.getTableObjAfter();
                Table before = message.getTableObjBefore();
                String afterTable = after.getTableName().toLowerCase(Locale.ROOT);
                boolean isRename = !before.getDbName().equalsIgnoreCase(after.getDbName())
                        || !before.getTableName().equalsIgnoreCase(afterTable);
                boolean isView = before.isSetViewExpandedText() || before.isSetViewOriginalText();
                if (isRename || isView) {
                    // rename (possibly across dbs) or view-recreate (same name) => unregister+register
                    return one(MetastoreChangeDescriptor.forTableRename(
                            before.getDbName(), before.getTableName(),
                            after.getDbName(), afterTable, eventId, updateTime));
                }
                return one(MetastoreChangeDescriptor.forTable(
                        Op.REFRESH_TABLE, before.getDbName(), before.getTableName(), null,
                        eventId, updateTime));
            }
            case "CREATE_DATABASE":
                return one(MetastoreChangeDescriptor.forDatabase(
                        Op.REGISTER_DATABASE, dbName, null, eventId, updateTime));
            case "DROP_DATABASE":
                return one(MetastoreChangeDescriptor.forDatabase(
                        Op.UNREGISTER_DATABASE, dbName, null, eventId, updateTime));
            case "ALTER_DATABASE": {
                JSONAlterDatabaseMessage message =
                        (JSONAlterDatabaseMessage) JSON.getAlterDatabaseMessage(body);
                Database before = message.getDbObjBefore();
                Database after = message.getDbObjAfter();
                if (before.getName().equalsIgnoreCase(after.getName())) {
                    // properties-only change: nothing the engine must react to
                    return Collections.emptyList();
                }
                return one(MetastoreChangeDescriptor.forDatabase(
                        Op.RENAME_DATABASE, before.getName(), after.getName(), eventId, updateTime));
            }
            case "ADD_PARTITION": {
                AddPartitionMessage message = JSON.getAddPartitionMessage(body);
                Table table = message.getTableObj();
                List<String> names = new ArrayList<>();
                List<String> colNames = partitionColNames(table);
                for (Partition partition : message.getPartitionObjs()) {
                    names.add(FileUtils.makePartName(colNames, partition.getValues()));
                }
                return one(MetastoreChangeDescriptor.forPartitions(
                        Op.ADD_PARTITIONS, dbName, table.getTableName(), names, eventId, updateTime));
            }
            case "DROP_PARTITION": {
                DropPartitionMessage message = JSON.getDropPartitionMessage(body);
                Table table = message.getTableObj();
                List<String> names = new ArrayList<>();
                List<String> colNames = partitionColNames(table);
                for (Map<String, String> partition : message.getPartitions()) {
                    names.add(rawPartName(partition, colNames));
                }
                return one(MetastoreChangeDescriptor.forPartitions(
                        Op.DROP_PARTITIONS, dbName, table.getTableName(), names, eventId, updateTime));
            }
            case "ALTER_PARTITION": {
                AlterPartitionMessage message = JSON.getAlterPartitionMessage(body);
                Table table = message.getTableObj();
                List<String> colNames = partitionColNames(table);
                String before = FileUtils.makePartName(colNames, message.getPtnObjBefore().getValues());
                String after = FileUtils.makePartName(colNames, message.getPtnObjAfter().getValues());
                if (!before.equalsIgnoreCase(after)) {
                    // partition rename = drop old + add new (on the event's db/table)
                    List<MetastoreChangeDescriptor> out = new ArrayList<>(2);
                    out.add(MetastoreChangeDescriptor.forPartitions(
                            Op.DROP_PARTITIONS, dbName, tblName, Collections.singletonList(before),
                            eventId, updateTime));
                    out.add(MetastoreChangeDescriptor.forPartitions(
                            Op.ADD_PARTITIONS, dbName, tblName, Collections.singletonList(after),
                            eventId, updateTime));
                    return out;
                }
                return one(MetastoreChangeDescriptor.forPartitions(
                        Op.REFRESH_PARTITIONS, dbName, table.getTableName(),
                        Collections.singletonList(after), eventId, updateTime));
            }
            case "INSERT":
                // non-partitioned insert: no partition event fires, just drop the table's caches
                return one(MetastoreChangeDescriptor.forTable(
                        Op.REFRESH_TABLE, dbName, tblName, null, eventId, updateTime));
            default:
                // ALTER_PARTITIONS / INSERT_PARTITIONS / ALLOC_WRITE_ID / COMMIT_TXN / ... => ignored
                return Collections.emptyList();
        }
    }

    private static List<String> partitionColNames(Table table) {
        return table.getPartitionKeys().stream().map(FieldSchema::getName).collect(Collectors.toList());
    }

    // Raw "col=val/col2=val2" name (mirrors the legacy DropPartition path, which does no escaping).
    private static String rawPartName(Map<String, String> part, List<String> colNames) {
        if (part.isEmpty() || colNames.size() != part.size()) {
            return "";
        }
        StringBuilder name = new StringBuilder();
        int i = 0;
        for (String colName : colNames) {
            if (i++ > 0) {
                name.append('/');
            }
            name.append(colName).append('=').append(part.get(colName));
        }
        return name.toString();
    }

    private static String lower(String s) {
        return (s == null || s.isEmpty()) ? s : s.toLowerCase(Locale.ROOT);
    }

    private static List<MetastoreChangeDescriptor> one(MetastoreChangeDescriptor descriptor) {
        return Collections.singletonList(descriptor);
    }

    private static String prepareBody(String format, String message) {
        if (format != null && format.startsWith(GZIP_FORMAT_PREFIX)) {
            return deCompress(message);
        }
        return message;
    }

    private static String deCompress(String messageBody) {
        try {
            byte[] decodedBytes = Base64.getDecoder().decode(messageBody.getBytes(StandardCharsets.UTF_8));
            try (ByteArrayInputStream in = new ByteArrayInputStream(decodedBytes);
                    GZIPInputStream is = new GZIPInputStream(in)) {
                return new String(IOUtils.toByteArray(is), StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            throw new RuntimeException("cannot decode the gzip notification message", e);
        }
    }
}
