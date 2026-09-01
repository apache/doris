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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.InstantiationUtil;

import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/** Statement-scoped Paimon write target shared by sink planning and transaction commit. */
final class PaimonWriteBinding {

    private final String tableName;
    private final FileStoreTable table;
    private final String serializedTable;
    private final Map<String, String> hadoopConfig;
    private final boolean overwrite;
    private final Map<String, String> staticPartition;

    private PaimonWriteBinding(String tableName, FileStoreTable table,
            Map<String, String> hadoopConfig, boolean overwrite,
            Map<String, String> staticPartition) {
        this.tableName = tableName;
        this.table = table;
        this.serializedTable = serialize(table);
        this.hadoopConfig = Collections.unmodifiableMap(new LinkedHashMap<>(hadoopConfig));
        this.overwrite = overwrite;
        this.staticPartition = Collections.unmodifiableMap(new LinkedHashMap<>(staticPartition));
    }

    static PaimonWriteBinding create(PaimonTableHandle handle, FileStoreTable table,
            Map<String, String> hadoopConfig, boolean overwrite,
            Map<String, String> requestedStaticPartition) {
        Map<String, String> staticPartition = resolveStaticPartition(table, requestedStaticPartition);
        FileStoreTable writeTable = configureTableForWrite(table, overwrite, staticPartition);
        return new PaimonWriteBinding(handle.getDatabaseName() + "." + handle.getTableName(),
                writeTable, hadoopConfig, overwrite, staticPartition);
    }

    static FileStoreTable configureTableForWrite(FileStoreTable table, boolean overwrite,
            Map<String, String> staticPartition) {
        if (!overwrite) {
            return table;
        }
        String dynamicOverwriteKey = CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key();
        boolean explicitlyDynamic = staticPartition.isEmpty()
                && Boolean.parseBoolean(table.options().get(dynamicOverwriteKey));
        if (explicitlyDynamic) {
            return table;
        }
        return table.copy(Collections.singletonMap(dynamicOverwriteKey, Boolean.FALSE.toString()));
    }

    private static Map<String, String> resolveStaticPartition(FileStoreTable table,
            Map<String, String> requested) {
        Map<String, String> canonicalNames = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (String partitionKey : table.partitionKeys()) {
            canonicalNames.put(partitionKey, partitionKey);
        }
        String defaultPartitionName = CoreOptions.fromMap(table.options()).partitionDefaultName();
        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : requested.entrySet()) {
            String canonicalName = canonicalNames.get(entry.getKey());
            if (canonicalName == null) {
                throw new DorisConnectorException("Column '" + entry.getKey()
                        + "' is not a partition column of Paimon table");
            }
            String value = entry.getValue();
            result.put(canonicalName,
                    value == null || "NULL".equalsIgnoreCase(value) ? defaultPartitionName : value);
        }
        return result;
    }

    private static String serialize(FileStoreTable table) {
        try {
            return Base64.getEncoder().encodeToString(InstantiationUtil.serializeObject(table));
        } catch (IOException e) {
            throw new DorisConnectorException("Failed to serialize Paimon write table", e);
        }
    }

    String tableName() {
        return tableName;
    }

    FileStoreTable getTable() {
        return table;
    }

    String getSerializedTable() {
        return serializedTable;
    }

    Map<String, String> getHadoopConfig() {
        return hadoopConfig;
    }

    boolean isOverwrite() {
        return overwrite;
    }

    Map<String, String> getStaticPartition() {
        return staticPartition;
    }
}
