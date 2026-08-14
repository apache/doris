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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Immutable statement binding shared by the Paimon write plan and transaction. */
final class PaimonWriteBinding {

    private final Identifier identifier;
    private final FileStoreTable table;
    private final String serializedTable;
    private final Map<String, String> hadoopConfig;
    private final boolean overwrite;
    private final Map<String, String> staticPartition;
    private final String metadataIdentity;

    PaimonWriteBinding(Identifier identifier, FileStoreTable table,
            Map<String, String> hadoopConfig, boolean overwrite,
            Map<String, String> staticPartition, String metadataIdentity) {
        this.identifier = identifier;
        this.table = table;
        this.serializedTable = PaimonScanPlanProvider.encodeObjectToString(
                PaimonScanPlanProvider.dropCatalogLoader(table));
        this.hadoopConfig = Collections.unmodifiableMap(new LinkedHashMap<>(hadoopConfig));
        this.overwrite = overwrite;
        this.staticPartition = Collections.unmodifiableMap(new LinkedHashMap<>(staticPartition));
        this.metadataIdentity = metadataIdentity;
    }

    Identifier getIdentifier() {
        return identifier;
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

    String getMetadataIdentity() {
        return metadataIdentity;
    }

    String tableName() {
        return identifier.getFullName();
    }

    static FileStoreTable configureTableForWrite(FileStoreTable table, boolean overwrite,
            Map<String, String> staticPartition) {
        if (!overwrite) {
            return table;
        }
        String option = CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key();
        boolean explicitlyDynamic = staticPartition.isEmpty()
                && Boolean.parseBoolean(table.options().get(option));
        if (explicitlyDynamic) {
            return table;
        }
        // Doris OVERWRITE without PARTITION means whole-table replacement by default.
        return table.copy(Collections.singletonMap(option, Boolean.FALSE.toString()));
    }
}
