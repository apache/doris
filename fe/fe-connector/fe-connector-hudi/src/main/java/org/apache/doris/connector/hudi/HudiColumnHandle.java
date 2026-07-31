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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.api.handle.ConnectorColumnHandle;

import java.util.Objects;

/**
 * Column handle for Hudi tables, carrying column name, type,
 * whether the column is a partition key, and the Hudi InternalSchema
 * field id (for schema-evolution BY_FIELD_ID matching; HD-C4b).
 */
public class HudiColumnHandle implements ConnectorColumnHandle {

    private static final long serialVersionUID = 1L;

    private final String name;
    private final String typeName;
    private final boolean isPartitionKey;
    // Hudi InternalSchema field id (stable across renames), sourced from the mode-aware InternalSchema and
    // threaded here by HudiConnectorMetadata.getColumnHandles. ConnectorColumn.UNSET_UNIQUE_ID (-1) when no id
    // was resolved (e.g. a _hoodie_* meta column absent from a commit-metadata schema). BE's field-id mode is
    // PER-FILE, not per-column, so an unresolved id CANNOT fall back BY_NAME on its own; instead the whole
    // scan-level dict is gated OFF when any projected column is unresolved (see
    // HudiSchemaUtils.buildSchemaEvolutionProp) -> BE stays on BY_NAME for the entire scan. Deliberately NOT part
    // of equals/hashCode: the handle's identity stays name+type (mirror IcebergColumnHandle, which keeps identity
    // by name and does not fold the field id in).
    private final int fieldId;

    public HudiColumnHandle(String name, String typeName, boolean isPartitionKey, int fieldId) {
        this.name = Objects.requireNonNull(name);
        this.typeName = Objects.requireNonNull(typeName);
        this.isPartitionKey = isPartitionKey;
        this.fieldId = fieldId;
    }

    public String getName() {
        return name;
    }

    public String getTypeName() {
        return typeName;
    }

    public boolean isPartitionKey() {
        return isPartitionKey;
    }

    public int getFieldId() {
        return fieldId;
    }

    public String getColumnName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HudiColumnHandle)) {
            return false;
        }
        HudiColumnHandle that = (HudiColumnHandle) o;
        return name.equals(that.name) && typeName.equals(that.typeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, typeName);
    }

    @Override
    public String toString() {
        return "HudiColumnHandle{" + name + ":" + typeName + "[" + fieldId + "]"
                + (isPartitionKey ? " [partition]" : "") + "}";
    }
}
