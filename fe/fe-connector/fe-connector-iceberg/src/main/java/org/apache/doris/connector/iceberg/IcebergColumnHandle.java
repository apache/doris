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

import org.apache.doris.connector.api.handle.ConnectorColumnHandle;

import java.util.Objects;

/**
 * Iceberg {@link ConnectorColumnHandle}, mirroring the paimon connector's {@code PaimonColumnHandle}. Carries
 * the (lowercased) column name and the iceberg field id. {@code IcebergConnectorMetadata.getColumnHandles}
 * produces these so {@code PluginDrivenScanNode.buildColumnHandles} can hand the provider the pruned set of
 * requested columns — which the field-id schema dictionary (T06) keys the {@code current_schema_id = -1}
 * entry off (the CI #969249 fix: the dict's top-level names == the BE scan-slot names BY CONSTRUCTION).
 *
 * <p>Equality/hashCode are by name only (mirrors {@code PaimonColumnHandle}): a handle identifies a column
 * by its (lowercased) name, the same key {@code allHandles.get(slot.getColumn().getName())} looks it up by.
 */
public class IcebergColumnHandle implements ConnectorColumnHandle {

    private static final long serialVersionUID = 1L;

    private final String name;
    private final int fieldId;

    public IcebergColumnHandle(String name, int fieldId) {
        this.name = Objects.requireNonNull(name, "name");
        this.fieldId = fieldId;
    }

    public String getName() {
        return name;
    }

    public int getFieldId() {
        return fieldId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergColumnHandle)) {
            return false;
        }
        IcebergColumnHandle that = (IcebergColumnHandle) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return name + "[" + fieldId + "]";
    }
}
