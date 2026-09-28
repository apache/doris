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

import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SchemaCacheKey;

import com.google.common.base.Objects;

import java.util.Optional;

public class IcebergSchemaCacheKey extends SchemaCacheKey {
    private final String tableUuid;
    private final long schemaId;
    private final int partitionSpecId;
    // The requested schemaId may be historical while the frozen table has a newer current schema.
    // A rename changes this ID even when the table UUID and partition spec ID stay unchanged.
    private final int projectionSchemaId;
    private final boolean enableMappingVarbinary;
    private final boolean enableMappingTimestampTz;

    public IcebergSchemaCacheKey(NameMapping nameMapping, long schemaId) {
        this(nameMapping, "", schemaId, -1, false, false);
    }

    public IcebergSchemaCacheKey(NameMapping nameMapping, String tableUuid, long schemaId) {
        this(nameMapping, tableUuid, schemaId, -1, false, false);
    }

    public IcebergSchemaCacheKey(NameMapping nameMapping, String tableUuid, long schemaId, int partitionSpecId) {
        this(nameMapping, tableUuid, schemaId, partitionSpecId, false, false);
    }

    public IcebergSchemaCacheKey(NameMapping nameMapping, String tableUuid, long schemaId,
            boolean enableMappingVarbinary, boolean enableMappingTimestampTz) {
        this(nameMapping, tableUuid, schemaId, -1,
                enableMappingVarbinary, enableMappingTimestampTz);
    }

    public IcebergSchemaCacheKey(NameMapping nameMapping, String tableUuid, long schemaId, int partitionSpecId,
            boolean enableMappingVarbinary, boolean enableMappingTimestampTz) {
        this(nameMapping, tableUuid, schemaId, partitionSpecId, -1,
                enableMappingVarbinary, enableMappingTimestampTz);
    }

    public IcebergSchemaCacheKey(NameMapping nameMapping, String tableUuid, long schemaId, int partitionSpecId,
            int projectionSchemaId, boolean enableMappingVarbinary, boolean enableMappingTimestampTz) {
        super(nameMapping);
        this.tableUuid = java.util.Objects.requireNonNull(tableUuid, "tableUuid can not be null");
        this.schemaId = schemaId;
        this.partitionSpecId = partitionSpecId;
        this.projectionSchemaId = projectionSchemaId;
        this.enableMappingVarbinary = enableMappingVarbinary;
        this.enableMappingTimestampTz = enableMappingTimestampTz;
    }

    public Optional<String> getTableUuid() {
        return tableUuid.isEmpty() ? Optional.empty() : Optional.of(tableUuid);
    }

    public long getSchemaId() {
        return schemaId;
    }

    public int getPartitionSpecId() {
        return partitionSpecId;
    }

    public int getProjectionSchemaId() {
        return projectionSchemaId;
    }

    public boolean isEnableMappingVarbinary() {
        return enableMappingVarbinary;
    }

    public boolean isEnableMappingTimestampTz() {
        return enableMappingTimestampTz;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergSchemaCacheKey)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        IcebergSchemaCacheKey that = (IcebergSchemaCacheKey) o;
        return schemaId == that.schemaId
                && partitionSpecId == that.partitionSpecId
                && projectionSchemaId == that.projectionSchemaId
                && enableMappingVarbinary == that.enableMappingVarbinary
                && enableMappingTimestampTz == that.enableMappingTimestampTz
                && tableUuid.equals(that.tableUuid);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), tableUuid, schemaId, partitionSpecId, projectionSchemaId,
                enableMappingVarbinary, enableMappingTimestampTz);
    }
}
