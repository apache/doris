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

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.write.ConnectorWriteTarget;

import java.util.List;
import java.util.Optional;

/** Explicit INSERT target; its schema is independent of any read snapshot of the same table. */
final class IcebergWriteTarget implements ConnectorWriteTarget {
    private final IcebergTableHandle tableHandle;
    private final IcebergWriteSchemaContext schemaContext;
    private final String metadataIdentity;

    IcebergWriteTarget(IcebergTableHandle tableHandle, IcebergWriteSchemaContext schemaContext,
            String metadataIdentity) {
        this.tableHandle = tableHandle;
        this.schemaContext = schemaContext;
        this.metadataIdentity = metadataIdentity;
    }

    @Override
    public IcebergTableHandle getTableHandle() {
        return tableHandle;
    }

    @Override
    public Optional<String> getBranchName() {
        return schemaContext.getBranchName();
    }

    @Override
    public List<ConnectorColumn> getColumns() {
        return schemaContext.getColumns();
    }

    @Override
    public List<String> getPartitionColumnNames() {
        return IcebergPartitionUtils.identityPartitionSourceColumns(
                schemaContext.getSchema(), schemaContext.getPartitionSpec());
    }

    @Override
    public String getMetadataIdentity() {
        return metadataIdentity;
    }

    IcebergWriteSchemaContext getSchemaContext() {
        return schemaContext;
    }
}
