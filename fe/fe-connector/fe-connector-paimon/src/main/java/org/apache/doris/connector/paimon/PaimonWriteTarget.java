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

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.write.ConnectorWriteTarget;

import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** One branch's current schema and writer table, independent of the statement's read snapshot. */
final class PaimonWriteTarget implements ConnectorWriteTarget {
    private final PaimonTableHandle tableHandle;
    private final FileStoreTable table;
    private final Optional<String> branchName;
    private final List<ConnectorColumn> columns;
    private final String metadataIdentity;

    PaimonWriteTarget(PaimonTableHandle tableHandle, FileStoreTable table, TableSchema persistedSchema,
            Optional<String> branchName, List<ConnectorColumn> columns) {
        this.tableHandle = tableHandle;
        this.table = table;
        this.branchName = branchName;
        this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
        this.metadataIdentity = identity(table, persistedSchema);
    }

    @Override
    public PaimonTableHandle getTableHandle() {
        return tableHandle;
    }

    @Override
    public Optional<String> getBranchName() {
        return branchName;
    }

    @Override
    public List<ConnectorColumn> getColumns() {
        return columns;
    }

    @Override
    public List<String> getPartitionColumnNames() {
        return table.schema().partitionKeys();
    }

    @Override
    public String getMetadataIdentity() {
        return metadataIdentity;
    }

    FileStoreTable getTable() {
        return table;
    }

    void validateCurrentSchema() {
        TableSchema latest = table.schemaManager().latest().orElseThrow(() ->
                new DorisConnectorException("Paimon write target schema disappeared: "
                        + tableHandle + "; retry the statement"));
        if (!metadataIdentity.equals(identity(table, latest))) {
            throw new DorisConnectorException("Paimon write target schema changed: "
                    + tableHandle + "; retry the statement");
        }
    }

    private static String identity(FileStoreTable table, TableSchema schema) {
        return table.schemaManager().toSchemaPath(schema.id()) + "\n" + PaimonSchemaPin.schemaDigest(schema);
    }
}
