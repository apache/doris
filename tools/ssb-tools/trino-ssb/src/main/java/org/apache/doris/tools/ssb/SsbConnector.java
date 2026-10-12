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

package org.apache.doris.tools.ssb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.transaction.IsolationLevel;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class SsbConnector implements Connector {
    // Keep the original tool's ten dbgen partitions: changing -C changes some random streams.
    static final int LINEORDER_PARTS = 10;
    private static final List<String> SCHEMAS = List.of("sf1", "sf100", "sf1000");
    private final Path dbgen;

    SsbConnector(Path dbgen) {
        this.dbgen = dbgen;
    }

    public enum Transaction implements ConnectorTransactionHandle {
        INSTANCE
    }

    public record Table(@JsonProperty("schema") String schema, @JsonProperty("name") String name)
            implements ConnectorTableHandle {
        @JsonCreator
        public Table {
            if (!SCHEMAS.contains(schema)) {
                throw new IllegalArgumentException("Supported SSB schemas: " + SCHEMAS);
            }
            SsbTable.fromName(name);
        }
    }

    public record Column(@JsonProperty("index") int index) implements ColumnHandle {
        @JsonCreator
        public Column {
        }
    }

    public record Split(@JsonProperty("part") int part, @JsonProperty("totalParts") int totalParts)
            implements ConnectorSplit {
        @JsonCreator
        public Split {
            if ((totalParts != 1 && totalParts != LINEORDER_PARTS) || part < 1 || part > totalParts) {
                throw new IllegalArgumentException("Invalid SSB partition " + part + "/" + totalParts);
            }
        }

        @Override
        @JsonIgnore
        public boolean isRemotelyAccessible() {
            return true;
        }

        @Override
        @JsonIgnore
        public List<HostAddress> getAddresses() {
            return List.of();
        }

        @Override
        @JsonIgnore
        public Object getInfo() {
            return Map.of("part", part, "totalParts", totalParts);
        }

        @Override
        @JsonIgnore
        public long getRetainedSizeInBytes() {
            return 24; // object header and two int fields
        }

        @Override
        @JsonIgnore
        public SplitWeight getSplitWeight() {
            return SplitWeight.standard();
        }
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolation, boolean readOnly, boolean autoCommit) {
        return Transaction.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction) {
        return new Metadata();
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return new ConnectorSplitManager() {
            @Override
            public io.trino.spi.connector.ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction,
                    ConnectorSession session, ConnectorTableHandle table, io.trino.spi.connector.DynamicFilter filter,
                    io.trino.spi.connector.Constraint constraint) {
                int count = SsbTable.fromName(((Table) table).name()) == SsbTable.LINEORDER ? LINEORDER_PARTS : 1;
                List<Split> splits = new ArrayList<>();
                for (int part = 1; part <= count; part++) {
                    splits.add(new Split(part, count));
                }
                return new FixedSplitSource(splits);
            }
        };
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return new ConnectorRecordSetProvider() {
            @Override
            public io.trino.spi.connector.RecordSet getRecordSet(ConnectorTransactionHandle transaction,
                    ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table,
                    List<? extends ColumnHandle> columns) {
                return new SsbRecordSet(dbgen, (Table) table, (Split) split,
                        columns.stream().map(column -> ((Column) column).index()).toList());
            }
        };
    }

    private static final class Metadata implements ConnectorMetadata {
        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return SCHEMAS;
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName table) {
            if (!SCHEMAS.contains(table.getSchemaName())) {
                return null;
            }
            for (SsbTable ssbTable : SsbTable.values()) {
                if (ssbTable.tableName().equals(table.getTableName())) {
                    return new Table(table.getSchemaName(), table.getTableName());
                }
            }
            return null;
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schema) {
            List<SchemaTableName> tables = new ArrayList<>();
            for (String name : SCHEMAS) {
                if (schema.isEmpty() || schema.get().equals(name)) {
                    for (SsbTable table : SsbTable.values()) {
                        tables.add(new SchemaTableName(name, table.tableName()));
                    }
                }
            }
            return tables;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle handle) {
            Table table = (Table) handle;
            return new ConnectorTableMetadata(new SchemaTableName(table.schema(), table.name()),
                    SsbTable.fromName(table.name()).columns);
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle handle) {
            List<ColumnMetadata> columns = getTableMetadata(session, handle).getColumns();
            Map<String, ColumnHandle> result = new LinkedHashMap<>();
            for (int i = 0; i < columns.size(); i++) {
                result.put(columns.get(i).getName(), new Column(i));
            }
            return result;
        }

        @Override
        public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle table,
                ColumnHandle column) {
            return getTableMetadata(session, table).getColumns().get(((Column) column).index());
        }

        @Override
        public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
                SchemaTablePrefix prefix) {
            Map<SchemaTableName, List<ColumnMetadata>> result = new LinkedHashMap<>();
            for (SchemaTableName table : listTables(session, prefix.getSchema())) {
                if (prefix.matches(table)) {
                    result.put(table, SsbTable.fromName(table.getTableName()).columns);
                }
            }
            return result;
        }
    }
}
