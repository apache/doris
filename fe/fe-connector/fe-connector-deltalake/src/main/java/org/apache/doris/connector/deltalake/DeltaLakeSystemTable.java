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


package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.handle.NamedColumnHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.thrift.TDeltaLakeSysTableColumn;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

interface DeltaLakeSystemTable {
    String name();

    ConnectorTableSchema schema(DeltaLakeTableHandle handle);

    boolean supportsTimeTravel();

    List<ConnectorScanRange> planScan(DeltaLakeRuntime runtime, DeltaLakeTableHandle handle,
            ConnectorScanRequest request);

    default List<TDeltaLakeSysTableColumn> projectedColumns(DeltaLakeTableHandle handle,
            ConnectorScanRequest request) {
        Set<String> names = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        // A pure COUNT(*) may carry an arbitrary minimum-width slot in its scan tuple.
        if (!request.isCountPushdown() || request.getFilter().isPresent()) {
            request.getColumns().forEach(column -> names.add(((NamedColumnHandle) column).getName()));
        }
        request.getFilter().ifPresent(filter -> addFilterColumns(filter, names));
        List<ConnectorColumn> schema = schema(handle).getColumns();
        List<TDeltaLakeSysTableColumn> columns = new ArrayList<>(names.size());
        for (int i = 0; i < schema.size(); i++) {
            String name = schema.get(i).getName();
            if (names.contains(name)) {
                columns.add(new TDeltaLakeSysTableColumn().setSchemaOrdinal(i).setName(name));
            }
        }
        return columns;
    }

    private static void addFilterColumns(ConnectorExpression filter, Set<String> names) {
        if (filter instanceof ConnectorColumnRef ref) {
            names.add(ref.getColumnName());
        }
        filter.getChildren().forEach(child -> addFilterColumns(child, names));
    }
}
