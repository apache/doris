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

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.thrift.TDeltaLakeSysTableColumn;

import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

import java.util.List;
import java.util.Map;

final class DeltaLakePropertiesSystemTable extends DeltaLakeSnapshotSystemTable {
    DeltaLakePropertiesSystemTable() {
        super("properties", new StructType().add("key", StringType.STRING).add("value", StringType.STRING));
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorSession session, DeltaLakeRuntime runtime,
            DeltaLakeTableHandle handle, ConnectorScanRequest request) {
        List<TDeltaLakeSysTableColumn> columns = projectedColumns(runtime, handle, request);
        var rows = handle.snapshot(runtime).getTableProperties().entrySet().stream()
                .sorted(Map.Entry.comparingByKey()).map(entry -> DeltaLakeSystemTableRows.row(columns,
                        col -> DeltaLakeSystemTableRows.text(col.equals("key") ? entry.getKey() : entry.getValue())));
        return DeltaLakeSystemTableRows.pack(name(), columns, rows.iterator(), request.getScanContext());
    }
}
