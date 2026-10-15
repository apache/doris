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

package org.apache.iceberg;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** A data-table scan that keeps partition specs bound to the scan's schema. */
public final class SchemaAwareDataTableScan extends DataTableScan {

    private SchemaAwareDataTableScan(Table table, Schema schema, TableScanContext context) {
        super(table, schema, context);
    }

    public static TableScan newScan(Table table) {
        // Inherit the context of the table's own scan: it carries the table's metrics reporter (for example a REST
        // catalog's scan reports), so a historical scan reports like a current one. A catalog-specific Table may
        // return its own scan type; such a table keeps the plain context it had before.
        TableScan tableScan = table.newScan();
        TableScanContext context = tableScan instanceof DataTableScan
                ? ((DataTableScan) tableScan).context() : TableScanContext.empty();
        return new SchemaAwareDataTableScan(table, table.schema(), context);
    }

    /** Returns the selected snapshot's specs bound to the scan schema. */
    public static Map<Integer, PartitionSpec> specsFor(TableScan scan) {
        Table table = scan.table();
        Schema schema = scan.schema();
        Map<Integer, PartitionSpec> tableSpecs = table.specs();
        if (schema.sameSchema(table.schema())) {
            return tableSpecs;
        }

        Map<Integer, PartitionSpec> specs = new LinkedHashMap<>();
        Snapshot snapshot = scan.snapshot();
        if (snapshot != null) {
            // Later, unused specs can have partition names that conflict with historical columns.
            // Bind only specs referenced by this snapshot, including those needed for delete files.
            for (ManifestFile manifest : snapshot.allManifests(table.io())) {
                specs.computeIfAbsent(manifest.partitionSpecId(),
                        id -> tableSpecs.get(id).toUnbound().bind(schema, true));
            }
        }
        return Collections.unmodifiableMap(specs);
    }

    @Override
    protected Map<Integer, PartitionSpec> specs() {
        // A metadata-only schema commit preserves the current snapshot ID, so schema identity—not snapshot
        // identity—must decide whether historical partition specs need rebinding.
        return specsFor(this);
    }

    @Override
    protected TableScan newRefinedScan(Table table, Schema schema, TableScanContext context) {
        return new SchemaAwareDataTableScan(table, schema, context);
    }
}
