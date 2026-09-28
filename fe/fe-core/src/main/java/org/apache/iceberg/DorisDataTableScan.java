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

import java.util.HashMap;
import java.util.Map;

/** Keeps partition pruning aligned with the schema selected by the Doris relation. */
public class DorisDataTableScan extends DataTableScan {
    private DorisDataTableScan(Table table, Schema schema, TableScanContext context) {
        super(table, schema, context);
    }

    public static TableScan wrap(TableScan scan) {
        if (!(scan instanceof DataTableScan)) {
            return scan;
        }
        DataTableScan dataScan = (DataTableScan) scan;
        return new DorisDataTableScan(dataScan.table(), dataScan.tableSchema(), dataScan.context());
    }

    /** Rebinds each spec by field ID to the full schema selected for the scan. */
    public static Map<Integer, PartitionSpec> specsForScan(TableScan scan) {
        Map<Integer, PartitionSpec> tableSpecs = scan.table().specs();
        Schema schema = scan.schema();
        if (tableSpecs.values().stream().allMatch(spec -> spec.schema() == schema)) {
            return tableSpecs;
        }
        Map<Integer, PartitionSpec> specs = new HashMap<>();
        // A schema-only update keeps the snapshot ID unchanged. Snapshot IDs therefore cannot
        // determine whether table specs still use the schema selected by a historical query.
        tableSpecs.forEach((id, spec) ->
                specs.put(id, spec.schema() == schema ? spec : spec.toUnbound().bind(schema, true)));
        return specs;
    }

    @Override
    protected Map<Integer, PartitionSpec> specs() {
        return specsForScan(this);
    }

    @Override
    protected TableScan newRefinedScan(Table table, Schema schema, TableScanContext context) {
        return new DorisDataTableScan(table, schema, context);
    }
}
