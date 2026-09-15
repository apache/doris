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
        return new SchemaAwareDataTableScan(table, table.schema(), TableScanContext.empty());
    }

    /** Returns every table spec rebound to {@code schema}. */
    public static Map<Integer, PartitionSpec> specsFor(Table table, Schema schema) {
        if (schema.sameSchema(table.schema())) {
            return table.specs();
        }

        Map<Integer, PartitionSpec> specs = new LinkedHashMap<>();
        table.specs().forEach((id, spec) -> specs.put(id, spec.toUnbound().bind(schema, true)));
        return Collections.unmodifiableMap(specs);
    }

    @Override
    protected Map<Integer, PartitionSpec> specs() {
        // A metadata-only schema commit preserves the current snapshot ID, so schema identity—not snapshot
        // identity—must decide whether historical partition specs need rebinding.
        return specsFor(table(), tableSchema());
    }

    @Override
    protected TableScan newRefinedScan(Table table, Schema schema, TableScanContext context) {
        return new SchemaAwareDataTableScan(table, schema, context);
    }
}
