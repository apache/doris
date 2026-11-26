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

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

/**
 * The {@code t$labels} system table: the catalog-provided labels of an iceberg table loaded from a REST catalog
 * (apache/iceberg#15750), one row per object-level or field-level label.
 *
 * <p>Unlike every other iceberg system table it is not a {@link org.apache.iceberg.MetadataTableType}: labels
 * come from the REST load response, not from the table metadata, are not versioned with snapshots, and only a
 * REST catalog returns them. Rows reflect the table load the statement read, so they follow the iceberg table
 * cache like the rest of the table's state. The schema mirrors iceberg's proposed {@code LabelsTable}.
 */
final class IcebergLabelsSysTable {
    static final String NAME = "labels";

    static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "scope", Types.StringType.get()),
            Types.NestedField.optional(2, "field_id", Types.IntegerType.get()),
            Types.NestedField.optional(3, "field_name", Types.StringType.get()),
            Types.NestedField.required(4, "key", Types.StringType.get()),
            Types.NestedField.required(5, "value", Types.StringType.get()));

    private IcebergLabelsSysTable() {
    }

    /** Whether {@code handle} is a {@code $labels} handle; sys handles carry the lower-cased name. */
    static boolean isLabels(IcebergTableHandle handle) {
        return NAME.equals(handle.getSysTableName());
    }
}
