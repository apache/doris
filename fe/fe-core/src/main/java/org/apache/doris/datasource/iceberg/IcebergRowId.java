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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;

import java.util.ArrayList;

/**
 * Iceberg hidden row-id column definition for __DORIS_ICEBERG_ROWID_COL__.
 */
public final class IcebergRowId {
    private static final Type ROW_ID_TYPE = createRowIdType();
    private static final Column ROW_ID_COLUMN = createRowIdColumn();

    private IcebergRowId() {}

    public static Type getRowIdType() {
        return ROW_ID_TYPE;
    }

    // Shared instance; do not mutate.
    public static Column createHiddenColumn() {
        return ROW_ID_COLUMN;
    }

    private static Column createRowIdColumn() {
        Column column = new Column(
                Column.ICEBERG_ROWID_COL,
                ROW_ID_TYPE,
                false,
                null,
                false,
                null,
                "Iceberg row position metadata");
        column.setIsVisible(false);
        return column;
    }

    private static Type createRowIdType() {
        ArrayList<StructField> fields = new ArrayList<>();
        fields.add(new StructField("file_path", ScalarType.createStringType()));
        fields.add(new StructField("row_position", ScalarType.createType(PrimitiveType.BIGINT)));
        fields.add(new StructField("partition_spec_id", ScalarType.createType(PrimitiveType.INT)));
        fields.add(new StructField("partition_data", ScalarType.createStringType()));
        return new StructType(fields);
    }
}
