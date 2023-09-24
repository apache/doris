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

package org.apache.doris.alter;

public enum AlterOpType {
    // rollup
    ADD_ROLLUP,
    DROP_ROLLUP,
    // schema change
    SCHEMA_CHANGE,
    // partition
    ADD_PARTITION,
    DROP_PARTITION,
    REPLACE_PARTITION,
    MODIFY_PARTITION,
    // rename
    RENAME,
    // table property
    MODIFY_TABLE_PROPERTY,
    // Some operations are performed synchronously, so we distinguish them by suffix _SYNC
    MODIFY_TABLE_PROPERTY_SYNC,
    // others operation, such as add/drop backend. currently, we do not care about them
    ALTER_OTHER,
    ENABLE_FEATURE,
    REPLACE_TABLE,
    MODIFY_DISTRIBUTION,
    MODIFY_TABLE_COMMENT,
    MODIFY_COLUMN_COMMENT,
    MODIFY_ENGINE,
    INVALID_OP; // INVALID_OP must be the last one

    // true means 2 operations have no conflict.
    public static Boolean[][] COMPATIBILITY_MATRIX;

    static {
        COMPATIBILITY_MATRIX = new Boolean[INVALID_OP.ordinal() + 1][INVALID_OP.ordinal() + 1];
        for (int i = 0; i < INVALID_OP.ordinal(); i++) {
            for (int j = 0; j < INVALID_OP.ordinal(); j++) {
                COMPATIBILITY_MATRIX[i][j] = false;
            }
        }

        // rollup can be added or dropped in batch
        COMPATIBILITY_MATRIX[ADD_ROLLUP.ordinal()][ADD_ROLLUP.ordinal()] = true;
        COMPATIBILITY_MATRIX[DROP_ROLLUP.ordinal()][DROP_ROLLUP.ordinal()] = true;
        // schema change, such as add/modify/drop columns can be processed in batch
        COMPATIBILITY_MATRIX[SCHEMA_CHANGE.ordinal()][SCHEMA_CHANGE.ordinal()] = true;
        // can modify multi column comments at same time
        COMPATIBILITY_MATRIX[MODIFY_COLUMN_COMMENT.ordinal()][MODIFY_COLUMN_COMMENT.ordinal()] = true;
        // can drop multi partition at same time
        COMPATIBILITY_MATRIX[DROP_PARTITION.ordinal()][DROP_PARTITION.ordinal()] = true;

    }

    public boolean needCheckCapacity() {
        return this == ADD_ROLLUP || this == SCHEMA_CHANGE || this == ADD_PARTITION || this == ENABLE_FEATURE;
    }

}
