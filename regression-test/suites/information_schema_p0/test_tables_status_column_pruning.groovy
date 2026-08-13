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

suite("test_tables_status_column_pruning") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    def assertNoTabletStatusColumns = {
        notContains "col=TABLE_ROWS"
        notContains "col=DATA_LENGTH"
        notContains "col=AVG_ROW_LENGTH"
        notContains "col=INDEX_LENGTH"
    }

    explain {
        verbose true
        sql """
            SELECT UPDATE_TIME
            FROM information_schema.tables
            WHERE TABLE_SCHEMA = 'information_schema'
        """
        contains "TABLE: information_schema.tables"
        contains "final projections: UPDATE_TIME"
        contains "col=TABLE_SCHEMA"
        contains "col=UPDATE_TIME"
        notContains "col=TABLE_NAME"
        assertNoTabletStatusColumns.delegate = delegate
        assertNoTabletStatusColumns.call()
    }

    explain {
        verbose true
        sql """
            SELECT TABLE_NAME, UPDATE_TIME
            FROM information_schema.tables
            WHERE TABLE_SCHEMA = 'information_schema'
        """
        contains "TABLE: information_schema.tables"
        contains "final projections: TABLE_NAME"
        contains "UPDATE_TIME"
        contains "col=TABLE_SCHEMA"
        contains "col=TABLE_NAME"
        contains "col=UPDATE_TIME"
        assertNoTabletStatusColumns.delegate = delegate
        assertNoTabletStatusColumns.call()
    }

    explain {
        verbose true
        sql """
            SELECT TABLE_NAME, UPDATE_TIME
            FROM information_schema.tables
            WHERE TABLE_SCHEMA = 'information_schema'
              AND TABLE_NAME = 'tables'
        """
        contains "TABLE: information_schema.tables"
        contains "final projections: TABLE_NAME"
        contains "UPDATE_TIME"
        contains "col=TABLE_SCHEMA"
        contains "col=TABLE_NAME"
        contains "col=UPDATE_TIME"
        assertNoTabletStatusColumns.delegate = delegate
        assertNoTabletStatusColumns.call()
    }

    explain {
        verbose true
        sql """
            SELECT TABLE_NAME, ENGINE, VERSION, ROW_FORMAT, TABLE_ROWS, AVG_ROW_LENGTH,
                   DATA_LENGTH, MAX_DATA_LENGTH, INDEX_LENGTH, DATA_FREE, AUTO_INCREMENT,
                   CREATE_TIME, UPDATE_TIME, CHECK_TIME, TABLE_COLLATION, CHECKSUM,
                   CREATE_OPTIONS, TABLE_COMMENT
            FROM information_schema.tables
            WHERE TABLE_SCHEMA = 'information_schema'
              AND TABLE_NAME LIKE 'tables'
        """
        contains "TABLE: information_schema.tables"
        contains "col=TABLE_ROWS"
        contains "col=DATA_LENGTH"
        contains "col=AVG_ROW_LENGTH"
        contains "col=INDEX_LENGTH"
    }
}
