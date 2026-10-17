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

// information_schema.columns reports a VARBINARY column with its type and its length in bytes, as DESC shows it,
// instead of "unknown". Doris tables cannot hold VARBINARY, so the columns come from a view.
suite("test_varbinary_information_schema") {
    sql "DROP VIEW IF EXISTS varbinary_information_schema_view"
    sql """CREATE VIEW varbinary_information_schema_view AS SELECT
           cast('abc' AS varbinary(16)) AS b16, cast('abc' AS varbinary) AS b_unbounded,
           cast('abc' AS varchar(10)) AS s10"""
    order_qt_desc "DESC varbinary_information_schema_view"
    order_qt_columns """SELECT ORDINAL_POSITION, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH,
           CHARACTER_OCTET_LENGTH, COLUMN_SIZE
           FROM information_schema.columns
           WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'varbinary_information_schema_view'"""
}
