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

suite("test_like_no_backslash_escapes_mode") {

    sql """ set sql_mode = "NO_BACKSLASH_ESCAPES";  """
    def tbName = "test_like_no_backslash_escapes_mode_tbl"
    sql "DROP TABLE IF EXISTS ${tbName}"

    sql """
        CREATE TABLE `${tbName}` (
          `id` INT NULL,
          `value` VARCHAR(100) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO ${tbName} VALUES (1, "TIN\\PEXNB601C6UUTAB");
    """

    qt_select1 """
        select * from ${tbName} where `value` like "%TIN\\PE%";
    """

    qt_select2 """
        select * from ${tbName} where `value` = "TIN\\PEXNB601C6UUTAB";
    """

    // sql "DROP TABLE ${tbName};"
}