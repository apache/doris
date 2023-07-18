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

suite("test_show_decimalv3") {
    sql """DROP TABLE IF EXISTS showdb """
    sql """
        CREATE TABLE IF NOT EXISTS showdb (
            `id` int(11) NOT NULL ,
            `dd` decimal(15,6)
        )
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
        "enable_unique_key_merge_on_write" = "true",
        "replication_num" = "1")
    """
    qt_select1 """desc showdb"""
    qt_select2 """desc showdb all"""

    sql """DROP TABLE IF EXISTS showdb """
    sql """
        CREATE TABLE IF NOT EXISTS showdb (
            `id` int(11) NOT NULL ,
            `dd` decimal
        )
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
        "enable_unique_key_merge_on_write" = "true",
        "replication_num" = "1")
    """
    qt_select3 """desc showdb"""
    qt_select4 """desc showdb all"""

}
