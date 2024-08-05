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

suite("test_convert_tz") {
    sql """
        CREATE TABLE `cvt_tz` (
            `rowid` int NULL,
            `dt` datetime NULL,
            `property_value` varchar(65533) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`rowid`, `dt`, `property_value`)
        DISTRIBUTED BY HASH(`rowid`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """insert into cvt_tz select number, "2024-04-18 23:20:00", NULL from numbers("number" = "20"); """

    order_qt_sql1 """
        select convert_tz(dt, '+00:00', IF(property_value IS NULL, '+00:00', property_value)) from cvt_tz
    """
}