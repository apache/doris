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

suite("select_distinct", "arrow_flight_sql") {
    sql """DROP TABLE IF EXISTS decimal_a;"""
    sql """DROP TABLE IF EXISTS decimal_b;"""
    sql """DROP TABLE IF EXISTS decimal_c;"""

    sql """
    CREATE TABLE IF NOT EXISTS `decimal_a` (
      `id` int(11) NOT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    CREATE TABLE IF NOT EXISTS `decimal_b` (
      `id` int(11) NOT NULL,
      `age` decimal(11, 3) NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    CREATE TABLE IF NOT EXISTS `decimal_c` (
      `id` int(11) NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 64
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """insert into decimal_a values(1);"""
    sql """insert into decimal_b values (1, 5.3);"""
    sql """insert into decimal_c values(1);"""

    qt_distinct_decimal_cast """
    select distinct
      decimal_a.id,
      case
        when decimal_b.age >= 0 then decimal_b.age
        when decimal_b.age >= 0 then floor(decimal_b.age/365)
      end
    from
      decimal_a
      inner join decimal_b on decimal_a.id =decimal_b.id
      left join decimal_c on decimal_a.id=decimal_c.id;
    """

    sql """DROP TABLE IF EXISTS decimal_a;"""
    sql """DROP TABLE IF EXISTS decimal_b;"""
    sql """DROP TABLE IF EXISTS decimal_c;"""
}
