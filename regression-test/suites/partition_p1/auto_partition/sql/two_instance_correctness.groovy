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

suite("two_instance_correctness") {

    // finish time of instances have diff
    sql "DROP TABLE IF EXISTS two_bkt;"
    sql """
        create table two_bkt(
            k0 date not null
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS 2
        properties("replication_num" = "1");
    """

    sql """ insert into two_bkt values ("2012-12-11"); """
    sql """ insert into two_bkt select "2020-12-12" from numbers("number" = "20000"); """

    sql " DROP TABLE IF EXISTS two_bkt_dest; "
    sql """
        create table two_bkt_dest(
            k0 date not null
        )
        AUTO PARTITION BY RANGE (date_trunc(k0, 'day')) ()
        DISTRIBUTED BY HASH(`k0`) BUCKETS 10
        properties("replication_num" = "1");
    """
    sql " insert into two_bkt_dest select * from two_bkt; "

    qt_sql " select count(distinct k0) from two_bkt_dest; "
}
