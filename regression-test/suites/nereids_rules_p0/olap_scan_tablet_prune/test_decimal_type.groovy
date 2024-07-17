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
suite("test_decimal_type") {

    sql """
        drop table if exists test_decimal_type
    """

    sql """
        create table test_decimal_type (c1 decimal(8, 0), c2 decimal(18, 0)) duplicate key(c1, c2) distributed by hash(c1, c2) buckets 10 properties("replication_num"="1");
    """

    sql """
        insert into test_decimal_type values (20240710, 110), (20240720,220), (20240120, 120), (20240730, 330), (20240740,440),(20240750,550);
    """

    sql """
        sync
    """

    order_qt_test_decimal_prune """
        select * from test_decimal_type where c1 = 20240750 and c2 = 550
    """
}
