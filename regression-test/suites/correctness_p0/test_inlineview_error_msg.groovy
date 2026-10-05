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

suite("test_inlineview_error_msg") {
    sql """
        drop table if exists tmp_can_drop_t1;
    """

    sql """
        drop table if exists tmp_can_drop_t2;
    """

    sql """
        create table tmp_can_drop_t1 (
            cust_id varchar(96),
            user_id varchar(96)
        )
        DISTRIBUTED by random BUCKETS 1
        PROPERTIES(
            "replication_num" = "1"
        );
    """

    sql """
        create table tmp_can_drop_t2 (
            cust_id varchar(96),
            usr_id varchar(96)
        )
        DISTRIBUTED by random BUCKETS 1
        PROPERTIES(
            "replication_num" = "1"
        );
    """
    test {
            sql """
            select
            a.cust_id,
            a.usr_id
            from (
            select
                a.cust_id,
                a.usr_id,
                a.user_id
            from tmp_can_drop_t1 a
            full join (
                select
                cust_id,
                usr_id
                from
                tmp_can_drop_t2
            ) b
            on b.cust_id = a.cust_id
            ) a;
        """
        exception "Unknown column 'usr_id' in 'a'"
    }
    
    sql """
        drop table if exists tmp_can_drop_t1;
    """

    sql """
        drop table if exists tmp_can_drop_t2;
    """
}
