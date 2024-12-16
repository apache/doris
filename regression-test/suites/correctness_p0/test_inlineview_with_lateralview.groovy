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

suite("test_inlineview_with_lateralview") {
    sql """
        drop table if exists lateralview_t1;
    """

    sql """
        create table lateralview_t1 (
            ip varchar(96)
        )
        DISTRIBUTED by random BUCKETS 1
        PROPERTIES(
            "replication_num" = "1"
        );
    """
    sql """insert into lateralview_t1 values ('1');"""

    qt_select """SELECT max(position)
                        OVER (partition by idf) c_tk
                    FROM 
                        (SELECT position,
                            idf
                        FROM 
                            (SELECT lag(position,
                            1,
                            0)
                                OVER (partition by position) last_position, position, idf, gap
                            FROM 
                                (SELECT idf,
                            position + 1 position,
                            gap
                                FROM 
                                    (SELECT ip AS idf,
                            120 AS gap
                                    FROM lateralview_t1 ) pro_actions lateral view explode_numbers(5) exp_tp AS position) events) last_position_events ) ttt;"""
    
    sql """
        drop table if exists lateralview_t1;
    """

}
