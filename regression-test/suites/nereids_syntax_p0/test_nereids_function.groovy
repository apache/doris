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

suite("test_nereids_function") {
    sql "SET enable_nereids_planner=true"

    sql """
        DROP TABLE IF EXISTS sequence_match_test1
       """

    sql """
        CREATE TABLE sequence_match_test1(
                    `uid` int COMMENT 'user id',
                    `date` datetime COMMENT 'date time', 
                    `number` int NULL COMMENT 'number' 
                    )
        DUPLICATE KEY(uid) 
        DISTRIBUTED BY HASH(uid) BUCKETS 3 
        PROPERTIES ( 
            "replication_num" = "1"
        ); 
        """

    sql """
    INSERT INTO sequence_match_test1(uid, date, number) values (1, '2022-11-02 10:41:00', 1),
                                                   (2, '2022-11-02 11:41:00', 7),
                                                   (3, '2022-11-02 16:15:01', 3),
                                                   (4, '2022-11-02 19:05:04', 4),
                                                   (5, '2022-11-02 21:24:12', 5);
    """

    sql "SET enable_fallback_to_original_planner=false"

    sql """sync"""
    
    qt_match """
        SELECT sequence_match('(?1)(?2)', date, number = 1, number = 7) FROM sequence_match_test1;
    """

    qt_not_match """
        SELECT sequence_match('(?1)(?t>3600)(?2)', date, number = 1, number = 7) FROM sequence_match_test1;
    """

}
