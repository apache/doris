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


// Checklist: D06 D07 E09 J08.
suite("test_uuid_window_batches", "p0") {

    sql "DROP TABLE IF EXISTS uuid_window_batches"
    sql """CREATE TABLE uuid_window_batches (id INT,u UUID)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3 PROPERTIES('replication_num'='1')"""
    sql """INSERT INTO uuid_window_batches SELECT number,
           IF(number % 1024 = 0,NULL,CAST(LPAD(HEX(number),32,'0') AS UUID))
           FROM numbers('number'='4097')"""
    // Evaluate the window before selecting rows around block and partition boundaries.
    qt_boundaries """SELECT * FROM (
          SELECT id,u,LAG(u) OVER(ORDER BY id) prev,LEAD(u) OVER(ORDER BY id) next,
                 MIN(u) OVER(ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) next_min,
                 FIRST_VALUE(u) OVER(PARTITION BY id DIV 1024 ORDER BY id) first_u
          FROM uuid_window_batches) w
          WHERE id IN (0,1,1023,1024,1025,2047,2048,2049,4095,4096) ORDER BY id"""

}
