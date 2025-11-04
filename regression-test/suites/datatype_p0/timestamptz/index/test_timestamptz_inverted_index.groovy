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

suite("test_timestamptz_inverted_index") {
    sql "set time_zone = '+08:00'; "

    sql """
        DROP TABLE IF EXISTS `timestamptz_inverted_index`;
    """
    sql """
        CREATE TABLE `timestamptz_inverted_index` (
          `id` INT,
          `name` VARCHAR(50),
          `ts_tz` TIMESTAMPTZ,
          INDEX idx_tz (`ts_tz`) USING INVERTED
        ) 
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    sql """INSERT INTO timestamptz_inverted_index VALUES
    (1, 'name1', '2023-01-01 12:00:00 +03:00'),
    (2, 'name2', '2023-02-02 12:00:00 +03:00'),
    (3, 'name3', '2023-03-03 12:00:00 -05:00');
    """

    qt_inverted_index """
        SELECT * FROM timestamptz_inverted_index WHERE ts_tz = '2023-02-02 12:00:00 +03:00';
    """
}