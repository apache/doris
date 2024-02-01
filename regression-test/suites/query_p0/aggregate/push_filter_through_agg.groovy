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

suite("push_filter_through_agg") {
    sql """DROP TABLE IF EXISTS t_push_filter_through_agg"""
    sql """DROP VIEW IF EXISTS view_i"""
    sql """
        CREATE TABLE t_push_filter_through_agg (col1 varchar(11451) not null, col2 int not null, col3 int not null)
        UNIQUE KEY(col1)
        DISTRIBUTED BY HASH(col1)
        BUCKETS 3
        PROPERTIES(
            "replication_num"="1"
        );
    """

    sql """
        CREATE VIEW `view_i` AS 
        SELECT 
          `b`.`col1` AS `col1`, 
          `b`.`col2` AS `col2`
        FROM 
          (
            SELECT 
              `col1` AS `col1`, 
              sum(`cost`) AS `col2`
            FROM 
              (
                SELECT 
                  `col1` AS `col1`, 
                  sum(
                    CAST(`col3` AS INT)
                  ) AS `cost` 
                FROM 
                  `t_push_filter_through_agg` 
                GROUP BY 
                  `col1`
              ) a 
            GROUP BY 
              `col1`
          ) b;
    """

     qt_sql """
         SELECT SUM(`col2`) FROM view_i WHERE `col1` BETWEEN 10 AND 20 LIMIT 1;
     """
}
