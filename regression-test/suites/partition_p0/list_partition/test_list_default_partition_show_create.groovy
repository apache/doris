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

suite("test_list_default_partition_show_create") {
    sql "drop table if exists list_default"
    sql """
        CREATE TABLE IF NOT EXISTS list_default ( 
            k1 int NOT NULL, 
            k2 int NOT NULL) 
        UNIQUE KEY(k1)
        PARTITION BY LIST(k1) ( 
            PARTITION p1 ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")
        """

    def res = sql "show create table list_default"
    logger.info(res[0][1])
    assertTrue(res[0][1].contains("(PARTITION p1)"))
}
