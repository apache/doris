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

suite("partitionRowCount") {
    sql """
        drop table if exists partitionRowCountTable;
        create table partitionRowCountTable (a int,
        b int,
        c int)
        partition by range (a)
        (partition p1 values [("1", "2"), ("10", "20")),
        partition p2 values [("20", "100"), ("30", "200")),
        partition p3 values [("300", "-1"), ("400", "1000")) 
        )
        distributed by hash(a) properties("replication_num"="1");
        insert into partitionRowCountTable values (5, 3, 0), (22, 150, 1), (333, 1, 2);
        insert into partitionRowCountTable values (5, 3, 10), (22, 150, 11), (333, 1, 12);
        analyze table partitionRowCountTable with sync;
    """
    explain {
        sql """physical plan
            select * from partitionRowCountTable where a < 250;
            """
        contains("PhysicalOlapScan[partitionRowCountTable partitions(2/3)]@0 ( stats=4 )")
    }

}