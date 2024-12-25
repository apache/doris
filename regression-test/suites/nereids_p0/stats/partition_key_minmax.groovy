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

suite("partition_key_minmax") {
    sql """
        drop table if exists rangetable;
        create table rangetable (a int,
        b int,
        c int)
        partition by range (a, b)
        (partition p1 values [("1", "2"), ("10", "20")),
        partition p2 values [("20", "100"), ("30", "200")),
        partition p3 values [("300", "-1"), ("400", "1000")) 
        )
        distributed by hash(a) properties("replication_num"="1");

        insert into rangetable values (5, 3, 0), (22, 150, 1), (333, 1, 2),(6, 1, 3);

        analyze table rangetable with sync;
    """
    def columnStats = sql """show column cached stats rangetable"""
    logger.info("rangetable cached stats: " + columnStats)
    explain {
        sql """memo plan
            select * from rangetable where a < 250;
            """
        containsAny("a#0 -> ndv=2.6667, min=5.000000(5), max=30.000000(30), count=2.6667")
        containsAny("a#0 -> ndv=3, min=5.000000(5), max=30.000000(30), count=2.6667")
    }

    sql """
    drop table if exists listtable;
    create table listtable(id int, city varchar(20), value int)
    PARTITION BY LIST(id, city)
    (
        PARTITION p1_city VALUES IN (("1", "Beijing"), ("1", "Shanghai")),
        PARTITION p2_city VALUES IN (("2", "Beijing"), ("2", "Shanghai")),
        PARTITION p3_city VALUES IN (("3", "Beijing"), ("3", "Shanghai"))
    )
    distributed by hash(id) properties("replication_num"="1");

    insert into listtable values (1, "Beijing", 0), (2, "Beijing", 0), (3, "Beijing", 0);

    analyze table listtable with sync;
    """

    columnStats = sql """show column cached stats listtable"""
    logger.info("listtable cached stats: " + columnStats)

    explain {
        sql """
         memo plan select * from listtable where id >=3;
        """
        containsAny("id#0 -> ndv=1.0000, min=3.000000(3), max=3.000000(3), count=1.0000,")
        containsAny("id#0 -> ndv=1.0000, min=1.000000(1), max=3.000000(3), count=1.0000,")
    }
}

