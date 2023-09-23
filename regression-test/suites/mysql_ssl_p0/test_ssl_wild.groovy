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

suite("test_ssl_wild") {
    def tbName = "tb_test_ssl_wild"
    int test_count = 20;
        sql "DROP TABLE IF EXISTS ${tbName}"
        // char not null to null
        sql """
            CREATE TABLE IF NOT EXISTS ${tbName} (
                col1 bigint(20) NULL,
                col2 bigint(20) NULL,
                col3 text NULL,
                col4 text NULL,
                col5 text NULL,
                col6 text NULL,
                col7 text NULL,
                col8 text NULL,
                col9 text NULL,
                col10 text NULL,
                col11 text NULL,
                col12 text NULL,
                col13 text NULL,
                col14 text NULL,
                col15 text NULL,
                col16 text NULL,
                col17 text NULL,
                col18 text NULL,
                col19 text NULL,
                col20 text NULL,
                col21 text NULL,
                col22 text NULL,
                col23 text NULL,
                col24 int(11) NULL,
                col25 decimal(20, 2) NULL,
                col26 decimal(22, 4) NULL,
                col27 decimal(16, 8) NULL,
                col28 decimal(16, 8) NULL,
                col29 decimal(16, 2) NULL,
                col30 decimal(16, 2) NULL,
                col31 decimal(16, 2) NULL,
                col32 decimal(16, 2) NULL,
                col33 decimal(16, 2) NULL,
                col34 decimal(16, 2) NULL,
                col35 decimal(16, 2) NULL,
                col36 decimal(16, 2) NULL,
                col37 decimal(16, 2) NULL,
                col38 decimal(16, 2) NULL,
                col39 decimal(16, 2) NULL,
                col40 decimal(16, 2) NULL,
                col41 decimal(16, 2) NULL,
                col42 decimal(16, 2) NULL,
                col43 decimal(16, 2) NULL,
                col44 decimal(16, 2) NULL,
                col45 decimal(16, 2) NULL,
                col46 decimal(16, 2) NULL,
                col47 int(11) NULL,
                col48 decimal(16, 2) NULL,
                col49 decimal(20, 2) NULL,
                col50 decimal(20, 2) NULL,
                col51 decimal(20, 2) NULL,
                col52 decimal(20, 2) NULL,
                col53 decimal(16, 2) NULL,
                col54 int(11) NULL,
                col55 int(11) NULL,
                col56 text NULL,
                col57 text NULL,
                col58 text NULL,
                col59 text NULL,
                col60 text NULL,
                col61 decimal(22, 6) NULL,
                col62 text NULL,
                col63 text NULL,
                col64 text NULL,
                col65 text NULL,
                col66 int(11) NULL
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(col1) BUCKETS 10
            properties("replication_num" = "1");
        """
        sql """insert into ${tbName}(col1) values(1) """
    while (test_count-- > 1) {
        StringBuilder insertCommand = new StringBuilder();
        insertCommand.append("INSERT INTO ${tbName} select * from ${tbName}");
        sql insertCommand.toString()
    }
    sql """select * from ${tbName}"""
}