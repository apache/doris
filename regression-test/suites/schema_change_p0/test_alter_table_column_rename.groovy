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

suite("test_alter_table_column_rename") {
    def tbName = "alter_table_column_rename"

    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName} (
                k1 INT NOT NULL,
                value1 varchar(16) NOT NULL,
                value2 int NOT NULL
            )
            DUPLICATE KEY (k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1 properties("replication_num" = "1");
        """

    sql """ insert into ${tbName} values (1, 'a', 2) """
    sql """ select * from ${tbName} """

    // rename column name
    sql """ ALTER TABLE ${tbName} RENAME COLUMN value2 new_col """

    List<List<Object>> result = sql """ show frontends """
    for (row : result) {
        //println row
        String jdbcUrl = "jdbc:mysql://" + row[1] + ":" + row[4]
        def result1 = connect(user = 'root', password = '', jdbcUrl) {
            sql """ SYNC """
            sql """ use regression_test_schema_change_p0 """
            sql """ select * from ${tbName} where new_col = 2 """
        }
    }

}
