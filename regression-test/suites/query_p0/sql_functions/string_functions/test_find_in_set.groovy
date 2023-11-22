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

suite("test_find_in_set") {

    def tbName = "test_find_in_set"
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName} (
                 timea varchar(30)  NULL,
                 creatr varchar(30) NULL
            )
            DISTRIBUTED BY HASH(timea) BUCKETS 5 properties("replication_num" = "1");
        """
    sql """
        INSERT INTO ${tbName} VALUES  ('1 ','a'),('2  ','b'), ('1,2,3','d'),(null,'c');
        """

    // LIKE as predicate
    qt_sql "select * from ${tbName} where find_in_set(\"1\", timea) > 0;"
    qt_sql "select * from ${tbName} where find_in_set(\"1 \", timea) > 0;"
    qt_sql "select *, find_in_set(\"1 \", timea) from ${tbName} order by 1;"

    sql "DROP TABLE ${tbName};"
}