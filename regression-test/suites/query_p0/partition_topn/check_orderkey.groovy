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

suite("check_orderkey") {
    sql """
    create table students02 (
        id int, age int, name varchar
    )properties ('replication_num'='1');

    insert into students02 values (1, 1, 'a'), (1, 2, 'b'), (1, 3, 'c');
    """
    //"ORDER BY age desc" and "ORDER BY age" is different, do not use partition topn
    explain {
        sql """
            select
            t.id,
            t.name,
            t.age,
            t.desc_age
            from (
            SELECT
                ROW_NUMBER() OVER (partition by id ORDER BY age desc) AS desc_age,
                ROW_NUMBER() OVER (partition by id ORDER BY age) AS age,
                id,
                name
            FROM students02) t
            where t.desc_age=1;
            """
        notContains("VPartitionTopN")
    }

    qt_exe """
    select
        t.id,
        t.name,
        t.age,
        t.desc_age
    from (
    SELECT
        ROW_NUMBER() OVER (partition by id ORDER BY age desc) AS desc_age,
        ROW_NUMBER() OVER (partition by id ORDER BY age) AS age,
        id,
        name
    FROM students02) t
    where t.desc_age=1;
        """
}