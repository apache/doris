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

// lower_case_table_names=0
suite("case_sensitive_0_test", "rec_cte") {

    sql """
        WITH RECURSIVE UpperCTE AS (
            SELECT cast(1 as int) AS n
            UNION ALL
            SELECT cast(n + 1 as int) FROM UpperCTE WHERE n < 3
        )
        SELECT * FROM UpperCTE;"""

    test {
        sql """
            WITH RECURSIVE UpperCTE AS (
                SELECT cast(1 as int) AS n
                UNION ALL
                SELECT cast(n + 1 as int) FROM uppercte WHERE n < 3  -- 此处小写会触发 Table not found
            )
            SELECT * FROM UpperCTE;"""
        exception("does not exist")
    }

    sql """CREATE DATABASE IF NOT EXISTS Rec_cte_case_sensitive_DB_2;"""
    sql """CREATE DATABASE IF NOT EXISTS rec_cte_case_sensitive_db_2;"""
    sql """USE Rec_cte_case_sensitive_DB_2;"""
    sql """drop table if exists Department_Table"""
    sql """
        CREATE TABLE Department_Table (
            dep_id INT,
            parent_id INT,
            name VARCHAR(50)
        ) 
        DISTRIBUTED BY HASH(dep_id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");"""

    sql """INSERT INTO Department_Table VALUES (1, NULL, 'Headquarter'), (2, 1, 'R&D Group');"""

    // failure: using lowercase to reference the physical table
    test {
        sql """
            WITH RECURSIVE DepTree(id, name) AS (
                SELECT dep_id, name FROM department_table WHERE dep_id = 1 -- 这里使用了小写
                UNION ALL
                SELECT t.dep_id, t.name FROM department_table t 
                JOIN DepTree s ON t.parent_id = s.id
            )
            SELECT * FROM DepTree;"""
        exception("does not exist")
    }

    sql """        
        WITH RECURSIVE DepTree(id, name) AS (
            SELECT dep_id, name FROM Department_Table WHERE dep_id = 1 
            UNION ALL
            SELECT t.dep_id, t.name FROM Department_Table t 
            JOIN DepTree s ON t.parent_id = s.id
        )
        SELECT * FROM DepTree;"""

    // success: column names with mixed case
    sql """
        WITH RECURSIVE Column_Test(ID) AS (
            SELECT cast(1 as int)
            UNION ALL
            SELECT cast(id + 1 as int) FROM Column_Test WHERE id < 3
        )
        SELECT id FROM Column_Test;"""

    test {
        sql """WITH RECURSIVE Column_Test(ID) AS (
                SELECT cast(1 as int)
                UNION ALL
                SELECT cast(id + 1 as int) FROM column_test WHERE id < 3
            )
            SELECT id FROM column_Test;"""
        exception("does not exist")
    }

    // Cross-database reference and CTE have the same name
    sql """
        WITH RECURSIVE rec_cte_case_sensitive_db_2(n) AS (
            SELECT cast(1 as int)
            UNION ALL
            SELECT cast(n + 1 as int) FROM rec_cte_case_sensitive_db_2 WHERE n < 3
        )
        SELECT * FROM Rec_cte_case_sensitive_DB_2.Department_Table;"""


}
