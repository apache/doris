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

suite("same_data_type_recursive_test", "rec_cte") {
    String db = context.config.getDbNameByFile(context.file)
    def prefix_str = "data_null_not_recursive_"
    def tb_name = prefix_str + "tb"

    // No data satisfies the recursion criteria
    sql """drop table if exists ${tb_name}"""
    sql """CREATE TABLE ${tb_name} (
            EmployeeID INT not null,
            Name VARCHAR(50)  not NULL,
            ManagerID INT not NULL
        ) DUPLICATE KEY(EmployeeID) 
        DISTRIBUTED BY HASH(EmployeeID) 
        BUCKETS 3 PROPERTIES ('replication_num' = '1');"""

    sql """INSERT INTO ${tb_name} VALUES 
        (101, 'Alice', 999), 
        (102, 'Bob', 999), 
        (103, 'Charlie', 999);"""

    sql """
        WITH recursive HierarchyTraversal (
            EmployeeID, 
            EmployeeName, 
            ManagerID, 
            TraversalLevel
        )
        AS
        (
            SELECT 
                EmployeeID, 
                Name AS EmployeeName, 
                ManagerID, 
                cast(1 as bigint) AS TraversalLevel
            FROM 
                ${tb_name}
            WHERE 
                ManagerID IS NULL
        
            UNION ALL
        
            SELECT 
                E.EmployeeID, 
                E.Name AS EmployeeName, 
                E.ManagerID, 
                cast(R.TraversalLevel + 1 as bigint) AS TraversalLevel
            FROM 
                ${tb_name} AS E
            INNER JOIN 
                HierarchyTraversal AS R
                ON E.ManagerID = R.EmployeeID
        )
        SELECT 
            *
        FROM 
            HierarchyTraversal;"""



    sql """drop table if exists ${tb_name}"""
    sql """CREATE TABLE ${tb_name} (
            EmployeeID INT null,
            Name VARCHAR(50) NULL,
            ManagerID INT NULL
        ) DUPLICATE KEY(EmployeeID) 
        DISTRIBUTED BY HASH(EmployeeID) 
        BUCKETS 3 PROPERTIES ('replication_num' = '1');"""
    sql """INSERT INTO ${tb_name} VALUES 
        (100, 'CEO Root', NULL), 
        (101, 'Manager Alpha', 100), 
        (102, 'Manager Beta', 100),
        (103, 'Manager Gamma', 100);"""

    sql """
        WITH recursive ShallowHierarchy (
            EmployeeID, 
            EmployeeName, 
            ManagerID, 
            TraversalLevel
        )
        AS
        (
            SELECT 
                EmployeeID, 
                Name AS EmployeeName, 
                ManagerID, 
                cast(1 as bigint) AS TraversalLevel
            FROM 
                ${tb_name}
            WHERE 
                EmployeeID = 100 
        
            UNION ALL
        
            SELECT 
                E.EmployeeID, 
                E.Name AS EmployeeName, 
                E.ManagerID, 
                cast(R.TraversalLevel + 1 as bigint) AS TraversalLevel
            FROM 
                ${tb_name} AS E
            INNER JOIN 
                ShallowHierarchy AS R
                ON E.ManagerID = R.EmployeeID
        )
        SELECT 
            *
        FROM 
            ShallowHierarchy
        WHERE 
            TraversalLevel > 1
        ORDER BY
            EmployeeID;"""

    sql """drop table if exists ${tb_name}"""
    sql """CREATE TABLE ${tb_name} (
            EmployeeID INT null,
            Name VARCHAR(50)  NULL,
            ManagerID INT NULL
        ) DUPLICATE KEY(EmployeeID) 
        DISTRIBUTED BY HASH(EmployeeID) 
        BUCKETS 3 PROPERTIES ('replication_num' = '1');"""
    sql """INSERT INTO ${tb_name} VALUES 
        (100, 'Project A', NULL), 
        (101, 'Project B', NULL), 
        (102, 'Project C', NULL);"""

    sql """
        WITH recursive ExhaustiveAnchor (
            EmployeeID, 
            EmployeeName, 
            ManagerID, 
            TraversalLevel
        )
        AS
        (
            SELECT 
                EmployeeID, 
                Name AS EmployeeName, 
                ManagerID, 
                cast(1 as bigint) AS TraversalLevel
            FROM 
                ${tb_name}
            WHERE 
                ManagerID IS NULL
        
            UNION ALL
        
            SELECT 
                E.EmployeeID, 
                E.Name AS EmployeeName, 
                E.ManagerID, 
                cast(R.TraversalLevel + 1 as bigint) AS TraversalLevel
            FROM 
                ${tb_name} AS E
            INNER JOIN 
                ExhaustiveAnchor AS R
                ON E.ManagerID = R.EmployeeID
        )
        SELECT 
            *
        FROM 
            ExhaustiveAnchor
        ORDER BY
            EmployeeID;"""


    // Some data satisfies the recursion criteria, while others do not
    sql """drop table if exists ${tb_name}"""
    sql """CREATE TABLE ${tb_name} (
            EmployeeID INT null,
            Name VARCHAR(50) NULL,
            ManagerID INT NULL
        ) DUPLICATE KEY(EmployeeID) 
        DISTRIBUTED BY HASH(EmployeeID) 
        BUCKETS 3 PROPERTIES ('replication_num' = '1');"""

    sql """INSERT INTO ${tb_name} VALUES 
                (100, 'CEO Alpha', NULL), 
                (101, 'Manager X', 100), 
                (102, 'Worker Y', 101),
                (200, 'Isolated Lead', 999),
                (201, 'Isolated Worker', 200);"""

    sql """
        WITH recursive MixedHierarchyTraversal (
            EmployeeID, 
            EmployeeName, 
            ManagerID, 
            TraversalLevel
        )
        AS
        (
            SELECT 
                EmployeeID, 
                Name AS EmployeeName, 
                ManagerID, 
                cast(1 as bigint) AS TraversalLevel
            FROM 
                ${tb_name}
            WHERE 
                EmployeeID = 100
        
            UNION ALL
        
            SELECT 
                E.EmployeeID, 
                E.Name AS EmployeeName, 
                E.ManagerID, 
                cast(R.TraversalLevel + 1 as bigint) AS TraversalLevel
            FROM 
                ${tb_name} AS E
            INNER JOIN 
                MixedHierarchyTraversal AS R
                ON E.ManagerID = R.EmployeeID
        )
        SELECT 
            *
        FROM 
            MixedHierarchyTraversal
        ORDER BY
            EmployeeID;"""


    // Multi-branch recursion, executed successfully
    sql """drop table if exists ${tb_name}"""
    sql """CREATE TABLE ${tb_name} (
            EmployeeID INT null,
            Name VARCHAR(50) NULL,
            ManagerID INT NULL
        ) DUPLICATE KEY(EmployeeID) 
        DISTRIBUTED BY HASH(EmployeeID) 
        BUCKETS 3 PROPERTIES ('replication_num' = '1');"""

    sql """INSERT INTO ${tb_name} VALUES 
                (100, 'CEO Root', NULL), 
                (101, 'Manager Alpha', 100), 
                (102, 'Worker A1', 101),
                (103, 'Worker A2', 101),
                (104, 'Manager Beta', 100),
                (105, 'Worker B1', 104);"""

    sql """
        WITH recursive MultiBranchHierarchy (
            EmployeeID, 
            EmployeeName, 
            ManagerID, 
            TraversalLevel,
            BranchPath
        )
        AS
        (
            SELECT 
                EmployeeID, 
                Name AS EmployeeName, 
                ManagerID, 
                cast(1 as bigint) AS TraversalLevel,
                CAST(Name AS VARCHAR(65533)) AS BranchPath
            FROM 
                ${tb_name}
            WHERE 
                EmployeeID = 100
        
            UNION ALL
        
            SELECT 
                E.EmployeeID, 
                E.Name AS EmployeeName, 
                E.ManagerID, 
                cast(R.TraversalLevel + 1 as bigint) AS TraversalLevel,
                cast(CONCAT(R.BranchPath, ' -> ', E.Name) as VARCHAR(65533)) AS BranchPath
            FROM 
                ${tb_name} AS E
            INNER JOIN 
                MultiBranchHierarchy AS R
                ON E.ManagerID = R.EmployeeID
        )
        SELECT 
            *
        FROM 
            MultiBranchHierarchy
        ORDER BY
            BranchPath;"""

    // Cyclic recursion with UNION
    sql """drop table if exists ${tb_name}"""
    sql """CREATE TABLE ${tb_name} (
            EmployeeID INT null,
            Name VARCHAR(50) NULL,
            ManagerID INT NULL
        ) DUPLICATE KEY(EmployeeID) 
        DISTRIBUTED BY HASH(EmployeeID) 
        BUCKETS 3 PROPERTIES ('replication_num' = '1');"""

    sql """INSERT INTO ${tb_name} VALUES 
                (100, 'CEO Root', NULL), 
                (201, 'Cycle Node A', 202), 
                (202, 'Cycle Node B', 201);"""

    test {
        sql """
        WITH recursive CycleTraversal (
            EmployeeID, 
            EmployeeName, 
            ManagerID, 
            TraversalCount,
            PathCheck      
        )
        AS
        (
            SELECT 
                EmployeeID, 
                Name AS EmployeeName, 
                ManagerID, 
                cast(1 as bigint) AS TraversalLevel,
                -- CAST(',' + CAST(EmployeeID AS VARCHAR(65533)) + ',' AS VARCHAR(65533)) AS PathCheck 
                cast(concat(',', CAST(EmployeeID AS VARCHAR(65533)), ',') as VARCHAR(65533) ) AS PathCheck
            FROM 
                ${tb_name}
            WHERE 
                EmployeeID = 201 
        
            UNION 
            
            SELECT 
                E.EmployeeID, 
                E.Name AS EmployeeName, 
                E.ManagerID, 
                cast(R.TraversalCount + 1 as bigint) AS TraversalCount,
                -- R.PathCheck + CAST(E.EmployeeID AS VARCHAR(MAX)) + ',' AS PathCheck
                cast(concat(R.PathCheck, CAST(E.EmployeeID AS VARCHAR(65533)), ',') as VARCHAR(65533)) AS PathCheck 
            FROM 
                ${tb_name} AS E
            INNER JOIN 
                CycleTraversal AS R 
                ON E.ManagerID = R.EmployeeID 
        )
        SELECT 
            EmployeeID,
            EmployeeName,
            TraversalCount,
            PathCheck
        FROM 
            CycleTraversal
        ORDER BY
            TraversalCount;"""
        exception "reach cte_max_recursion_depth 100"
    }

    // Cyclic recursion with UNION ALL
    sql """drop table if exists ${tb_name}"""
    sql """CREATE TABLE ${tb_name} (
            EmployeeID INT null,
            Name VARCHAR(50) NULL,
            ManagerID INT NULL
        ) DUPLICATE KEY(EmployeeID) 
        DISTRIBUTED BY HASH(EmployeeID) 
        BUCKETS 3 PROPERTIES ('replication_num' = '1');"""

    sql """INSERT INTO ${tb_name} VALUES 
                (100, 'CEO Root', NULL), 
                (201, 'Cycle Node A', 202), 
                (202, 'Cycle Node B', 201);"""

    test {
        sql """
        WITH recursive CycleTraversal (
            EmployeeID, 
            EmployeeName, 
            ManagerID, 
            TraversalCount,
            PathCheck
        )
        AS
        (
            SELECT 
                EmployeeID, 
                Name AS EmployeeName, 
                ManagerID, 
                cast(1 as bigint) AS TraversalCount, 
                cast(concat(',', CAST(EmployeeID AS VARCHAR(65533)), ',') as VARCHAR(65533)) as PathCheck 
            FROM 
                ${tb_name}
            WHERE 
                EmployeeID = 201 
        
            UNION ALL
            
            SELECT 
                E.EmployeeID, 
                E.Name AS EmployeeName, 
                E.ManagerID, 
                cast(R.TraversalCount + 1 as bigint) AS TraversalCount,
                cast(concat(R.PathCheck, CAST(E.EmployeeID AS VARCHAR(65533)), ',') as VARCHAR(65533)) as PathCheck
            FROM 
                ${tb_name} AS E
            INNER JOIN 
                CycleTraversal AS R 
                ON E.ManagerID = R.EmployeeID 
        )
        SELECT 
            EmployeeID,
            EmployeeName,
            TraversalCount,
            PathCheck
        FROM 
            CycleTraversal
        ORDER BY
            TraversalCount;"""
        exception "reach cte_max_recursion_depth 100"
    }

}
