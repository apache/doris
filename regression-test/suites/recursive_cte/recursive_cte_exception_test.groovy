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

suite("exception_test", "rec_cte") {
    String db = context.config.getDbNameByFile(context.file)
    def prefix_str = "exception_"
    def tb_name = prefix_str + "recursive_cte_tb"

    sql """drop table if exists ${tb_name}"""
    sql """CREATE TABLE ${tb_name} (
            EmployeeID INT not null,
            Name VARCHAR(50) NOT NULL,
            ManagerID INT NULL
        ) DUPLICATE KEY(EmployeeID) 
        DISTRIBUTED BY HASH(EmployeeID) 
        BUCKETS 3 PROPERTIES ('replication_num' = '1');"""

    sql """INSERT INTO ${tb_name} VALUES 
        (100, 'Manager X', 1), 
        (101, 'Alice', 100), 
        (102, 'Bob', 100), 
        (103, 'Charlie', 101), 
        (104, 'David', 103), 
        (105, 'Eve', 101);"""

    test {
        sql """
        WITH recursive SubordinateHierarchy (
                EmployeeID,
                EmployeeID,
                EmployeeName,
                ManagerID,
                Level,
                Comments
        )
        AS
        (
                SELECT
                EmployeeID,
                EmployeeID,
                Name AS EmployeeName,
                ManagerID,
                cast(0 as bigint) AS Level,
                CAST(NULL AS VARCHAR(100)) AS Comments
        FROM
        ${tb_name}
        WHERE
        EmployeeID = 100

        UNION ALL

        SELECT
        E.EmployeeID,
        E.EmployeeID,
        E.Name AS EmployeeName,
        E.ManagerID,
        cast(H.Level + 1 as bigint) AS Level,
        cast(H.EmployeeName as VARCHAR(100))
        FROM
        ${tb_name} AS E
        INNER JOIN
        SubordinateHierarchy AS H
        ON E.ManagerID = H.EmployeeID
        )
        SELECT
                *
                FROM
        SubordinateHierarchy
        ORDER BY
        Level, EmployeeID;
        """
        exception "is ambiguous"
    }

    // drop recursive part, become normal cte, no recursive cte
    sql """
        WITH recursive SubordinateHierarchy (
        EmployeeID, 
        EmployeeName, 
        ManagerID, 
        Level,
        Comments
    )
    AS
    (
        SELECT 
            EmployeeID, 
            Name AS EmployeeName, 
            ManagerID, 
            cast(0 as bigint) AS Level,
            CAST(NULL AS VARCHAR(100)) AS Comments 
        FROM 
            ${tb_name}
        WHERE 
            EmployeeID = 100
    )
    SELECT 
        *
    FROM 
        SubordinateHierarchy
    ORDER BY 
        Level, EmployeeID;
    """


    test {
        sql """
            WITH recursive SubordinateHierarchy (
            EmployeeID, 
            EmployeeName, 
            ManagerID, 
            Level,
            Comments
        )
        AS
        (

            SELECT 
                E.EmployeeID, 
                E.Name AS EmployeeName, 
                E.ManagerID, 
                cast(H.Level + 1 as bigint) AS Level,
                cast(H.EmployeeName as VARCHAR(100))
            FROM 
                ${tb_name} AS E
            INNER JOIN 
                SubordinateHierarchy AS H
                ON E.ManagerID = H.EmployeeID
        )
        SELECT 
            *
        FROM 
            SubordinateHierarchy
        ORDER BY 
            Level, EmployeeID;
        """
        exception "recursive cte must be union"
    }

    test {
        sql """
            WITH 
            StartingEmployees AS (
                SELECT 
                    EmployeeID,
                    Name,
                    ManagerID
                FROM 
                    ${tb_name}
                WHERE 
                    Name LIKE 'A%' 
            ),
            SubordinateHierarchy (
            EmployeeID, 
            EmployeeName, 
            ManagerID, 
            Level,
            Comments
            )
            AS
            (
            SELECT 
                EmployeeID, 
                Name AS EmployeeName, 
                ManagerID, 
                cast(0 as bigint) AS Level,
                CAST(NULL AS VARCHAR(100)) AS Comments 
            FROM 
                ${tb_name}
            WHERE 
                EmployeeID = 100
                
            UNION ALL
            
            SELECT 
                E.EmployeeID, 
                E.Name AS EmployeeName, 
                E.ManagerID, 
                cast(H.Level + 1 as bigint) AS Level,
                cast(H.EmployeeName as VARCHAR(100))
            FROM 
                ${tb_name} AS E
            INNER JOIN 
                SubordinateHierarchy AS H
                ON E.ManagerID = H.EmployeeID
            )
            SELECT 
                EmployeeID, 
                EmployeeName, 
                ManagerID, 
                Level,
                Comments
            FROM 
                StartingEmployees
            ORDER BY 
                Path DESC;
        """
        exception "does not exist"
    }


}
