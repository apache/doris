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

suite("create_and_insert_select_test", "rec_cte") {

    String db = context.config.getDbNameByFile(context.file)
    def prefix_str = "create_and_insert_select_"
    def tb_name = prefix_str + "recursive_cte_tb"
    def table_cte_name = prefix_str + "temp_cte_table"

    sql """drop table if exists ${tb_name}"""
    sql """CREATE TABLE ${tb_name} (
            EmployeeID INT not null,
            Name VARCHAR(50)  not NULL,
            ManagerID INT not NULL
        ) DUPLICATE KEY(EmployeeID) 
        DISTRIBUTED BY HASH(EmployeeID) 
        BUCKETS 3 PROPERTIES ('replication_num' = '1');"""

    sql """INSERT INTO ${tb_name} VALUES 
        (100, 'Manager X', 0), 
        (101, 'Alice', 100), 
        (102, 'Bob', 100), 
        (103, 'Charlie', 101), 
        (104, 'David', 103), 
        (105, 'Eve', 101);"""


    sql """drop table if exists ${table_cte_name}"""
    sql """
        CREATE table ${table_cte_name} 
        PROPERTIES ('replication_num' = '1') 
        AS  
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
            *
        FROM 
            SubordinateHierarchy
        ORDER BY 
            Level, EmployeeID;
    """

    def tb_res1 = sql """select * from ${table_cte_name}"""
    assertTrue(tb_res1.size() > 0)

    sql """
        insert into ${table_cte_name}  
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
            *
        FROM 
            SubordinateHierarchy
        ORDER BY 
            Level, EmployeeID;
    """

    def tb_res2 = sql """select * from ${table_cte_name}"""
    assertTrue(tb_res1.size() * 2 == tb_res2.size())

}
