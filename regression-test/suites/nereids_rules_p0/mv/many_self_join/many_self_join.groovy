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

suite("many_self_join") {

    def db_name = context.config.getDbNameByFile(context.file)
    def tb_name = db_name + "_tb"
    def mv_name = db_name + "_mtmv"
    sql """drop table if exists ${tb_name}"""
    sql """
        CREATE TABLE ${tb_name} (
            employee_id INT,
            employee_name VARCHAR(100),
            manager_id INT
        ) ENGINE=OLAP
        DUPLICATE KEY(employee_id)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`employee_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql """
        INSERT INTO ${tb_name} (employee_id, employee_name, manager_id) VALUES
            (1, 'CEO', 1),
            (2, 'Manager1', 1),
            (3, 'Manager2', 1)
        """

    sql """drop MATERIALIZED VIEW if exists ${mv_name};"""
    def mtmv_sql = """SELECT 
            e1.employee_name AS employee,
            e2.employee_name AS manager_1,
            e3.employee_name AS manager_2,
            e4.employee_name AS manager_3,
            e5.employee_name AS manager_4,
            e6.employee_name AS manager_5,
            e7.employee_name AS manager_6,
            e8.employee_name AS manager_7,
            e9.employee_name AS manager_8,
            e10.employee_name AS manager_9
        FROM 
            ${tb_name} e1
        LEFT JOIN 
            ${tb_name} e2 ON e1.manager_id = e2.employee_id
        LEFT JOIN 
            ${tb_name} e3 ON e2.manager_id = e3.employee_id
        LEFT JOIN 
            ${tb_name} e4 ON e3.manager_id = e4.employee_id
        LEFT JOIN 
            ${tb_name} e5 ON e4.manager_id = e5.employee_id
        LEFT JOIN 
            ${tb_name} e6 ON e5.manager_id = e6.employee_id
        LEFT JOIN 
            ${tb_name} e7 ON e6.manager_id = e7.employee_id
        LEFT JOIN 
            ${tb_name} e8 ON e7.manager_id = e8.employee_id
        LEFT JOIN 
            ${tb_name} e9 ON e8.manager_id = e9.employee_id
        LEFT JOIN 
            ${tb_name} e10 ON e9.manager_id = e10.employee_id"""

    create_async_mv(db_name, mv_name, mtmv_sql)

    def mark = true
    sql """USE ${db_name}"""
    sql """set enable_nereids_timeout = true;"""
    try {
        mv_rewrite_success(mtmv_sql, mv_name)
    } catch (Exception e) {
        log.info("e.getMessage(): " + e.getMessage())
        mark = false
    }

    assertTrue(mark)

}

