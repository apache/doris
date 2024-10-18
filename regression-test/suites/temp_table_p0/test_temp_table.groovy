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

suite('test_temp_table', 'p0,restart_fe') {
    def show_tables = sql """
                use regression_test_table_p0;
                show data;
                """
    def hasTempTable = false
    for (int i = 0; i < show_tables.size(); i++) {
        if (show_tables[i][0].equals("t_test_temp_table2")) {
            hasTempTable = true;
        }
    }
    assertFalse(hasTempTable)

    try {
        sql "show data from t_test_temp_table2;"
        throw new IllegalStateException("Should throw error")
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("table not found"), ex.getMessage())
    }

    try {
        sql "select * from t_test_temp_table2;"
        throw new IllegalStateException("Should throw error")
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("does not exist in database"), ex.getMessage())
    }

    //clean
    sql """drop table if exists t_test_table_with_data"""
    sql """drop database if exists regression_test_temp_table_db2"""
}