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

suite("alter_column_test_generated_column") {
    def waitSchemaChangeJob = { String tableName /* param */ ->
        int tryTimes = 30
        while (tryTimes-- > 0) {
            def jobResult = sql """SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
            if (jobResult == null || jobResult.isEmpty()) {
                sleep(3000)
                return;
            }
            def jobState = jobResult[0][9].toString()
            if ('cancelled'.equalsIgnoreCase(jobState)) {
                logger.info("jobResult:{}", jobResult)
                throw new IllegalStateException("${tableName}'s job has been cancelled")
            }
            if ('finished'.equalsIgnoreCase(jobState)) {
                logger.info("jobResult:{}", jobResult)
                sleep(3000)
                return
            }
            sleep(1000)
        }
        assertTrue(false)
    }
    def getMVJobState = { tableName ->
        def jobStateResult = sql """  SHOW ALTER TABLE ROLLUP WHERE TableName='${tableName}' ORDER BY CreateTime DESC LIMIT 1 """
        return jobStateResult[0][8]
    }
    def waitForMVJob =  (tbName, timeout) -> {
        while (timeout--){
            String result = getMVJobState(tbName)
            if (result == "FINISHED") {
                sleep(3000)
                break
            } else {
                sleep(100)
                if (timeout < 1){
                    assertEquals(1,2)
                }
            }
        }
    }
    sql "SET enable_nereids_planner=true;"
    sql "SET enable_fallback_to_original_planner=false;"

    multi_sql """
    drop table if exists alter_column_gen_col;
    create table alter_column_gen_col (a int, b int, c int as(a+b), d int as(c+1), e int)
    duplicate key(a) distributed by hash(a) PROPERTIES("replication_num" = "1");
    alter table alter_column_gen_col add rollup r1 (c,b,a)
    """
    waitForMVJob("alter_column_gen_col", 3000)
    // add column
    test {
        sql "alter table alter_column_gen_col add column f int as (a+1);"
        exception "Not supporting alter table add generated columns."
    }
    test {
        sql "alter table alter_column_gen_col add column f int as (a+1) to r1"
        exception "Not supporting alter table add generated columns."
    }
    test {
        sql "alter table alter_column_gen_col add column f int as (a+1) after a to r1;"
        exception "Not supporting alter table add generated columns."
    }
    test {
        sql """alter table alter_column_gen_col add column (f int as (a+1), g int) to r1;"""
        exception "Not supporting alter table add generated columns."
    }

    // drop column
    // rollup
    multi_sql """
        insert into alter_column_gen_col values(1,2,default,default,5);
        insert into alter_column_gen_col values(9,2,default,default,3);
        insert into alter_column_gen_col values(6,2,default,default,5);
    """
    explain {
        sql "select c,b from alter_column_gen_col where c=10;"
        contains "r1"
    }
    qt_drop_gen_col_rollup "alter table alter_column_gen_col drop column c from r1;"
    waitSchemaChangeJob("alter_column_gen_col")
    explain {
        sql "select b from alter_column_gen_col where b=10;"
        contains "r1"
    }
    multi_sql """
        insert into alter_column_gen_col values(9,2,default,default,3);
        insert into alter_column_gen_col values(6,2,default,default,5);
    """
    // modify column
    test {
        sql "alter table alter_column_gen_col modify column c double as (a+b);"
        exception "Not supporting alter table modify generated columns."
    }
    test {
        sql "alter table alter_column_gen_col modify column c double as (a+b) after e;"
        exception "Not supporting alter table modify generated columns."
    }
    test {
        sql "alter table alter_column_gen_col modify column c int as (a+b) after e;"
        exception "Not supporting alter table modify generated columns."
    }

    // reorder column
    qt_reorder "alter table alter_column_gen_col order by(a,c,b,d,e);"
    waitSchemaChangeJob("alter_column_gen_col")

    test {
        sql "alter table alter_column_gen_col order by(a,d,b,c,e);"
        exception "The specified column order is incorrect, `d` should come after `c`, because both of them are generated columns, and `d` refers to `c`."
    }
    qt_after_reorder_insert "insert into alter_column_gen_col(a,b,e) values(12,3,4);"
    qt_reorder_rollup "alter table alter_column_gen_col order by (a,b) from r1"
    waitSchemaChangeJob("alter_column_gen_col")

    // rename column
    test {
        sql "alter table alter_column_gen_col rename column c c1"
        exception ""
    }
    qt_rename_gen_col  "alter table alter_column_gen_col rename column d d1"
    waitSchemaChangeJob("alter_column_gen_col")
    qt_after_rename_insert "insert into alter_column_gen_col(a,b,e) values(16,2,4);"
    qt_after_rename_insert_select "select * from alter_column_gen_col order by 1,2,3,4,5"
}