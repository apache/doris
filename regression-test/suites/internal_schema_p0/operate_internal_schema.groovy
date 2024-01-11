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

// this suite is for creating table with timestamp datatype in defferent 
// case. For example: 'year' and 'Year' datatype should also be valid in definition

suite("operate_internal_schema") {
    def testTable = "operate_internal_schema_user"
    sql "use __internal_schema"
    sql "DROP TABLE IF EXISTS ${testTable}"
    //alter db
    sql "ALTER DATABASE __internal_schema SET PROPERTIES('replication_allocation' = '');"
    //create table
    sql """
       CREATE TABLE IF NOT EXISTS ${testTable}
       (
           `user_id` LARGEINT NOT NULL,
           `age` SMALLINT
       )
       UNIQUE KEY(`user_id`)
       DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
       PROPERTIES (
       "replication_allocation" = "tag.location.default: 1"
       );
    """
    //alter table
    sql "ALTER TABLE ${testTable} MODIFY COMMENT "new_comment";"
    //insert
    sql "insert into ${testTable} values(1,2);"
    //update
    sql "update ${testTable} set age=2 where user_id=1;"
    //delete
    sql "delete from ${testTable} where user_id=1;"
    // truncate
    sql "truncate table ${testTable};"
    // insert overwrite
    sql "insert overwrite table ${testTable} values(1,3)"


    def tokens = context.config.jdbcUrl.split('/')
    def url=tokens[0] + "//" + tokens[2] + "/" + "__internal_schema" + "?"
    connect(user=admin, url=url) {
            //alter table
            sql "ALTER TABLE ${testTable} MODIFY COMMENT "new_comment";"
            //insert
            sql "insert into ${testTable} values(1,2);"
            //update
            sql "update ${testTable} set age=2 where user_id=1;"
            //delete
            sql "delete from ${testTable} where user_id=1;"
            // truncate
            sql "truncate table ${testTable};"
            // insert overwrite
            sql "insert overwrite table ${testTable} values(1,3)"
        }

}
