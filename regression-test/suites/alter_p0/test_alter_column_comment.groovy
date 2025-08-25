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

suite('test_alter_column_comment', 'nonConcurrent') {
    def tbl = 'test_alter_column_comment_tbl'
    def cmt = '0123456789012345678901234567890123456789012345678901234567890123456789'

    sql "ADMIN SET ALL FRONTENDS CONFIG ('column_comment_length_limit' = '64')"

    sql "DROP TABLE IF EXISTS ${tbl} FORCE"

    // create table with too long column comment
    test {
        sql """
            CREATE TABLE ${tbl} (
                `k1` BIGINT NOT NULL comment "${cmt}",
                `v1` BIGINT NULL comment "${cmt}",
                `v2` INT NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        exception("Column comment is too long");
    }

    // create table for following tests
    sql """
        CREATE TABLE ${tbl} (
            `k1` BIGINT NOT NULL,
            `v1` BIGINT NULL,
            `v2` INT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """
        INSERT INTO ${tbl} VALUES (1,1,1),(2,2,2),(3,3,3)
    """
    sql """ SYNC """

    // alter add column with too long column comment
    test {
        sql """
            ALTER TABLE ${tbl} add column v3 int comment "${cmt}"
        """
        exception("Column comment is too long");
    }

    // alter modify column with too long column comment
    test {
        sql """
            ALTER TABLE ${tbl} modify column v2 int comment "${cmt}"
        """
        exception("Column comment is too long");
    }

    // alter add column with short column comment
    sql """
        ALTER TABLE ${tbl} add column v3 int comment "short_comment"
    """
    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tbl}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    // alter modify column with short column comment
    sql """
        ALTER TABLE ${tbl} modify column v2 int comment "short_comment_v2"
    """
    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tbl}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    // Check table structure after ALTER TABLE
    qt_select """ SELECT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,COLUMN_COMMENT FROM information_schema.columns where TABLE_SCHEMA='${context.dbName}' and table_name='${tbl}' order by TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME """

    // use a smaller config, column comment should be truncated
    sql "ADMIN SET FRONTEND CONFIG ('column_comment_length_limit' = '4')"
    qt_select1 """ SELECT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,COLUMN_COMMENT FROM information_schema.columns where TABLE_SCHEMA='${context.dbName}' and table_name='${tbl}' order by TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME """

    sql "DROP TABLE IF EXISTS ${tbl} FORCE"
    // restore column_comment_length_limit
    sql """ ADMIN SET ALL FRONTENDS CONFIG ("column_comment_length_limit" = "-1"); """
}
