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

suite("test_encrypt_sql") {

    def dbName = "test_encrypt_sql_db"
    def tableName = "test_encrypt_sql_table"

    sql """drop database if exists ${dbName}"""
    sql """create database ${dbName}"""
    sql """use ${dbName}"""

    sql """CREATE TABLE `${tableName}` (
              `year` int NULL,
              `country` text NULL,
              `product` text NULL,
              `profit` int NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`year`)
            DISTRIBUTED BY HASH(`year`) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            ); 
    """

    def user = "test_encrypt_sql_user"
    def pwd = "123456"

    sql "drop user if exists ${user}"
    sql "create user ${user} IDENTIFIED BY '${pwd}'"
    sql "grant ADMIN_PRIV on *.*.* to ${user}"
    sql "admin set frontend config('enable_nereids_load'='true')"

    connect(user, "${pwd}", context.config.jdbcUrl) {
        try {
            sql """EXPORT TABLE ${dbName}.${tableName} TO "s3://abc/aaa"
                    PROPERTIES(
                        "format" = "csv",
                        "max_file_size" = "2048MB"
                    )
                    WITH s3 (
                        "s3.endpoint" = "xxx",
                        "s3.region" = "ap-beijing",
                        "s3.secret_key"="abc",
                        "s3.access_key" = "abc"
                    );
            """
        } catch (Exception e) {}

        try {
            sql """SELECT * FROM ${dbName}.${tableName}
                    INTO OUTFILE "s3://abc/aaa"
                    FORMAT AS ORC
                    PROPERTIES(
                        "s3.endpoint" = "xxx",
                        "s3.region" = "ap-beijing",
                        "s3.secret_key"="abc",
                        "s3.access_key" = "abc"
                    );
            """
        } catch (Exception e) {}

        try {
            sql """LOAD LABEL test_load_s3_orc_encrypt
                    (
                        DATA INFILE("s3://abc/aaa")
                        INTO TABLE ${tableName}
                        FORMAT AS "ORC"
                    )
                    WITH S3
                    (
                        "provider" = "S3",
                        "AWS_ENDPOINT" = "xxx",
                        "AWS_ACCESS_KEY" = "abc",
                        "AWS_SECRET_KEY" = "abc",
                        "AWS_REGION" = "ap-beijing"
                    )
            """
        } catch (Exception e) {}

        try {
            sql"""CREATE CATALOG ${tableName} 
                    PROPERTIES( 
                        'type'='iceberg', 
                        'iceberg.catalog.type' = 'hadoop', 
                        'warehouse' = 's3://bucket/dir/key', 
                        's3.endpoint' = 's3.us-east-1.amazonaws.com', 
                        's3.access_key' = 'ak', 
                        's3.secret_key' = 'sk'
                    );
            """
        } catch (Exception e) {}
        // for jdbc table or es table
        try {
            sql"""CREATE TABLE mysql_${tableName} (
                    k1 DATE, 
                    k2 INT, 
                    k3 SMALLINT, 
                    k4 VARCHAR(2048), 
                    k5 DATETIME
                )
                ENGINE=mysql
                PROPERTIES(
                    'host' = '127.0.0.1',
                    'port' = '8234',
                    'user' = 'abc',
                    'password' = '123',
                    'database' = 'mysql_db',
                    'table' = 'mysql_table'
                );
            """
        } catch (Exception e) {}

        try {
            sql"""CREATE EXTERNAL TABLE broker_${tableName}(
                    k1 tinyint, 
                    k2 smallint, 
                    k3 int, 
                    k4 bigint) 
                ENGINE=broker 
                PROPERTIES(
                    "broker_name" = "hdfs",
                    "path" = "hdfs://xxxxxxx/qe/a.txt",
                    "column_separator" = "\\t"
                ) 
                BROKER PROPERTIES(
                    "username"="root", 
                    "password"="123"
                );
            """
        } catch (Exception e) {}

        try {
            sql"""INSERT INTO test_s3load
                    SELECT * FROM S3_${tableName} (
                        "uri" = "s3://your_bucket_name/s3load_example.csv",
                        "format" = "csv",
                        "provider" = "OSS",
                        "s3.endpoint" = "oss-cn-hangzhou.aliyuncs.com",
                        "s3.region" = "oss-cn-hangzhou",
                        "s3.access_key" = "abc",
                        "s3.secret_key" = "abc",
                        "column_separator" = ",",
                        "csv_schema" = "user_id:int;name:string;age:int"
                    );
            """
        } catch (Exception e) {}

        try {
            sql"""SELECT * FROM S3_${tableName} (
                        "uri" = "s3://your_bucket_name/s3load_example.csv",
                        "format" = "csv",
                        "provider" = "OSS",
                        "s3.endpoint" = "oss-cn-hangzhou.aliyuncs.com",
                        "s3.region" = "oss-cn-hangzhou",
                        "s3.access_key" = "abc",
                        "s3.secret_key" = "abc",
                        "column_separator" = ",",
                        "csv_schema" = "user_id:int;name:string;age:int"
                  );
            """
        } catch (Exception e) {}

        sql "SET LDAP_ADMIN_PASSWORD = PASSWORD('123456')"

        sql "SET PASSWORD FOR '${user}' = PASSWORD('123456')"
    }

    Thread.sleep(15000)
    sql "call flush_audit_log()"

    def dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
    def date = dateFormat.format(new Date())
    qt_sql "select stmt from __internal_schema.audit_log where user = '${user}' and time > '${date}' and (stmt like '%${tableName}%' or stmt like '%PASSWORD%') order by stmt"

    sql "drop table ${tableName}"
    sql "drop database ${dbName}"
    sql "drop user ${user}"
}
