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

suite("load", "smoke") {
    def externalStageName = "smoke_test_tpch"
    def prefix = "tpch/sf1"

    // tpch_sf1_p1 is writted to test unique key table merge correctly.
    // It creates unique key table and sets bucket num to 1 in order to make sure that
    // many rowsets will be created during loading and then the merge process will be triggered.

    def tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
    try_sql """drop stage if exists ${externalStageName}"""
    for (String table in tables) {
        sql """ DROP TABLE IF EXISTS ${table}; """
        sql new File("""${context.file.parent}/ddl/${table}.sql""").text
    }

    sql """
        create stage if not exists ${externalStageName} properties(
        'endpoint' = '${getS3Endpoint()}' ,
        'region' = '${getS3Region()}' ,
        'bucket' = '${getS3BucketName()}' ,
        'prefix' = 'smoke-test' ,
        'ak' = '${getS3AK()}' ,
        'sk' = '${getS3SK()}' ,
        'provider' = '${getS3Provider()}',
        'access_type' = 'aksk',
        'default.file.column_separator' = "|" 
        );
    """

    for (String tableName in tables) {
        def result = sql " copy into ${tableName} from @${externalStageName}('${prefix}/${tableName}.csv.split00.gz') properties ('file.compression' = 'gz', 'copy.async' = 'false'); "
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals("FINISHED"))

        result = sql " copy into ${tableName} from @${externalStageName}('${prefix}/${tableName}.csv.split01.gz') properties ('file.compression' = 'gz', 'copy.async' = 'false'); "
        logger.info("copy result: " + result)
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 8)
        assertTrue(result[0][1].equals("FINISHED"))
    }

    def table = "revenue1"
    sql new File("""${context.file.parent}/ddl/${table}_delete.sql""").text
    sql new File("""${context.file.parent}/ddl/${table}.sql""").text
    try_sql """drop stage if exists ${externalStageName}"""
}
