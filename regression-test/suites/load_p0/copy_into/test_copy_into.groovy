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

suite("test_copy_into", "p0") {
    if (isCloudMode()) {
        def externalStageName = "test_copy_into"
        def filePrefix = "copy_into/json"
        def tableName = "json_test"
        try_sql """drop stage if exists ${externalStageName}"""
        sql """
            create stage if not exists ${externalStageName} 
            properties ('endpoint' = '${getS3Endpoint()}' ,
            'region' = '${getS3Region()}' ,
            'bucket' = '${getS3BucketName()}' ,
            'prefix' = 'smoke-test' ,
            'ak' = '${getS3AK()}' ,
            'sk' = '${getS3SK()}' ,
            'provider' = '${getS3Provider()}',
            'access_type' = 'aksk',
            'default.file.column_separator' = "|");
        """

        def dropTable =  """ DROP TABLE IF EXISTS ${tableName}; """
        def createTable = """
            CREATE TABLE ${tableName} (
                    p_partkey     int NOT NULL DEFAULT "1",
                    p_name        VARCHAR(55) NOT NULL DEFAULT "2",
                    p_mfgr        VARCHAR(25) NOT NULL DEFAULT "3",
                    p_brand       VARCHAR(10) NOT NULL DEFAULT "4",
                    p_type        VARCHAR(25) NOT NULL DEFAULT "5",
                    p_size        int NOT NULL DEFAULT "6"
                    )ENGINE=OLAP
            DUPLICATE KEY(`p_partkey`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 3;
        """

        def tartgetColumnsList = [ 
                            """""",
                            """""",
                            """""",
                            """(p_partkey, p_name, p_mfgr, p_brand, p_type, p_size)""",
                            """(p_partkey, p_name, p_mfgr, p_brand, p_type, p_size)""",
                            """(p_size, p_type, p_brand, p_mfgr, p_name, p_partkey)""",
                            """(p_partkey, p_name, p_mfgr, p_brand)""",
                            """(p_partkey, p_name, p_size)""",
                            """(p_partkey, p_name, p_mfgr, p_brand, p_type, p_size)""",
                            """(p_partkey, p_name, p_mfgr, p_brand, p_type, p_size)""",
                            ]

        def selectColumnsList = [ 
                            """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size""",
                            """p_partkey, p_name, p_mfgr, p_brand, not_exist, p_size""",
                            """*""",
                            """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size""",
                            """p_partkey, p_name, p_mfgr, p_brand, not_exist, p_size""",
                            """p_size, p_type, p_brand, p_mfgr, p_name, p_partkey""",
                            """p_partkey, p_name, p_mfgr, p_brand """,
                            """p_partkey, p_name, greatest(cast(p_partkey as int), cast(p_size as int))""",
                            """p_partkey + 1,  p_name, p_mfgr, p_brand, p_type, p_size * 2""",
                            """p_partkey, p_name, p_mfgr, p_brand, p_type, p_size""",
                            ]

        def whereExprs = [
                            "",
                            "",
                            "",
                            "",
                            "",
                            "",
                            "",
                            "",
                            "",
                            "where p_partkey>10",
                            ]

        def loadRows = [
                        "100490",
                        "0",
                        "100490",
                        "100490",
                        "0",
                        "100490",
                        "100490",
                        "100490",
                        "100490",
                        "100480",
                        ]

        def errorMsgs = [
                        "",
                        "quality not good enough to cancel",
                        "",
                        "",
                        "quality not good enough to cancel",
                        "",
                        "",
                        "",
                        "",
                        "",
                        ]


        def do_copy_into = {table, targetColumns, selectColumns, stageName, prefix, whereExpr ->
            sql """
                copy into $table $targetColumns
                from (select $selectColumns from @${stageName}('${prefix}/part.json') $whereExpr)
                properties ('file.type' = 'json', 'copy.async' = 'false');
                """
        }

        def result;
        for (int i = 0; i < tartgetColumnsList.size(); i++) {
            sql "$dropTable"
            sql "$createTable"
            result = do_copy_into.call(tableName, tartgetColumnsList[i], selectColumnsList[i],
                                        externalStageName, filePrefix, whereExprs[i])
            logger.info("i: " + i + ", copy result: " + result)
            assertTrue(result.size() == 1)
            if (result[0][1].equals("FINISHED")) {
                assertTrue(result[0][4].equals(loadRows[i]), "expected: " + loadRows[i] + ", actual: " + result[0][4])
                continue;
            }
            if (result[0][1].equals("CANCELLED")) {
                assertTrue(errorMsgs[i] == result[0][3], "expected: " + errorMsgs[i] + ", actual: " + result[0][3])
                continue;
            }
            assertTrue(false, "should not come here")
        }
        try_sql """drop stage if exists ${externalStageName}"""
    }
}
