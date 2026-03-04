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

suite("test_pythonudf_boolean") {
    def pyPath = """${context.file.parent}/udf_scripts/pyudf.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = "3.8.10"
    log.info("Python Zip path: ${pyPath}".toString())
    try {
        sql """ DROP TABLE IF EXISTS test_pythonudf_boolean """
        sql """
        CREATE TABLE IF NOT EXISTS test_pythonudf_boolean (
            `user_id`  INT     NOT NULL COMMENT "",
            `boo_1`    BOOLEAN NOT NULL COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """

        sql """ INSERT INTO test_pythonudf_boolean (`user_id`,`boo_1`) VALUES
                (111,true),
                (112,false),
                (113,0),
                (114,1)
            """
        qt_select_default """ SELECT * FROM test_pythonudf_boolean t ORDER BY user_id; """

        File path1 = new File(pyPath)
        if (!path1.exists()) {
            throw new IllegalStateException("""${pyPath} doesn't exist! """)
        }

        sql """ CREATE FUNCTION python_udf_boolean_test(BOOLEAN) RETURNS BOOLEAN PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="boolean_test.evaluate",
            "type"="PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        ); """

        qt_select """ SELECT python_udf_boolean_test(1)     as result; """
        qt_select """ SELECT python_udf_boolean_test(0)     as result ; """
        qt_select """ SELECT python_udf_boolean_test(true)  as result ; """
        qt_select """ SELECT python_udf_boolean_test(false) as result ; """
        qt_select """ SELECT python_udf_boolean_test(null)  as result ; """
        qt_select """ SELECT user_id,python_udf_boolean_test(boo_1) as result FROM test_pythonudf_boolean order by user_id; """
        


    } finally {
        try_sql("DROP FUNCTION IF EXISTS python_udf_boolean_test(BOOLEAN);")
        try_sql("DROP TABLE IF EXISTS test_pythonudf_boolean")
    }
}
