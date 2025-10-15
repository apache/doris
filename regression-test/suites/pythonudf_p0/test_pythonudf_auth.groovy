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

suite("test_pythonudf_auth") {
    def pyPath = """${context.file.parent}/udf_scripts/pyudf.zip"""
    scp_udf_file_to_all_be(pyPath)
    log.info("Python Zip path: ${pyPath}".toString())
    File path = new File(pyPath)
    if (!path.exists()) {
        throw new IllegalStateException("""${pyPath} doesn't exist! """)
    }

    def tokens = context.config.jdbcUrl.split('/')
    def url=tokens[0] + "//" + tokens[2] + "/" + "information_schema" + "?"

    def user = 'udf_auth_user'
    def pwd = '123456'
    def dbName = 'udf_auth_db'

    try_sql("DROP USER ${user}")
    try_sql("DROP FUNCTION IF EXISTS python_udf_auth_test(int);")
    sql """DROP DATABASE IF EXISTS ${dbName}"""

    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE DATABASE ${dbName}"""

    sql """USE ${dbName}"""
    sql """ CREATE FUNCTION python_udf_auth_test(int) RETURNS int PROPERTIES (
        "file"="file://${pyPath}",
        "symbol"="int_test",
        "type"="PYTHON_UDF"
    ); """
    connect(user=user, password="${pwd}", url=url) {
        try {
            sql "select ${dbName}.python_udf_auth_test(1)"
            fail()
        } catch (Exception e) {
            log.info(e.getMessage())
        }
    }

    sql """GRANT SELECT_PRIV ON ${dbName}.* TO ${user}"""
    connect(user=user, password="${pwd}", url=url) {
        try {
            sql "select ${dbName}.python_udf_auth_test(1)"
        } catch (Exception e) {
            fail()
        }
    }
}
