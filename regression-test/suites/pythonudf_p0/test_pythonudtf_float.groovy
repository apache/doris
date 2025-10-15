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

suite("test_pythonudtf_float") {
    def tableName = "test_pythonudtf_float"
    def pyPath = """${context.file.parent}/udtf_scripts/pyudtf.zip"""

    log.info("Python Zip path: ${pyPath}".toString())
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id`  INT    NOT NULL COMMENT "",
            `float_1`  FLOAT  NOT NULL COMMENT "",
            `float_2`  FLOAT           COMMENT "",
            `double_1` DOUBLE NOT NULL COMMENT "",
            `double_2` DOUBLE          COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
        
        
        sql """ INSERT INTO ${tableName} (`user_id`,`float_1`,`float_2`,double_1,double_2) VALUES
                (111,11111.11111,222222.3333333,12345678.34455677,1111111.999999999999),
                (112,1234556.11111,222222.3333333,222222222.3333333333333,4444444444444.555555555555),
                (113,87654321.11111,null,6666666666.6666666666,null)
            """
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY user_id; """

        File path = new File(pyPath)
        if (!path.exists()) {
            throw new IllegalStateException("""${pyPath} doesn't exist! """)
        }

        sql """DROP FUNCTION IF EXISTS udtf_float(float);"""
        sql """DROP FUNCTION IF EXISTS udtf_double(double);"""

        sql """ CREATE TABLES FUNCTION udtf_float(float) RETURNS array<float> PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="float_test",
            "type"="PYTHON_UDF"
        ); """

        sql """ CREATE TABLES FUNCTION udtf_double(double) RETURNS array<double> PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="double_test",
            "type"="PYTHON_UDF"
        ); """

        qt_select1 """ SELECT user_id, double_1, e1 FROM ${tableName} lateral view udtf_double(double_1) temp as e1 order by user_id; """
        qt_select2 """ SELECT user_id, double_2, e1 FROM ${tableName} lateral view udtf_double(double_2) temp as e1 order by user_id; """
        qt_select3 """ SELECT user_id, float_1, e1 FROM ${tableName} lateral view udtf_float(float_1) temp as e1 order by user_id; """
        qt_select4 """ SELECT user_id, float_2, e1 FROM ${tableName} lateral view udtf_float(float_2) temp as e1 order by user_id; """
    } finally {
        try_sql("DROP FUNCTION IF EXISTS udtf_float(float);")
        try_sql("DROP FUNCTION IF EXISTS udtf_double(double);")
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
