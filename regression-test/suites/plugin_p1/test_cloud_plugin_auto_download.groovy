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

suite("test_cloud_plugin_auto_download", "p1,external") {

    //sass cloud-mode only
    if (!isCloudMode() || enableStoragevault()) {
        logger.info("Skip test_plugin_auto_download because not in sass cloud mode")
        return
    }

    sql """drop database if exists internal.test_auto_download_db; """
    sql """create database if not exists internal.test_auto_download_db;"""

    sql """ use internal.test_auto_download_db; """

    sql """DROP FUNCTION IF EXISTS java_udf_add_one(int)"""

    sql """ CREATE FUNCTION java_udf_add_one(int) RETURNS int PROPERTIES (
        "file"="java-udf-demo-jar-with-dependencies.jar",
        "symbol"="org.apache.doris.udf.AddOne",
        "type"="JAVA_UDF"
    ); """

    def result2 = sql """
        select java_udf_add_one(100) as result
    """
    assertTrue(result2.size() > 0)
    assertEquals(result2[0][0], 101)

    sql """DROP FUNCTION IF EXISTS java_udf_add_one(int)"""

    // negative test case: non-existent UDF jar
    sql """DROP FUNCTION IF EXISTS java_udf_non_existent(int)"""
    try {
        sql """ CREATE FUNCTION java_udf_non_existent(int) RETURNS int PROPERTIES (
            "file"="non-existent-udf.jar",
            "symbol"="org.apache.doris.udf.NonExistent",
            "type"="JAVA_UDF"
        ); """

        sql """
            select java_udf_non_existent(100) as result
        """
        assertTrue(false, "Should have thrown exception for non-existent UDF jar")
    } catch (Exception e) {
        logger.info("Expected exception for non-existent UDF jar: " + e.getMessage())
        assertTrue(e.getMessage().contains("has been uploaded to cloud"))
    } finally {
        sql """DROP FUNCTION IF EXISTS java_udf_non_existent(int)"""
    }

    sql """ drop database if exists internal.test_auto_download_db; """
}
