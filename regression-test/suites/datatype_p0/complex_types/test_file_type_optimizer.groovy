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

suite("test_file_type_optimizer") {
    def publicStruct = "STRUCT<uri:VARCHAR(65533),offset:BIGINT,size:BIGINT," +
            "content_type:VARCHAR(1024),checksum:VARCHAR(1024),inline:VARBINARY>"
    def fileCtes = """
        WITH file_values AS (
            SELECT number AS id, CAST(JSON_PARSE(CONCAT('{"uri":"urn:optimizer:', number,
                '","offset":null,"size":null,"content_type":null,"checksum":null,"inline":null}')) AS FILE) AS f
            FROM numbers("number"="3")
        ), file_scan AS (
            SELECT id, f, CAST(ARRAY(CAST(f AS ${publicStruct})) AS ARRAY<FILE>) AS a,
                   CAST(NAMED_STRUCT('f', CAST(f AS ${publicStruct})) AS STRUCT<f:FILE>) AS s,
                   CAST(MAP('key', CAST(f AS ${publicStruct})) AS MAP<STRING,FILE>) AS m
            FROM file_values
        )
    """
    def fileQuery = { String statement -> "${fileCtes} ${statement}" }
    test {
        sql fileQuery("SELECT (SELECT i.f FROM file_scan i WHERE i.id = o.id) FROM file_scan o")
        exception "Agg Function any_value(FILE) is not implemented"
    }
    for (def field in ["a", "s", "m"]) {
        sql fileQuery("SELECT (SELECT i.${field} FROM file_scan i WHERE i.id = o.id) FROM file_scan o")
    }
    sql fileQuery("SELECT (SELECT i.id FROM file_scan i WHERE i.id = o.id) FROM file_scan o")
    sql fileQuery("SELECT (SELECT ELEMENT_AT(i.f, 'uri') FROM file_scan i WHERE i.id = o.id) FROM file_scan o")

    def originalSqlMode = sql("SELECT @@sql_mode")[0][0]
    try {
        sql "SET sql_mode = ''"
        test {
            sql fileQuery("SELECT f, COUNT(*) FROM file_scan")
            exception "Agg Function any_value(FILE) is not implemented"
        }
        for (def field in ["a", "s", "m"]) {
            sql fileQuery("SELECT ${field}, COUNT(*) FROM file_scan")
        }
        sql fileQuery("SELECT id, COUNT(*) FROM file_scan")
        sql fileQuery("SELECT COUNT(f), COUNT(*) FROM file_scan")
    } finally {
        sql "SET sql_mode = '${originalSqlMode}'"
    }
}
