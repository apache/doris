import org.junit.Assert

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

suite("test_local_tvf_enclose", "p0,tvf") {
    List<List<Object>> backends =  sql """ show backends """
    assertTrue(backends.size() > 0)
    def be_id = backends[0][0]

    String filename = "enclose_csv.csv"

    def dataFilePath = context.config.dataPath + "/external_table_p0/tvf/${filename}"

    def outFilePath="/"

    for (List<Object> backend : backends) {
         def be_host = backend[1]
         scpFiles ("root", be_host, dataFilePath, outFilePath, false);
    }


    sql """set enable_nereids_planner=true"""
    sql """set enable_fallback_to_original_planner=false"""

    qt_enclose_1 """
                    select * from local(
                        "file_path" = "${filename}",
                        "backend_id" = "${be_id}",
                        "format" = "csv_with_names",
                        "column_separator" = ", ",
                        "enclose" = "\\\"") order by id;            
                """

    qt_enclose_2 """
                    select * from local(
                        "file_path" = "${filename}",
                        "backend_id" = "${be_id}",
                        "format" = "csv_with_names",
                        "column_separator" = ", ",
                        "enclose" = "\\\"",
                        "trim_double_quotes" = "true") order by id;            
                """

    // test error case
    test {
        sql """
                select * from local(
                        "file_path" = "${filename}",
                        "backend_id" = "${be_id}",
                        "format" = "csv_with_names",
                        "column_separator" = ", ",
                        "enclose" = "\\\"\\\"") order by id;            
                """
        // check exception message contains
        exception "enclose should not be longer than one byte."
    }
}
