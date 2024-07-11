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

suite("test_tvf_csv", "p0,tvf") {
    List<List<Object>> backends =  sql """ show backends """
    assertTrue(backends.size() > 0)
    def be_id = backends[0][0]
    def dataFilePath = context.config.dataPath + "/external_table_p0/tvf/mixed_line_endings.csv"

    def outFilePath="/tvf"

    for (List<Object> backend : backends) {
        def be_host = backend[1]
        scpFiles ("root", be_host, dataFilePath, outFilePath, false);
    }

    String filename = "mixed_line_endings.csv"

    sql """set enable_nereids_planner=true"""
    sql """set enable_fallback_to_original_planner=false"""

    sql """ set keep_carriage_return = true; """
    qt_csv_1"""
    select * from local(
        "file_path" = "${outFilePath}/${filename}",
        "backend_id" = "${be_id}",
        "format" = "csv",
        "column_separator" = ","
        )
        order by c1,c2,c3,c4;            
    """

    qt_csv_2"""
    select length(c4) from local(
        "file_path" = "${outFilePath}/${filename}",
        "backend_id" = "${be_id}",
        "format" = "csv",
        "column_separator" = ","
        ) where length(c4) == 2;            
    """

    
    sql """ set keep_carriage_return = false; """

    qt_csv_3 """
    select * from local(
        "file_path" = "${outFilePath}/${filename}",
        "backend_id" = "${be_id}",
        "format" = "csv",
        "column_separator" = ","
        )
        order by c1,c2,c3,c4;            
    """

    qt_csv_4 """
    select length(c4) from local(
        "file_path" = "${outFilePath}/${filename}",
        "backend_id" = "${be_id}",
        "format" = "csv",
        "column_separator" = ","
        ) where length(c4) == 2;            
    """


}
