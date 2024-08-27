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

// This suit test the `backends` tvf
suite("test_local_tvf_parquet_unsigned_integers", "p0") {
    List<List<Object>> backends =  sql """ show backends """
    def dataFilePath = context.config.dataPath + "/external_table_p0/tvf/"

    assertTrue(backends.size() > 0)

    def be_id = backends[0][0]
    // cluster mode need to make sure all be has this data
    def outFilePath="/"
    def transFile01="${dataFilePath}/unsigned_integers_1.parquet"
    def transFile02="${dataFilePath}/unsigned_integers_2.parquet"
    def transFile03="${dataFilePath}/unsigned_integers_3.parquet"
    def transFile04="${dataFilePath}/unsigned_integers_4.parquet"

    for (List<Object> backend : backends) {
        def be_host = backend[1]
        scpFiles ("root", be_host, transFile01, outFilePath, false);
        scpFiles ("root", be_host, transFile02, outFilePath, false);
        scpFiles ("root", be_host, transFile03, outFilePath, false);
        scpFiles ("root", be_host, transFile04, outFilePath, false);
    }

    def file1 = outFilePath + "unsigned_integers_1.parquet";
    def file2 = outFilePath + "unsigned_integers_2.parquet";
    def file3 = outFilePath + "unsigned_integers_3.parquet";
    def file4 = outFilePath + "unsigned_integers_4.parquet";




    qt_test_1 """ select * from local( "file_path" = "${file1}", "backend_id" = "${be_id}", "format" = "parquet") order by id ;"""

    qt_test_2 """ desc function local( "file_path" = "${file1}", "backend_id" = "${be_id}", "format" = "parquet");"""

    qt_test_3 """ desc function local( "file_path" = "${file2}", "backend_id" = "${be_id}", "format" = "parquet");"""
    
    qt_test_4 """ desc function local( "file_path" = "${file3}", "backend_id" = "${be_id}", "format" = "parquet");"""

    qt_test_5 """ select * from local( "file_path" = "${file2}", "backend_id" = "${be_id}", "format" = "parquet") order by id ;"""

    qt_test_6 """ select * from local( "file_path" = "${file3}", "backend_id" = "${be_id}", "format" = "parquet") order by id limit 10;"""

    qt_test_7 """ desc function local( "file_path" = "${file4}", "backend_id" = "${be_id}", "format" = "parquet");"""

    qt_test_8 """ select * from local( "file_path" = "${file4}", "backend_id" = "${be_id}", "format" = "parquet") order by id ;"""



    qt_test_9 """ select * from local( "file_path" = "${file1}", "backend_id" = "${be_id}", "format" = "parquet") where uint8_column = 200 order by id ;"""

    qt_test_10 """ select * from local( "file_path" = "${file1}", "backend_id" = "${be_id}", "format" = "parquet") where uint16_column = 41727 order by id ;"""

    qt_test_11 """ select * from local( "file_path" = "${file1}", "backend_id" = "${be_id}", "format" = "parquet") where uint32_column = 2299955463 order by id ;"""

    qt_test_12 """ select * from local( "file_path" = "${file1}", "backend_id" = "${be_id}", "format" = "parquet") where uint64_column = 15103440093398422258 order by id ;"""



    qt_test_13 """ select * from local( "file_path" = "${file2}", "backend_id" = "${be_id}", "format" = "parquet") where uint8_column = 222 order by id ;"""

    qt_test_14 """ select * from local( "file_path" = "${file2}", "backend_id" = "${be_id}", "format" = "parquet") where uint16_column = 58068 order by id ;"""

    qt_test_15 """ select * from local( "file_path" = "${file2}", "backend_id" = "${be_id}", "format" = "parquet") where uint32_column = 4213847006 order by id ;"""

    qt_test_16 """ select * from local( "file_path" = "${file2}", "backend_id" = "${be_id}", "format" = "parquet") where uint64_column = 10613547124477746521 order by id ;"""


    qt_test_17 """ select count(id) from local( "file_path" = "${file3}", "backend_id" = "${be_id}", "format" = "parquet")  ;"""

    qt_test_18 """ select count(uint8_column) from local( "file_path" = "${file3}", "backend_id" = "${be_id}", "format" = "parquet")  ;"""

    qt_test_19 """ select count(uint16_column) from local( "file_path" = "${file3}", "backend_id" = "${be_id}", "format" = "parquet")  ;"""
    
    qt_test_20 """ select count(uint32_column) from local( "file_path" = "${file3}", "backend_id" = "${be_id}", "format" = "parquet")  ;"""
 
    qt_test_21 """ select count(uint64_column) from local( "file_path" = "${file3}", "backend_id" = "${be_id}", "format" = "parquet")  ;"""
    
    qt_test_22 """ select * from local( "file_path" = "${file3}", "backend_id" = "${be_id}", "format" = "parquet") where uint64_column = 18446744073709551614 order by id ;"""



}
