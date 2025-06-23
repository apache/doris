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
suite("test_local_tvf_lzo", "p0,external,external_docker") {
    List<List<Object>> backends =  sql """ show backends """
    def dataFilePath = context.config.dataPath + "/external_table_p0/tvf/lzo"

    assertTrue(backends.size() > 0)

    def be_id = backends[0][0]
    // cluster mode need to make sure all be has this data
    def outFilePath="/"
    def transFile01="${dataFilePath}/test_compress.lzo"
    def transFile02="${dataFilePath}/test_no_compress_with_empty_block_begin.lzo"
    def transFile03="${dataFilePath}/test_no_compress_with_empty_block_end.lzo"
    def transFile04="${dataFilePath}/test_no_compress_with_empty_block_middle.lzo"

    for (List<Object> backend : backends) {
        def be_host = backend[1]
        scpFiles ("root", be_host, transFile01, outFilePath, false);
        scpFiles ("root", be_host, transFile02, outFilePath, false);
        scpFiles ("root", be_host, transFile03, outFilePath, false);
        scpFiles ("root", be_host, transFile04, outFilePath, false);
    }

    def file1 = outFilePath + "test_compress.lzo";
    def file2 = outFilePath + "test_no_compress_with_empty_block_begin.lzo";
    def file3 = outFilePath + "test_no_compress_with_empty_block_end.lzo";
    def file4 = outFilePath + "test_no_compress_with_empty_block_middle.lzo";

    order_qt_test_1 """ select * from local( "file_path" = "${file1}", "backend_id" = "${be_id}", "format" = "csv");"""
    order_qt_test_2 """ select * from local( "file_path" = "${file2}", "backend_id" = "${be_id}", "format" = "csv");"""
    order_qt_test_3 """ select * from local( "file_path" = "${file3}", "backend_id" = "${be_id}", "format" = "csv");"""
    order_qt_test_4 """ select * from local( "file_path" = "${file4}", "backend_id" = "${be_id}", "format" = "csv");"""

    qt_test_5 """ select count(*) from local( "file_path" = "${file1}", "backend_id" = "${be_id}", "format" = "csv");"""
    qt_test_6 """ select count(*) from local( "file_path" = "${file2}", "backend_id" = "${be_id}", "format" = "csv");"""
    qt_test_7 """ select count(*) from local( "file_path" = "${file3}", "backend_id" = "${be_id}", "format" = "csv");"""
    qt_test_8 """ select count(*) from local( "file_path" = "${file4}", "backend_id" = "${be_id}", "format" = "csv");"""

}
