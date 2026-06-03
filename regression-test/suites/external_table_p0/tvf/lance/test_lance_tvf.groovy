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

suite("test_lance_tvf", "p0,external,lance") {

    List<List<Object>> backends = sql """ show backends """
    assertTrue(backends.size() > 0)
    def be_id = backends[0][0]

    // Copy lance test datasets to all BE nodes (fromDst=false means upload TO BE)
    def dataFilePath = context.config.dataPath + "/external_table_p0/tvf/lance"
    def outFilePath = "/"
    for (List<Object> backend : backends) {
        def be_host = backend[1]
        scpFiles("root", be_host, dataFilePath + "/single.lance", outFilePath, false)
        scpFiles("root", be_host, dataFilePath + "/multi.lance", outFilePath, false)
    }

    def single_path = "single.lance/data/*.lance"
    def multi_path = "multi.lance/data/*.lance"

    // --- Test 1: SELECT * (single fragment, 5 rows) ---
    order_qt_select_all """
        select * from local(
            "file_path" = "${single_path}",
            "backend_id" = "${be_id}",
            "format" = "lance"
        ) order by id
    """

    // --- Test 2: Column projection ---
    order_qt_projection """
        select name, score from local(
            "file_path" = "${single_path}",
            "backend_id" = "${be_id}",
            "format" = "lance"
        ) order by name
    """

    // --- Test 3: COUNT(*) ---
    qt_count """
        select count(*) as cnt from local(
            "file_path" = "${single_path}",
            "backend_id" = "${be_id}",
            "format" = "lance"
        )
    """

    // --- Test 4: WHERE filter ---
    order_qt_filter """
        select * from local(
            "file_path" = "${single_path}",
            "backend_id" = "${be_id}",
            "format" = "lance"
        ) where score > 88 order by id
    """

    // --- Test 5: LIMIT ---
    qt_limit """
        select * from local(
            "file_path" = "${single_path}",
            "backend_id" = "${be_id}",
            "format" = "lance"
        ) order by id limit 2
    """

    // --- Test 6: Multi-fragment COUNT (3 fragments, 15 rows total, no duplicates) ---
    qt_multi_count """
        select count(*) as cnt, min(id) as min_id, max(id) as max_id from local(
            "file_path" = "${multi_path}",
            "backend_id" = "${be_id}",
            "format" = "lance"
        )
    """

    // --- Test 7: Multi-fragment with WHERE ---
    order_qt_multi_filter """
        select * from local(
            "file_path" = "${multi_path}",
            "backend_id" = "${be_id}",
            "format" = "lance"
        ) where id > 10 order by id
    """

    // --- Test 8: Multi-fragment aggregation ---
    qt_multi_agg """
        select sum(value) as total, avg(value) as avg_val from local(
            "file_path" = "${multi_path}",
            "backend_id" = "${be_id}",
            "format" = "lance"
        )
    """

    // --- Test 9: Error case - nonexistent path ---
    test {
        sql """
            select * from local(
                "file_path" = "nonexistent/path.lance",
                "backend_id" = "${be_id}",
                "format" = "lance"
            )
        """
        exception "No matches found"
    }
}
