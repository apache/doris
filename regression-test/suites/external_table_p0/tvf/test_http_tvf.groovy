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

// Licensed to the Apache Software Foundation (ASF) ...
import com.sun.net.httpserver.HttpServer
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpExchange
import java.nio.file.Files
import java.nio.file.Paths
import java.net.InetSocketAddress

suite("test_http_tvf", "p2") {
    // csv
    qt_sql01 """
        SELECT *
        FROM http(
            "uri" = "https://raw.githubusercontent.com/apache/doris/refs/heads/master/regression-test/data/load_p0/http_stream/all_types.csv",
            "format" = "csv",
            "column_separator" = ","
        )
        ORDER BY c1 limit 10;
    """

    qt_sql02 """
        SELECT count(*)
        FROM http(
            "uri" = "https://raw.githubusercontent.com/apache/doris/refs/heads/master/regression-test/data/load_p0/http_stream/all_types.csv",
            "format" = "csv",
            "column_separator" = ","
        );
    """

    qt_sql03 """
        desc function
        http(
            "uri" = "https://raw.githubusercontent.com/apache/doris/refs/heads/master/regression-test/data/load_p0/http_stream/all_types.csv",
            "format" = "csv",
            "column_separator" = ","
        );
    """

    qt_sql04 """
        desc function
        file(
            "uri" = "https://raw.githubusercontent.com/apache/doris/refs/heads/master/regression-test/data/load_p0/http_stream/all_types.csv",
            "format" = "csv",
            "fs.http.support" = "true",
            "column_separator" = ","
        );
    """

    // csv with gz
    qt_sql05 """
        desc function
        http(
            "uri" = "https://raw.githubusercontent.com/apache/doris/refs/heads/master/regression-test/data/load_p0/stream_load/all_types.csv.gz",
            "format" = "csv",
            "column_separator" = ",",
            "compress_type" = "gz"
        );
    """

    qt_sql05 """
        select count(*) from
        http(
            "uri" = "https://raw.githubusercontent.com/apache/doris/refs/heads/master/regression-test/data/load_p0/stream_load/all_types.csv.gz",
            "format" = "csv",
            "column_separator" = ",",
            "compress_type" = "gz"
        );
    """

    // json
    qt_sql06 """
        select * from
        http(
            "uri" = "https://raw.githubusercontent.com/apache/doris/refs/heads/master/regression-test/data/load_p0/stream_load/basic_data.json",
            "format" = "json",
            "strip_outer_array" = true
        ) order by k00;
    """

    qt_sql07 """
        desc function
        http(
            "uri" = "https://raw.githubusercontent.com/apache/doris/refs/heads/master/regression-test/data/load_p0/stream_load/basic_data.json",
            "format" = "json",
            "strip_outer_array" = true
        );
    """

    // parquet/orc
    qt_sql08 """
        select * from
        http(
            "uri" = "https://raw.githubusercontent.com/apache/doris/refs/heads/master/regression-test/data/external_table_p0/tvf/t.parquet",
            "format" = "parquet"
        ) order by id limit 10;
    """

    qt_sql09 """
        select arr_map, id from
        http(
            "uri" = "https://raw.githubusercontent.com/apache/doris/refs/heads/master/regression-test/data/external_table_p0/tvf/t.parquet",
            "format" = "parquet"
        ) order by id limit 10;
    """

    qt_sql10 """
        desc function
        http(
            "uri" = "https://raw.githubusercontent.com/apache/doris/refs/heads/master/regression-test/data/external_table_p0/tvf/t.parquet",
            "format" = "parquet"
        );
    """

    qt_sql11 """
        select m, id from
        http(
            "uri" = "https://raw.githubusercontent.com/apache/doris/refs/heads/master/regression-test/data/types/complex_types/mm.orc",
            "format" = "orc"
        ) order by id limit 10;
    """

    qt_sql12 """
        desc function
        http(
            "uri" = "https://raw.githubusercontent.com/apache/doris/refs/heads/master/regression-test/data/types/complex_types/mm.orc",
            "format" = "orc"
        );
    """
}
