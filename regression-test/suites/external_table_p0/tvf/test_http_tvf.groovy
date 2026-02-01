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
        select count(*) from
        http(
            "uri" = "https://raw.githubusercontent.com/apache/doris/refs/heads/master/regression-test/data/load_p0/stream_load/basic_data.json",
            "format" = "json",
            "strip_outer_array" = true
        );
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

    // non range
    test {
        sql """
            select * from
            http(
                "uri" = "https://raw.githubusercontent.com/apache/doris/refs/heads/master/regression-test/data/load_p0/stream_load/test_decimal.parquet",
                "format" = "parquet",
                "http.enable.range.request" = "false",
                "http.max.request.size.bytes" = "1000"
            );
        """
        exception """exceeds maximum allowed size (1000 bytes"""
    }

    // non range and large than readBuffer(1MB)
    qt_sql_norange """
        SELECT count(1) FROM
        HTTP(
            "uri" = "https://raw.githubusercontent.com/apache/doris/refs/tags/4.0.2-rc02/regression-test/data/datatype_p0/nested_types/query/test_nested_type_with_resize.csv",
            "format" = "csv",
            "http.enable.range.request"="false"
        );
    """

    qt_sql13 """
        select * from
        http(
            "uri" = "https://raw.githubusercontent.com/apache/doris/refs/heads/master/regression-test/data/load_p0/stream_load/test_decimal.parquet",
            "format" = "parquet",
            "http.enable.range.request" = "true",
            "http.max.request.size.bytes" = "1000"
        ) order by id;
    """

    qt_sql14 """
        select * from
        http(
            "uri" = "https://raw.githubusercontent.com/apache/doris/refs/heads/master/regression-test/data/load_p0/stream_load/test_decimal.parquet",
            "format" = "parquet",
            "http.enable.range.request" = "true",
            "http.max.request.size.bytes" = "2000"
        ) order by id;
    """

    // hf
    qt_sql15 """
        select count(*) from
        http(
            "uri" = "hf://datasets/fka/awesome-chatgpt-prompts/blob/main/prompts.csv",
            "format" = "csv"
        );
    """

    qt_sql16 """
        select count(*) from
        http(
            "uri" = "hf://datasets/fka/awesome-chatgpt-prompts/blob/main/*.csv",
            "format" = "csv"
        );
    """
    
    qt_sql17 """
        desc function
        http(
            "uri" = "hf://datasets/fka/awesome-chatgpt-prompts/blob/main/*.csv",
            "format" = "csv"
        );
    """

    // branch
    qt_sql18 """
        select * from
        http(
            "uri" = "hf://datasets/stanfordnlp/imdb@script/dataset_infos.json",
            "format" = "json"
        );
    """

    qt_sql19 """
        select * from
        http(
            "uri" = "hf://datasets/stanfordnlp/imdb@main/plain_text/test-00000-of-00001.parquet",
            "format" = "parquet"
        ) order by text limit 1;
    """

    // wildcard
    qt_sql20 """
        select * from
        http(
            "uri" = "hf://datasets/stanfordnlp/imdb@main/*/test-00000-of-00001.parquet",
            "format" = "parquet"
        ) order by text limit 1;
    """

    qt_sql21 """
        select * from
        http(
            "uri" = "hf://datasets/stanfordnlp/imdb@main/*/*.parquet",
            "format" = "parquet"
        ) order by text limit 1;
    """

    qt_sql21 """
        select * from
        http(
            "uri" = "hf://datasets/stanfordnlp/imdb@main/**/test-00000-of-0000[1].parquet",
            "format" = "parquet"
        ) order by text limit 1;
    """
}

