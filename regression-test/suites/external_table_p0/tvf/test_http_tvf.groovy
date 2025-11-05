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

    def port = 8800
    def httpRoot = "/tmp/http_test_files"
    def httpHost = "http://127.0.0.1:${port}"

    // 准备几个测试文件
    new File(httpRoot).mkdirs()
    new File("${httpRoot}/test.csv").text = """id,name,score
1,Alice,95
2,Bob,88
3,Charlie,91
"""
    new File("${httpRoot}/test_no_header.csv").text = """1,Alice,95
2,Bob,88
3,Charlie,91
"""
    new File("${httpRoot}/test.json").text = """{"id":1,"name":"Alice","score":95}
{"id":2,"name":"Bob","score":88}
{"id":3,"name":"Charlie","score":91}
"""


    def server = HttpServer.create(new InetSocketAddress(port), 0)
    server.createContext("/", { HttpExchange exchange ->
        def path = exchange.getRequestURI().getPath()
        def file = new File(httpRoot + path)
        if (file.exists()) {
            def bytes = Files.readAllBytes(file.toPath())
            exchange.sendResponseHeaders(200, bytes.length)
            exchange.responseBody.write(bytes)
        } else {
            exchange.sendResponseHeaders(404, 0)
            exchange.responseBody.write("Not Found".bytes)
        }
        exchange.responseBody.close()
    } as HttpHandler)
    server.start()
    logger.info("Embedded HTTP server started at ${httpHost}")

    
    qt_http_csv_basic """
        SELECT *
        FROM http(
            "uri" = "${httpHost}/test.csv",
            "format" = "csv",
            "csv_with_header" = "true",
            "csv_schema" = "id:int;name:string;score:int",
            "column_separator" = ",",
            "strict_mode" = "true"
        )
        ORDER BY id;
    """

    qt_http_json_line """
        SELECT *
        FROM http(
            "uri" = "${httpHost}/test.json",
            "format" = "json",
            "read_json_by_line" = "true"
        )
        ORDER BY id;
    """


    teardown {
        logger.info("Stop embedded HTTP server...")
        server.stop(0)
    }
}