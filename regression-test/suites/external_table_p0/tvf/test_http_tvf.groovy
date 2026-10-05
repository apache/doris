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
import java.net.InetAddress
import java.net.URLDecoder

suite("test_http_tvf", "p0") {
    def dataRoot = Paths.get(context.config.dataPath).toAbsolutePath().normalize()
    HttpServer httpServer = HttpServer.create(new InetSocketAddress("0.0.0.0", 0), 0)

    def writeResponse = { HttpExchange exchange, int status, byte[] body ->
        exchange.sendResponseHeaders(status, body.length)
        exchange.responseBody.withCloseable { os ->
            os.write(body)
        }
    }

    def sendRangeNotSatisfiable = { HttpExchange exchange, long fileSize ->
        exchange.responseHeaders.add("Content-Range", "bytes */${fileSize}")
        exchange.sendResponseHeaders(416, -1)
        exchange.close()
    }

    def copyRange = { inputStream, outputStream, long length ->
        byte[] buffer = new byte[8192]
        long remaining = length
        while (remaining > 0) {
            int readSize = inputStream.read(buffer, 0, (int) Math.min(buffer.length, remaining))
            if (readSize < 0) {
                break
            }
            outputStream.write(buffer, 0, readSize)
            remaining -= readSize
        }
    }

    def skipFully = { inputStream, long length ->
        long remaining = length
        while (remaining > 0) {
            long skipped = inputStream.skip(remaining)
            if (skipped <= 0) {
                if (inputStream.read() < 0) {
                    break
                }
                skipped = 1
            }
            remaining -= skipped
        }
    }

    httpServer.createContext("/", { HttpExchange exchange ->
        if (!"GET".equalsIgnoreCase(exchange.requestMethod) && !"HEAD".equalsIgnoreCase(exchange.requestMethod)) {
            writeResponse(exchange, 405, "Method Not Allowed".getBytes("UTF-8"))
            return
        }

        def requestPath = URLDecoder.decode(exchange.requestURI.path, "UTF-8")
        def relativePath = requestPath.startsWith("/") ? requestPath.substring(1) : requestPath
        def filePath = dataRoot.resolve(relativePath).normalize()
        if (!filePath.startsWith(dataRoot)) {
            writeResponse(exchange, 403, "Forbidden".getBytes("UTF-8"))
            return
        }
        if (!Files.isRegularFile(filePath)) {
            writeResponse(exchange, 404, "Not Found".getBytes("UTF-8"))
            return
        }

        long fileSize = Files.size(filePath)
        exchange.responseHeaders.add("Accept-Ranges", "bytes")
        exchange.responseHeaders.add("Content-Type", "application/octet-stream")

        long start = 0
        long end = fileSize - 1
        int status = 200
        String rangeHeader = exchange.requestHeaders.getFirst("Range")
        if (rangeHeader != null && rangeHeader.startsWith("bytes=")) {
            String range = rangeHeader.substring("bytes=".length()).split(",", 2)[0].trim()
            try {
                if (range.startsWith("-")) {
                    long suffixLength = Long.parseLong(range.substring(1))
                    if (suffixLength <= 0) {
                        sendRangeNotSatisfiable(exchange, fileSize)
                        return
                    }
                    start = Math.max(0, fileSize - suffixLength)
                } else {
                    String[] parts = range.split("-", 2)
                    start = Long.parseLong(parts[0])
                    if (parts.length > 1 && parts[1].length() > 0) {
                        end = Long.parseLong(parts[1])
                    }
                }
                if (start < 0 || start >= fileSize || end < start) {
                    sendRangeNotSatisfiable(exchange, fileSize)
                    return
                }
                end = Math.min(end, fileSize - 1)
                status = 206
                exchange.responseHeaders.add("Content-Range", "bytes ${start}-${end}/${fileSize}")
            } catch (NumberFormatException e) {
                sendRangeNotSatisfiable(exchange, fileSize)
                return
            }
        }

        long responseLength = fileSize == 0 ? 0 : end - start + 1
        exchange.responseHeaders.add("Content-Length", Long.toString(responseLength))
        exchange.sendResponseHeaders(status, "HEAD".equalsIgnoreCase(exchange.requestMethod) ? -1 : responseLength)
        if ("HEAD".equalsIgnoreCase(exchange.requestMethod)) {
            exchange.close()
            return
        }
        Files.newInputStream(filePath).withCloseable { input ->
            skipFully(input, start)
            exchange.responseBody.withCloseable { output ->
                copyRange(input, output, responseLength)
            }
        }
    } as HttpHandler)
    httpServer.start()

    String httpServerHost = context.config.otherConfigs.get("httpTvfHost")
    if (httpServerHost == null || httpServerHost.trim().isEmpty()) {
        httpServerHost = context.config.otherConfigs.get("externalEnvIp")
    }
    if (httpServerHost == null || httpServerHost.trim().isEmpty()) {
        httpServerHost = InetAddress.getLocalHost().getHostAddress()
    }
    String httpBaseUrl = "http://${httpServerHost}:${httpServer.address.port}"
    def httpUrl = { String relativePath -> "${httpBaseUrl}/${relativePath}" }
    logger.info("Start local HTTP server for http tvf test: ${httpBaseUrl}, root: ${dataRoot}")

    try {
    // csv
    qt_sql01 """
        SELECT *
        FROM http(
            "uri" = "${httpUrl("load_p0/http_stream/all_types.csv")}",
            "format" = "csv",
            "column_separator" = ","
        )
        ORDER BY c1 limit 10;
    """

    qt_sql02 """
        SELECT count(*)
        FROM http(
            "uri" = "${httpUrl("load_p0/http_stream/all_types.csv")}",
            "format" = "csv",
            "column_separator" = ","
        );
    """

    qt_sql03 """
        desc function
        http(
            "uri" = "${httpUrl("load_p0/http_stream/all_types.csv")}",
            "format" = "csv",
            "column_separator" = ","
        );
    """

    qt_sql04 """
        desc function
        file(
            "uri" = "${httpUrl("load_p0/http_stream/all_types.csv")}",
            "format" = "csv",
            "fs.http.support" = "true",
            "column_separator" = ","
        );
    """

    // csv with gz
    qt_sql05 """
        desc function
        http(
            "uri" = "${httpUrl("load_p0/stream_load/all_types.csv.gz")}",
            "format" = "csv",
            "column_separator" = ",",
            "compress_type" = "gz"
        );
    """

    qt_sql05 """
        select count(*) from
        http(
            "uri" = "${httpUrl("load_p0/stream_load/all_types.csv.gz")}",
            "format" = "csv",
            "column_separator" = ",",
            "compress_type" = "gz"
        );
    """

    // json
    qt_sql06 """
        select count(*) from
        http(
            "uri" = "${httpUrl("load_p0/stream_load/basic_data.json")}",
            "format" = "json",
            "strip_outer_array" = true
        );
    """

    qt_sql07 """
        desc function
        http(
            "uri" = "${httpUrl("load_p0/stream_load/basic_data.json")}",
            "format" = "json",
            "strip_outer_array" = true
        );
    """

    // parquet/orc
    qt_sql08 """
        select * from
        http(
            "uri" = "${httpUrl("external_table_p0/tvf/t.parquet")}",
            "format" = "parquet"
        ) order by id limit 10;
    """

    qt_sql09 """
        select arr_map, id from
        http(
            "uri" = "${httpUrl("external_table_p0/tvf/t.parquet")}",
            "format" = "parquet"
        ) order by id limit 10;
    """

    qt_sql10 """
        desc function
        http(
            "uri" = "${httpUrl("external_table_p0/tvf/t.parquet")}",
            "format" = "parquet"
        );
    """

    qt_sql11 """
        select m, id from
        http(
            "uri" = "${httpUrl("types/complex_types/mm.orc")}",
            "format" = "orc"
        ) order by id limit 10;
    """

    qt_sql12 """
        desc function
        http(
            "uri" = "${httpUrl("types/complex_types/mm.orc")}",
            "format" = "orc"
        );
    """

    // non range
    test {
        sql """
            select * from
            http(
                "uri" = "${httpUrl("load_p0/stream_load/test_decimal.parquet")}",
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
            "uri" = "${httpUrl("datatype_p0/nested_types/query/test_nested_type_with_resize.csv")}",
            "format" = "csv",
            "http.enable.range.request"="false"
        );
    """

    qt_sql13 """
        select * from
        http(
            "uri" = "${httpUrl("load_p0/stream_load/test_decimal.parquet")}",
            "format" = "parquet",
            "http.enable.range.request" = "true",
            "http.max.request.size.bytes" = "1000"
        ) order by id;
    """

    qt_sql14 """
        select * from
        http(
            "uri" = "${httpUrl("load_p0/stream_load/test_decimal.parquet")}",
            "format" = "parquet",
            "http.enable.range.request" = "true",
            "http.max.request.size.bytes" = "2000"
        ) order by id;
    """

    // hf
    def hfCountOneFile = sql """
        select count(*) from
        http(
            "uri" = "hf://datasets/fka/awesome-chatgpt-prompts/blob/main/prompts.csv",
            "format" = "csv"
        );
    """
    assertTrue(Long.parseLong(hfCountOneFile[0][0].toString()) > 0)

    def hfCountWildcard = sql """
        select count(*) from
        http(
            "uri" = "hf://datasets/fka/awesome-chatgpt-prompts/blob/main/*.csv",
            "format" = "csv"
        );
    """
    assertTrue(Long.parseLong(hfCountWildcard[0][0].toString()) > 0)
    
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
    } finally {
        httpServer.stop(0)
    }
}
