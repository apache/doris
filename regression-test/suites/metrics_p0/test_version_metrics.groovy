import java.util.regex.Matcher
import java.util.regex.Pattern

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

suite("test_version_metrics") {
    httpTest {
        endpoint context.config.feHttpAddress
        uri "/metrics"
        op "get"
        check { code, body ->
            logger.debug("code:${code} body:${body}");
            assertEquals(200, code)
            assertTrue(body.contains("doris_fe_version"))
            for (final def line in body.split("\n")) {
                if (line.startsWith("doris_fe_version")) {
                    Pattern pattern = Pattern.compile(/^doris_fe_version\{.*}\s+(\d+)$/)
                    Matcher matcher = pattern.matcher(line)
                    assertTrue(matcher.matches())
                    assertTrue(Long.parseLong(matcher.group(1)) >= 0)
                    break
                }
            }
        }
    }

    def res = sql_return_maparray("show backends")
    def beBrpcEndpoint = res[0].Host + ":" + res[0].BrpcPort

    httpTest {
        endpoint beBrpcEndpoint
        uri "/brpc_metrics"
        op "get"
        check { code, body ->
            logger.debug("code:${code} body:${body}");
            assertEquals(200, code)
            assertTrue(body.contains("doris_be_version"))
            for (final def line in body.split("\n")) {
                if (line.contains("doris_be_version") && !line.contains("#")) {
                    assertTrue(Long.parseLong(line.split(" ")[1]) >= 0)
                }
            }
        }
    }

    if (cluster.isRunning() && cluster.isCloudMode()) {

        def ms = cluster.getAllMetaservices().get(0)
        def msEndpoint = ms.host + ":" + ms.httpPort

        httpTest {
            endpoint msEndpoint
            uri "/brpc_metrics"
            op "get"
            check { code, body ->
                logger.debug("code:${code} body:${body}");
                assertEquals(200, code)
                assertTrue(body.contains("doris_cloud_version"))
                for (final def line in body.split("\n")) {
                    if (line.contains("doris_cloud_version") && !line.contains("#")) {
                        assertTrue(Long.parseLong(line.split(" ")[1]) >= 0)
                    }
                }
            }
        }
    }
}
