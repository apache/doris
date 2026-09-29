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
    def requiredVersionLabels = ["version", "major", "minor", "patch", "hotfix", "short_hash"] as Set
    def parseVersionMetric = { body, metricName ->
        def parsedMetric = null
        Pattern pattern = Pattern.compile("^" + Pattern.quote(metricName) + "\\{([^}]*)}\\s+(\\d+)$")
        for (final def line in body.readLines()) {
            Matcher matcher = pattern.matcher(line)
            if (!matcher.matches()) {
                continue
            }

            def labels = [:]
            for (String label : matcher.group(1).split(",")) {
                String[] keyValue = label.trim().split("=", 2)
                assertEquals(2, keyValue.length)
                assertTrue(keyValue[1].startsWith("\"") && keyValue[1].endsWith("\""))
                labels[keyValue[0]] = keyValue[1].substring(1, keyValue[1].length() - 1)
            }
            assertEquals(requiredVersionLabels, labels.keySet())
            parsedMetric = [labels: labels, value: Long.parseLong(matcher.group(2))]
            break
        }
        assertNotNull(parsedMetric)
        assertTrue(parsedMetric.value >= 0)
        return parsedMetric
    }

    def feVersionMetric = null
    httpTest {
        endpoint context.config.feHttpAddress
        uri "/metrics"
        op "get"
        check { code, body ->
            logger.debug("code:${code} body:${body}");
            assertEquals(200, code)
            assertTrue(body.contains("doris_fe_version"))
            feVersionMetric = parseVersionMetric(body, "doris_fe_version")
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
            def beVersionMetric = parseVersionMetric(body, "doris_be_version")
            assertEquals(feVersionMetric, beVersionMetric)
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
                def cloudVersionMetric = parseVersionMetric(body, "doris_cloud_version")
                assertEquals(feVersionMetric, cloudVersionMetric)
            }
        }
    }
}
