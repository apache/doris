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

suite("test_large_http_header", "p0") {
    def feHost = context.config.feHttpAddress
    def (host, port) = feHost.split(":")

    // Test with large HTTP header (100KB)
    def largeHeaderValue = "x" * (100 * 1024)

    def url = "http://${host}:${port}/api/health"

    try {
        def connection = new URL(url).openConnection()
        connection.setRequestMethod("GET")
        connection.setRequestProperty("X-Large-Header", largeHeaderValue)
        connection.setConnectTimeout(5000)
        connection.setReadTimeout(5000)

        def responseCode = connection.getResponseCode()

        // Should not return 431 (Request Header Fields Too Large)
        assertTrue(responseCode != 431, "Should not return 431 error with large header")

        logger.info("Response code: ${responseCode}")
    } catch (Exception e) {
        logger.error("Test failed with exception: ${e.message}")
        throw e
    }
}
