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

suite("test_dump_image") {
    httpTest {
        endpoint context.config.feHttpAddress
        uri "/dump"
        op "get"
        check { code, body ->
            logger.debug("code:${code} body:${body}");
            assertEquals(200, code)

            def bodyJson = parseJson(body)
            assertEquals("success", bodyJson["msg"])
            def dumpFilePath = bodyJson["data"]["dumpFilePath"]
            logger.debug("dumpFilePath:${dumpFilePath}");
            assertTrue(dumpFilePath.contains("image."))

            File dumpFile = new File(dumpFilePath);
            dumpFile.delete();
            logger.info("dumpFile:${dumpFilePath} deleted");
        }
    }
}