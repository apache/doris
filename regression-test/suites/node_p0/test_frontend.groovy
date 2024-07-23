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

suite("test_frontend") {
    def address = "127.0.0.1"
    def notExistPort = 12345

    for (int i = 0; i < 2; i++) {
        def result = sql """SHOW FRONTENDS;"""
        logger.debug("result:${result}")

        sql """ALTER SYSTEM ADD FOLLOWER "${address}:${notExistPort}";"""
        result = sql """SHOW FRONTENDS;"""
        logger.debug("result:${result}")

        sql """ALTER SYSTEM DROP FOLLOWER "${address}:${notExistPort}";"""
        result = sql """SHOW FRONTENDS;"""
        logger.debug("result:${result}")

        sql """ALTER SYSTEM ADD OBSERVER "${address}:${notExistPort}";"""
        result = sql """SHOW FRONTENDS;"""
        logger.debug("result:${result}")

        sql """ALTER SYSTEM DROP OBSERVER "${address}:${notExistPort}";"""
        result = sql """SHOW FRONTENDS;"""
        logger.debug("result:${result}")
    }

    def res = sql """SHOW FRONTENDS DISKS"""
    assertTrue(res.size() != 0)

    def res2 = sql """SHOW FRONTENDS Disks"""
    assertTrue(res2.size() != 0)
}
