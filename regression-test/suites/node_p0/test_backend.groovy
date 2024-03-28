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

suite("test_backend", "nonConcurrent") {
    def address = "127.0.0.1"
    def notExistPort = 12346

    for (int i = 0; i < 2; i++) {
        def result = sql """SHOW BACKENDS;"""
        logger.info("result:${result}")

        sql """ALTER SYSTEM ADD BACKEND "${address}:${notExistPort}";"""

        result = sql """SHOW BACKENDS;"""
        logger.info("result:${result}")

        sql """ALTER SYSTEM MODIFY BACKEND "${address}:${notExistPort}" SET ("disable_query" = "true"); """
        sql """ALTER SYSTEM MODIFY BACKEND "${address}:${notExistPort}" SET ("disable_load" = "true"); """

        result = sql """SHOW BACKENDS;"""
        logger.info("result:${result}")

        sql """ALTER SYSTEM DROPP BACKEND "${address}:${notExistPort}";"""

        result = sql """SHOW BACKENDS;"""
        logger.info("result:${result}")
    }

    if (context.config.jdbcUser.equals("root")) {
        def beId1 = null
        try {
            GetDebugPoint().enableDebugPointForAllFEs("SystemHandler.decommission_no_check_replica_num");
            try_sql """admin set frontend config("drop_backend_after_decommission" = "false")"""
            def result = sql_return_maparray """SHOW BACKENDS;"""
            logger.info("show backends result:${result}")
            for (def res : result) {
                beId1 = res.BackendId
                break
            }
            result = sql """ALTER SYSTEM DECOMMISSION BACKEND "${beId1}" """
            logger.info("ALTER SYSTEM DECOMMISSION BACKEND ${result}")
            result = sql_return_maparray """SHOW BACKENDS;"""
            for (def res : result) {
                if (res.BackendId == "${beId1}") {
                    assertTrue(res.SystemDecommissioned.toBoolean())
                }
            }
        } finally {
            try {
                if (beId1 != null) {
                    def result = sql """CANCEL DECOMMISSION BACKEND "${beId1}" """
                    logger.info("CANCEL DECOMMISSION BACKEND ${result}")

                    result = sql_return_maparray """SHOW BACKENDS;"""
                    for (def res : result) {
                        if (res.BackendId == "${beId1}") {
                            assertFalse(res.SystemDecommissioned.toBoolean())
                        }
                    }
                }
            } finally {
                GetDebugPoint().disableDebugPointForAllFEs('SystemHandler.decommission_no_check_replica_num');
                try_sql """admin set frontend config("drop_backend_after_decommission" = "true")"""
            }
        }
    }
}
