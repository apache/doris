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

import org.apache.doris.regression.util.JdbcUtils

// need nonConcurrent group
suite("test_flight_record", "nonConcurrent") {
    // only support jdk17+

    String version = System.getProperty("java.version")
    if (version.startsWith("1.") || Integer.valueOf(version.split("\\.")[0]) < 17) {
        logger.info("Only support jdk17+, current is ${version}, skip test")
        return
    }

    flightRecord {
        // whether delete jfr file after callback, default is true
        cleanUp true

        // the process name, default is DorisFE
        processName "DorisFE"

        // the jcmd extra config, default is empty
        extraConfig(["jdk.ObjectAllocationSample#throttle=\"100 /ns\""])


        // these sql will allocate some objects in localhost frontend,
        // and we start to record flight in localhost frontend
        record {
            def localhostFrontendUrl = JdbcUtils.replaceHostUrl(context.config.jdbcUrl, "127.0.0.1")
            connect(context.config.jdbcUser, context.config.jdbcPassword, localhostFrontendUrl) {
                for (def i in 0..10) {
                    sql "select 100"
                }
            }
        }

        // after record finished, we start to analyze the record events(items)
        callback { items, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            boolean filterNereids = true
            def allocationBytes = getAllocationBytes(items, filterNereids)
            logger.info("allocation bytes: " + allocationBytes)
        }
    }
}