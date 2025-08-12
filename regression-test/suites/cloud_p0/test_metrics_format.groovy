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

import org.apache.doris.regression.util.PromethuesChecker

suite('test_metrics_format') {
    if (!isCloudMode()) {
        log.info("not cloud mode just return")
        return
    }
    def get_meta_service_metric = { check_func ->
        httpTest {
            op "get"
            endpoint context.config.metaServiceHttpAddress
            uri "/brpc_metrics"
            check check_func
        }
    }

    get_meta_service_metric.call {
        respCode, body ->
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            Boolean res = PromethuesChecker.check(out)
            assertTrue(res)
    }
}