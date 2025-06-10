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

import org.junit.Assert

// This is a good example.
// Debug point must run in 'nonConcurrent' or 'docker' suites.
// If not a docker suite, must put nonConcurrent to the groups.
suite('debugpoint_action', 'nonConcurrent') {
    try {
        GetDebugPoint().enableDebugPointForAllFEs('PublishVersionDaemon.stop_publish', [timeout:1])
        GetDebugPoint().enableDebugPointForAllBEs('Tablet.build_tablet_report_info.version_miss',
                [tablet_id:'12345', version_miss:true, timeout:1])
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs('PublishVersionDaemon.stop_publish')
        GetDebugPoint().disableDebugPointForAllBEs('Tablet.build_tablet_report_info.version_miss')
    }
}

// This is a bad example.
// its group tag not contains nonConcurrent or docker.
suite('debugpoint_action_bad') {
    Exception exception = null;
    try {
        GetDebugPoint().enableDebugPointForAllFEs('debugpoint_action_bad_xx', [timeout:1])
    } catch (Exception e) {
        exception = e
    } finally {
        // this is bad example, should disable or clear debug points after end suite
        // GetDebugPoint().disableDebugPointForAllFEs('debugpoint_action_bad_xx')
    }

    Assert.assertNotNull(exception)
    def expectMsg = "Debug point must use in nonConcurrent suite or docker suite"
    def msg = exception.toString()
    log.info("meet exception: ${msg}")
    Assert.assertTrue("Expect exception msg contains '${expectMsg}', but meet '${msg}'",
            msg.contains(expectMsg))
}
