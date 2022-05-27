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

suite("timer_action", "demo") {
    def (sumResult, elapsedMillis) = timer {
        long sum = 0
        (1..10000).each {sum += it}
        sum // return value
    }

    // you should convert GString to String because the log of GString not contains the correct file name,
    // e.g. (NativeMethodAccessorImpl.java:-2) - sum: 50005000, elapsed: 47 ms
    // so, invoke toString and then we can get the log:
    // (timer_action.groovy:10) - sum: 50005000, elapsed: 47 ms
    logger.info("sum: ${sumResult}, elapsed: ${elapsedMillis} ms".toString())
}
