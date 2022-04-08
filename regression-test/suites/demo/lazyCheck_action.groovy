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

suite("lazyCheck_action_exceptions", "demo") {
    /***** 1. lazy check exceptions *****/

    // will not throw exception immediately
    def result = lazyCheck {
        sql "a b c d e d" // syntax error
    }
    assertTrue(result == null)

    result = lazyCheck {
        sql "select 100"
    }
    assertEquals(result[0][0], 100)

    logger.info("You will see this log")

    // if you not clear the lazyCheckExceptions, and then,
    // after this suite execute finished, the syntax error in the lazyCheck action will be thrown.
    lazyCheckExceptions.clear()
}

suite("lazyCheck_action_futures", "demo") {
    /***** 2. lazy check futures *****/

    // start new thread and lazy check future
    def futureResult = lazyCheckThread {
        sql "a b c d e d f"
    }
    assertTrue(futureResult instanceof java.util.concurrent.Future)

    logger.info("You will see this log too")

    // if you not clear the lazyCheckFutures, and then,
    // after this suite execute finished, the syntax error in the lazyCheckThread action will be thrown.
    lazyCheckFutures.clear()
}