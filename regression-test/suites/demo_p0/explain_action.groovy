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

suite("explain_action") {

    // this case check explain, so we disable nereids
    sql """set enable_nereids_planner=false"""

    explain {
        sql("select 100")

        // contains("OUTPUT EXPRS:\n    <slot 0> 100\n") && contains("PARTITION: UNPARTITIONED\n")
        contains "OUTPUT EXPRS:\n    100\n"
        contains "PARTITION: UNPARTITIONED\n"
    }

    explain {
        sql("select 100")

        // contains(" 100\n") && !contains("abcdefg") && !("1234567")
        contains " 100\n"
        notContains "abcdefg"
        notContains "1234567"
    }

    explain {
        sql("select 100")
        // simple callback
        check { explainStr -> explainStr.contains("abcdefg") || explainStr.contains(" 100\n") }
    }

    explain {
        sql("a b c d e")
        // callback with exception and time
        check { explainStr, exception, startTime, endTime ->
            // assertXxx() will invoke junit5's Assertions.assertXxx() dynamically
            assertTrue(exception != null)
        }
    }
}
