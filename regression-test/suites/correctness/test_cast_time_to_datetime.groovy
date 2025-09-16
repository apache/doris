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

suite("test_cast_time_to_datetime") {
    waitUntilSafeExecutionTime("NOT_CROSS_DAY_BOUNDARY", 2)
    def result1 = sql """ select datediff(now(), from_unixtime(cast(1742194502 as bigint),'yyyy-MM-dd HH:mm:ss')); """
    def result2 = sql """ select datediff(current_time(), from_unixtime(cast(1742194502 as bigint),'yyyy-MM-dd HH:mm:ss')); """
    assertEquals(result1[0][0], result2[0][0], "The results of the two SQL queries should be the same.")
    explain {
        sql """select cast(cast("500:00:00" as time) as datetime)""" // not legal for date part
        notContains "CAST"
    }
    explain {
        sql """select cast(cast("10:10:10" as time) as datetime)""" // also legal for date part
        notContains "CAST"
    }
    explain {
        sql """select cast(cast("500:00:00" as time) as date)""" // not legal for date part
        notContains "CAST"
    }
    explain {
        sql """select cast(cast("10:10:10" as time) as date)""" // also legal for date part
        notContains "CAST"
    }
}