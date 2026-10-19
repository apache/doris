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

suite("test_case_float_nonfinite") {
    for (def shortCircuit : [false, true]) {
        sql "set short_circuit_evaluation = ${shortCircuit}"
        for (def type : ["double", "float"]) {
            // Every row overflows from finite operands. Keeping number in the expression
            // prevents constant folding and makes unselected branches contain infinity too.
            def infinity = "cast((cast(number as double) + cast(1e308 as double)) * cast(1e308 as double) as ${type})"
            def nan = "cast((${infinity}) - (${infinity}) as ${type})"
            "qt_${type}_${shortCircuit}_then" """
                select number,
                       case when number = 0 then cast(1 as ${type})
                            when number = 1 then ${infinity} else cast(2 as ${type}) end,
                       case when number = 0 then cast(1 as ${type})
                            when number = 1 then cast(-(${infinity}) as ${type}) else cast(2 as ${type}) end,
                       case when number = 0 then cast(1 as ${type})
                            when number = 1 then ${nan} else cast(2 as ${type}) end
                from numbers("number" = "3") order by number
            """
            "qt_${type}_${shortCircuit}_else" """
                select number,
                       case when number = 0 then cast(1 as ${type})
                            when number = 1 then cast(2 as ${type}) else ${infinity} end,
                       case when number = 0 then cast(1 as ${type})
                            when number = 1 then cast(2 as ${type}) else cast(-(${infinity}) as ${type}) end,
                       case when number = 0 then cast(1 as ${type})
                            when number = 1 then cast(2 as ${type}) else ${nan} end
                from numbers("number" = "3") order by number
            """
            "qt_${type}_${shortCircuit}_nullable" """
                select number,
                       case when number = 0 then cast(1 as ${type})
                            when number = 1 then ${infinity} end
                from numbers("number" = "3") order by number
            """
            // The overflowing branch is evaluated in the first batch. Finite rows must
            // have the same result there and in the tail batch where it is never selected.
            "qt_${type}_${shortCircuit}_batches" """
                select result, count(*) from (
                    select case when number = 0 then cast(1 as ${type})
                                when number = 1 then ${infinity}
                                else cast(2 as ${type}) end as result
                    from numbers("number" = "4099")
                ) t group by result order by result
            """
        }
    }
}
