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

suite("hll_agg") {
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    // hll_union, hll_raw_agg, hll_union_agg
    test {
        sql """
                select
                  hll_cardinality(hll_union(h)),
                  hll_cardinality(hll_raw_agg(h)),
                  hll_union_agg(h)
                from
                (
                    select hll_hash(number) h, number from numbers(number = 10) tmp
                ) t
                """
        result([[10L, 10L, 10L]])
    }

    test {
        sql """
                select
                  hll_union_agg(h)
                from
                (
                    select hll_hash(number) h, number from numbers(number = 161) tmp
                ) t
                """
        result([[162L]])
    }

    test {
        sql """
                select
                  hll_union_agg(h)
                from
                (
                    select hll_hash(number) h, number from numbers(number = 162) tmp
                ) t
                """
        result([[163L]])
    }

    test {
        sql """
                select
                  hll_union_agg(h)
                from
                (
                    select hll_hash(number) h, number from numbers(number = 1000) tmp
                ) t
                """
        result([[1001L]])
    }

    test {
        sql """
                select
                  hll_union_agg(h)
                from
                (
                    select hll_hash(number) h, number from numbers(number = 2000) tmp
                ) t
                """
        result([[2007L]])
    }
}
