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

suite("test_array_distance_approximate") {
    for (entry in [["cosine_distance_approximate", "cosine_distance"],
            ["cosine_similarity_approximate", "cosine_similarity"]]) {
        for (vector in ["[1,0]", "[0,1]", "[0,0]", "[]"]) {
            def other = vector == "[]" ? "[]" : "[1,0]"
            assertEquals(sql("SELECT ${entry[1]}(${vector}, ${other})"),
                    sql("SELECT ${entry[0]}(${vector}, ${other})"))
        }
    }
    for (name in ["hamming_distance", "hamming_distance_approximate"]) {
        assertEquals(24.0f, sql("SELECT ${name}([0,255,170], [255,0,85])")[0][0] as float)
        assertEquals(0.0f, sql("SELECT ${name}([], [])")[0][0] as float)
        assertEquals(0.0f, sql("SELECT ${name}([255], [255])")[0][0] as float)
        def rows = sql """SELECT ${name}([number], [0]) FROM numbers("number"="4") ORDER BY number"""
        assertEquals([0.0f, 1.0f, 1.0f, 2.0f], rows.collect { it[0] as float })
        for (value in [-1, 256, 65536]) {
            test {
                sql "SELECT ${name}([${value}], [0])"
                exception "Hamming vector elements must be integers in [0, 255]"
            }
        }
        test {
            sql "SELECT ${name}([1.5], [0])"
            exception "Hamming distance requires integer arrays"
        }
        test {
            sql "SELECT ${name}([0], [0,1])"
            exception "different input element sizes"
        }
        test {
            sql "SELECT ${name}([NULL], [0])"
            exception "null"
        }
    }
}
