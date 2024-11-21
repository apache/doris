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

suite("operator_precedence") {

    test {
        sql """SELECT - 2 + 2 """
        result([[0]])
    }

    test {
        sql """SELECT 4 * 2 ^ 3"""
        result([[4]])
    }

    test {
        sql """SELECT 2 + 2 * 2"""
        result([[6]])
    }

    test {
        sql """SELECT 4 & 4 - 2"""
        result([[0]])
    }

    test {
        sql """SELECT 8 | 0 & 0"""
        result([[8]])
    }

    test {
        sql """SELECT 5 > 6 | 1"""
        result([[false]])
    }

    test {
        sql """SELECT 5 > 6 AND 1"""
        result([[false]])
    }

    test {
        sql """SELECT TRUE XOR FALSE AND FALSE"""
        result([[true]])
    }

    test {
        sql """SELECT TRUE OR FALSE AND FALSE"""
        result([[true]])
    }

    test {
        sql """SELECT TRUE OR FALSE XOR TRUE"""
        result([[true]])
    }
}
