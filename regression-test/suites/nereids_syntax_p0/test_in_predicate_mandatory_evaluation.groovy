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

suite("test_in_predicate_mandatory_evaluation") {
    test {
        sql """
            SELECT TRUE IN (TRUE, TRUE, assert_true(number > 0, 'in assertion'))
            FROM numbers('number' = '1')
        """
        exception "in assertion"
    }

    test {
        sql """
            SELECT TRUE NOT IN (TRUE, TRUE, assert_true(number > 0, 'not in assertion'))
            FROM numbers('number' = '1')
        """
        exception "not in assertion"
    }

    test {
        sql """
            SELECT CAST(1 AS BIGINT) IN (CAST(1 AS BIGINT), CAST(1 AS BIGINT),
                bitmap_count(to_bitmap_with_check(CAST(number AS BIGINT) - 1)))
            FROM numbers('number' = '1')
        """
        exception "The input: -1 is not valid"
    }

    test {
        sql """
            SELECT CAST(1 AS BIGINT) NOT IN (CAST(1 AS BIGINT), CAST(1 AS BIGINT),
                bitmap_count(to_bitmap_with_check(CAST(number AS BIGINT) - 1)))
            FROM numbers('number' = '1')
        """
        exception "The input: -1 is not valid"
    }
}
