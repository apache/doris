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

// An invalid regex pattern must raise the same error whether it is a constant
// or read from a column; it must never be silently turned into NULL.
suite("test_regexp_invalid_pattern") {
    sql "DROP TABLE IF EXISTS test_regexp_invalid_pattern"
    sql """
        CREATE TABLE test_regexp_invalid_pattern (
            id INT,
            s STRING,
            p STRING,
            repl STRING
        ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """
    sql """
        INSERT INTO test_regexp_invalid_pattern VALUES
            (1, 'abc', '(b)', 'x'),
            (2, 'abc', '[', 'x'),
            (3, 'abc', NULL, 'x');
    """

    // Valid column patterns keep working, NULL pattern still yields NULL.
    order_qt_extract_valid "SELECT id, regexp_extract(s, p, 1) FROM test_regexp_invalid_pattern WHERE id IN (1, 3)"
    order_qt_extract_or_null_valid "SELECT id, regexp_extract_or_null(s, p, 1) FROM test_regexp_invalid_pattern WHERE id IN (1, 3)"
    order_qt_extract_all_valid "SELECT id, regexp_extract_all(s, p) FROM test_regexp_invalid_pattern WHERE id IN (1, 3)"
    order_qt_extract_all_array_valid "SELECT id, regexp_extract_all_array(s, p) FROM test_regexp_invalid_pattern WHERE id IN (1, 3)"
    order_qt_replace_valid "SELECT id, regexp_replace(s, p, repl) FROM test_regexp_invalid_pattern WHERE id IN (1, 3)"
    order_qt_replace_one_valid "SELECT id, regexp_replace_one(s, p, repl) FROM test_regexp_invalid_pattern WHERE id IN (1, 3)"
    order_qt_count_valid "SELECT id, regexp_count(s, p) FROM test_regexp_invalid_pattern WHERE id IN (1, 3)"

    // Constant invalid pattern: rejected.
    test {
        sql "SELECT regexp_extract('abc', '[', 0)"
        exception "Invalid regex pattern"
    }
    test {
        sql "SELECT regexp_extract_or_null('abc', '[', 0)"
        exception "Invalid regex pattern"
    }
    test {
        sql "SELECT regexp_extract_all('abc', '[')"
        exception "Invalid regex pattern"
    }
    test {
        sql "SELECT regexp_extract_all_array('abc', '[')"
        exception "Invalid regex pattern"
    }
    test {
        sql "SELECT regexp_replace('abc', '[', 'x')"
        exception "Could not compile regexp pattern"
    }
    test {
        sql "SELECT regexp_replace_one('abc', '[', 'x')"
        exception "Could not compile regexp pattern"
    }
    test {
        sql "SELECT regexp_count('abc', '[')"
        exception "Could not compile regexp pattern"
    }

    // Column invalid pattern: rejected with the same error instead of returning NULL.
    test {
        sql "SELECT regexp_extract(s, p, 0) FROM test_regexp_invalid_pattern"
        exception "Invalid regex pattern"
    }
    test {
        sql "SELECT regexp_extract_or_null(s, p, 0) FROM test_regexp_invalid_pattern"
        exception "Invalid regex pattern"
    }
    test {
        sql "SELECT regexp_extract_all(s, p) FROM test_regexp_invalid_pattern"
        exception "Invalid regex pattern"
    }
    test {
        sql "SELECT regexp_extract_all_array(s, p) FROM test_regexp_invalid_pattern"
        exception "Invalid regex pattern"
    }
    test {
        sql "SELECT regexp_replace(s, p, repl) FROM test_regexp_invalid_pattern"
        exception "Could not compile regexp pattern"
    }
    test {
        sql "SELECT regexp_replace_one(s, p, repl) FROM test_regexp_invalid_pattern"
        exception "Could not compile regexp pattern"
    }
    test {
        sql "SELECT regexp_count(s, p) FROM test_regexp_invalid_pattern"
        exception "Could not compile regexp pattern"
    }

    // Constant string with column pattern is a column path too.
    test {
        sql "SELECT regexp_extract('abc', p, 0) FROM test_regexp_invalid_pattern WHERE id = 2"
        exception "Invalid regex pattern"
    }
    test {
        sql "SELECT regexp_replace('abc', p, 'x') FROM test_regexp_invalid_pattern WHERE id = 2"
        exception "Could not compile regexp pattern"
    }
}
