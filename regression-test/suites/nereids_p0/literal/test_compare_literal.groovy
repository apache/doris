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

suite('test_compare_literal') {
    for (def val in [true, false]) {
        sql "set debug_skip_fold_constant=${val}"

        // ipv4
        test {
            sql "select cast('170.0.0.100' as ipv4) = cast('170.0.0.100' as ipv4)"
            result([[true]])
        }
        test {
            sql "select cast('170.0.0.100' as ipv4) >= cast('170.0.0.100' as ipv4)"
            result([[true]])
        }
        test {
            sql "select cast('170.0.0.100' as ipv4) > cast('170.0.0.100' as ipv4)"
            result([[false]])
        }
        test {
            sql "select cast('170.0.0.100' as ipv4) = cast('160.0.0.200' as ipv4)"
            result([[false]])
        }
        test {
            sql "select cast('170.0.0.100' as ipv4) >= cast('160.0.0.200' as ipv4)"
            result([[true]])
        }
        test {
            sql "select cast('170.0.0.100' as ipv4) > cast('160.0.0.200' as ipv4)"
            result([[true]])
        }
        test {
            sql "select cast('170.0.0.100' as ipv4) < cast('160.0.0.200' as ipv4)"
            result([[false]])
        }

        // ipv6
        test {
            sql "select cast('1080:0:0:0:8:800:200C:417A' as ipv6) = cast('1080:0:0:0:8:800:200C:417A' as ipv6)"
            result([[true]])
        }
        test {
            sql "select cast('1080:0:0:0:8:800:200C:417A' as ipv6) >= cast('1080:0:0:0:8:800:200C:417A' as ipv6)"
            result([[true]])
        }
        test {
            sql "select cast('1080:0:0:0:8:800:200C:417A' as ipv6) > cast('1080:0:0:0:8:800:200C:417A' as ipv6)"
            result([[false]])
        }
        test {
            sql "select cast('1080:0:0:0:8:800:200C:417A' as ipv6) = cast('1000:0:0:0:8:800:200C:41AA' as ipv6)"
            result([[false]])
        }
        test {
            sql "select cast('1080:0:0:0:8:800:200C:417A' as ipv6) >= cast('1000:0:0:0:8:800:200C:41AA' as ipv6)"
            result([[true]])
        }
        test {
            sql "select cast('1080:0:0:0:8:800:200C:417A' as ipv6) > cast('1000:0:0:0:8:800:200C:41AA' as ipv6)"
            result([[true]])
        }
        test {
            sql "select cast('1080:0:0:0:8:800:200C:417A' as ipv6) < cast('1000:0:0:0:8:800:200C:41AA' as ipv6)"
            result([[false]])
        }

        // array
        test {
            sql 'select array(5, 6) = array(5, 6)'
            result([[true]])
        }
        test {
            sql 'select array(5, 6) >= array(5, 6)'
            result([[true]])
        }
        test {
            sql 'select array(5, 6) > array(5, 6)'
            result([[false]])
        }
        test {
            sql 'select array(5, 6) = array(5, 7)'
            result([[false]])
        }
        test {
            sql 'select array(5, 6) >= array(5, 7)'
            result([[false]])
        }
        test {
            sql 'select array(5, 6) > array(5, 7)'
            result([[false]])
        }
        test {
            sql 'select array(5, 6) < array(5, 7)'
            result([[true]])
        }
        test {
            sql 'select array(5, 6) < array(5, 6, 1)'
            result([[true]])
        }
        test {
            sql 'select array(5, 6) < array(6)'
            result([[true]])
        }
    }

    // test not comparable
    sql 'set debug_skip_fold_constant=false'

    // json
    test {
        sql "select cast('[1, 2]' as json) = cast('[1, 2]' as json)"
        exception 'comparison predicate could not contains json type'
    }
    test {
        sql "select cast('[1, 2]' as json) > cast('[1, 2]' as json)"
        exception 'comparison predicate could not contains json type'
    }

    // map
    test {
        sql 'select map(1, 2) = map(1, 2)'
        exception 'comparison predicate could not contains complex type'
    }
    test {
        sql 'select map(1, 2) > map(1, 2)'
        exception 'comparison predicate could not contains complex type'
    }

    // struct
    test {
        sql 'select struct(1, 2) = struct(1, 2)'
        exception 'comparison predicate could not contains complex type'
    }
    test {
        sql 'select struct(1, 2) > struct(1, 2)'
        exception 'comparison predicate could not contains complex type'
    }
}
