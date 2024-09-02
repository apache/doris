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

suite("test_math_unary_alway_nullable") {
    sql """
        set debug_skip_fold_constant=true;
    """

    qt_acos_1 """
        select acos(1.1), acos(1.1) is null;
    """
    qt_acos_2 """
        select acos(-1.1), acos(-1.1) is null;
    """
    qt_acos_3 """
        select acos(-1.1), acos(-1.1) is NULL, number from numbers("number"="10")
    """

    qt_asin_1 """
        select asin(1.1), asin(1.1) is null;
    """
    qt_asin_2 """
        select asin(-1.1), asin(-1.1) is null;
    """
    qt_asin_3 """
        select asin(-1.1), asin(-1.1) is NULL, number from numbers("number"="10")
    """

    qt_sqrt_1 """
        select sqrt(-1), sqrt(-1) is null;
    """
    qt_sqrt_2 """
        select sqrt(-1.1), sqrt(-1.1) is null;
    """
    qt_sqrt_3 """
        select sqrt(-1.1), sqrt(-1.1) is NULL, number from numbers("number"="10")
    """

}