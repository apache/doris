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

suite("test_math_function") {
    testFoldConst""" select cast('nan' as double) union all select cast('inf' as double) union all select cast('-inf' as double) union all select cast('0.0' as double) union all select cast('1.0' as double) union all select cast('-1.0' as double) union all select cast('1.0000001' as double) union all select cast('-1.0000001' as double) """

    qt_select_acos""" select acos(cast('nan' as double)), acos(cast('inf' as double)), acos(cast('-inf' as double)), acos(cast('0.0' as double)), acos(cast('1.0' as double)), acos(cast('-1.0' as double)), acos(cast('1.0000001' as double)), acos(cast('-1.0000001' as double)) """
   
    testFoldConst""" select cast('nan' as double) union all select cast('inf' as double) union all select cast('-inf' as double) union all select cast('0.0' as double) union all select cast('1.0' as double) union all select cast('-1.0' as double) union all select cast('0.9999999' as double) union all select cast('1.0000001' as double) """

    qt_select_acosh""" select acosh(cast('nan' as double)), acosh(cast('inf' as double)), acosh(cast('-inf' as double)), acosh(cast('0.0' as double)), acosh(cast('1.0' as double)), acosh(cast('-1.0' as double)), acosh(cast('0.9999999' as double)), acosh(cast('1.0000001' as double)) """

    testFoldConst""" select cast('nan' as double) union all select cast('inf' as double) union all select cast('-inf' as double) union all select cast('0.0' as double) union all select cast('-0.0' as double) union all select cast('1.0' as double) union all select cast('-1.0' as double) union all select cast('1.0000001' as double) union all select cast('-1.0000001' as double) """

    qt_select_asin""" select asin(cast('nan' as double)), asin(cast('inf' as double)), asin(cast('-inf' as double)), asin(cast('0.0' as double)), asin(cast('-0.0' as double)), asin(cast('1.0' as double)), asin(cast('-1.0' as double)), asin(cast('1.0000001' as double)), asin(cast('-1.0000001' as double)) """

    testFoldConst""" select cast('nan' as double) union all select cast('inf' as double) union all select cast('-inf' as double) union all select cast('0.0' as double) union all select cast('-0.0' as double) union all select cast('1.0' as double) union all select cast('-1.0' as double) union all select cast('1.0000001' as double) union all select cast('-1.0000001' as double) """

    qt_select_atanh""" select atanh(cast('nan' as double)), atanh(cast('inf' as double)), atanh(cast('-inf' as double)), atanh(cast('0.0' as double)), atanh(cast('-0.0' as double)), atanh(cast('1.0' as double)), atanh(cast('-1.0' as double)), atanh(cast('1.0000001' as double)), atanh(cast('-1.0000001' as double)) """

    testFoldConst""" select cast('nan' as double) union all select cast('inf' as double) union all select cast('-inf' as double) union all select cast('0.0' as double) union all select cast('-0.0' as double) union all select cast('1.0' as double) union all select cast('-1.0' as double) union all select cast('2.0' as double) union all select cast('-2.0' as double) """

    qt_select_sqrt""" select sqrt(cast('nan' as double)), sqrt(cast('inf' as double)), sqrt(cast('-inf' as double)), sqrt(cast('0.0' as double)), sqrt(cast('-0.0' as double)), sqrt(cast('1.0' as double)), sqrt(cast('-1.0' as double)), sqrt(cast('2.0' as double)), sqrt(cast('-2.0' as double)) """

}
