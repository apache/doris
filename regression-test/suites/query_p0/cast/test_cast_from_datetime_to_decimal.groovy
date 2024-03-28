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

suite("test_cast_from_datetime_to_decimalt") {
    // test cast from datetime to decimal
    qt_sql """select cast(a as decimal(30, 9)) from (select cast("2000-01-01" as datetime) a) t1;"""

    qt_sql """select cast(a as decimal(30, 9)) from (select cast("0000-00-01" as datetime) a) t1;"""
    
    qt_sql """select cast(a as decimal(30, 9)) from (select cast("0000-00-00" as datetime) a) t1;"""

    qt_sql """select cast(a as decimal(30, 9)) from (select cast("1999-12-31 13:59:59.999" as datetime(3)) a) t1;"""

    qt_sql """select cast(a as decimal(30, 9)) from (select cast("2000-02-29 11:11:11" as datetime) a) t1;"""

    qt_sql """select cast(a as decimal(30, 9)) from (select cast("2001-02-29 11:11:11" as datetime) a) t1;"""

    qt_sql """select cast(a as decimal(30, 9)) from (select cast("2000-02-29 11:11:11" as datetime(6)) a) t1;"""
    
    qt_sql """select cast(a as decimal(30, 3)) from (select cast("9999-12-31 23:59:59.999" as datetime(3)) a) t1;"""

    qt_sql """select cast(a as decimal(30, 9)) from (select cast("9999-12-31 23:59:59.999999" as datetime(6)) a) t1;"""

    test {
        sql """select cast(a as decimal(10, 0)) from (select cast("1999-12-31 13:59:59.999" as datetime) a) t1;"""
        exception "Arithmetic overflow, convert failed from 19991231140000, expected data is [-9999999999, 9999999999]"
    }

    qt_sql """select cast(a as decimal(30, 6)) from (select cast("9999-12-31 23:59:59.999999" as datetime(6)) a) t1;"""

    qt_sql """select cast(a as decimal(30, 7)) from (select cast("9999-12-31 23:59:59.999999" as datetime(6)) a) t1;"""

    qt_sql """select cast(a as decimal(30, 8)) from (select cast("9999-12-31 23:59:59.999999" as datetime(6)) a) t1;"""

    // what answer is correct for this sql?
    qt_sql """select cast(a as decimal(30, 4)) from (select cast("9999-12-31 23:59:59.999999" as datetime(6)) a) t1;"""
    
}    
    