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

suite("test_date_function_const") {
    sql 'set enable_fold_constant_by_be = false;'

    qt_select1 """
        select hours_add('2023-03-30 22:23:45.23452',8)
    """
    qt_select2 """
        select date_add('2023-03-30 22:23:45.23452',8)
    """ 
    qt_select3 """
        select minutes_add('2023-03-30 22:23:45.23452',8)
    """
    // using cast 
    qt_select4 """
        select hours_add(cast('2023-03-30 22:23:45.23452' as datetimev2(4)),8)
    """
    qt_select5 """
        select hours_add(cast('2023-03-30 22:23:45.23452' as datetimev2(6)),8)
    """ 

    sql 'set enable_fold_constant_by_be = true;'


    qt_select6 """
        select hours_add('2023-03-30 22:23:45.23452',8)
    """
    qt_select7 """
        select date_add('2023-03-30 22:23:45.23452',8)
    """ 
    qt_select8 """
        select minutes_add('2023-03-30 22:23:45.23452',8)
    """
    // using cast 
    qt_select9 """
        select hours_add(cast('2023-03-30 22:23:45.23452' as datetimev2(4)),8)
    """
    qt_select10 """
        select hours_add(cast('2023-03-30 22:23:45.23452' as datetimev2(6)),8)
    """ 
    explain {
        sql("""select date_add(CURRENT_DATE(),-2);""")
        notContains("00:00:00")
    }

    test {
        sql """select date_add("1900-01-01 12:00:00.123456", interval 10000000000 month);"""
        exception "Operation months_add 133705200962757184 1410065408 out of range"
    }
}
