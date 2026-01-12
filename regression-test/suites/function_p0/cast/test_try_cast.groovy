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


suite("test_try_cast") {
    // Test casting from integer types to boolean
    sql "set debug_skip_fold_constant=true;"

    sql "set enable_strict_cast=true;"

    sql """
        Drop table if exists test_try_cast;
    """

    sql """
        CREATE TABLE test_try_cast (id INT, str string , not_null_str string not null) DISTRIBUTED BY HASH(id) BUCKETS 4 PROPERTIES ("replication_num" = "1");
    """


    sql """
        insert into test_try_cast values (1, '123', '123'), (2, 'abc', 'abc'), (3, null, 'not null');
    """ 


    qt_sql """
        select id, str, try_cast(str as int)  , try_cast(not_null_str as int) from test_try_cast order by id;
    """


    qt_sql """
        select try_cast('abc' as int);
    """


    qt_sql """
        select try_cast('123' as int);
    """


    test {
        sql """select cast('abc' as int);"""
        exception "fail"
    }



    test {
        sql """select cast('[[[]]]' as array<int>);"""
        exception "fail"
    }

    qt_sql """
        select try_cast('[[[]]]' as array<int>);
    """


    test {
        sql """select cast('{123:456}' as json);""" 
        exception "Failed to parse json string"
    }

    qt_sql """
        select try_cast('{123:456}' as json);
    """


    test {
        sql """select cast(12345 as tinyint);"""
        exception "Value 12345 out of range for type tinyint"
    }

    qt_sql """
        select try_cast(12345 as tinyint);
    """


     test {
        sql """select cast(array(1) as map<int,int>);"""
        exception "can not cast from origin type ARRAY<TINYINT> to target type=MAP<INT,INT>"
    }


     test {
        sql """select try_cast(array(1) as map<int,int>);"""
        exception "can not cast from origin type ARRAY<TINYINT> to target type=MAP<INT,INT>"
    }

}