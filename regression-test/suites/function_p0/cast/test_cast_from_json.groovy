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

suite("test_cast_from_json") {
    sql "set debug_skip_fold_constant=true;"
    
    sql "set enable_strict_cast=false;"
    
    qt_cast_from_json_bool """
    SELECT 
        CAST(to_json(true) as BOOLEAN), 
        CAST(to_json(true) as TINYINT),
        CAST(to_json(true) as SMALLINT),
        CAST(to_json(true) as INT) ,
        CAST(to_json(true) as BIGINT), 
        CAST(to_json(true) as LARGEINT), 
        CAST(to_json(true) as DOUBLE) ,
        CAST(to_json(true) as FLOAT) ,
        CAST(to_json(true) as DECIMAL), 
        CAST(to_json(true) as STRING),
        CAST(to_json(true) as ARRAY<BOOLEAN>),
        CAST(to_json(true) as STRUCT<s_id:int>);
    """

    qt_cast_from_json_bool """
    SELECT 
        CAST(to_json(false) as BOOLEAN), 
        CAST(to_json(false) as TINYINT),
        CAST(to_json(false) as SMALLINT),
        CAST(to_json(false) as INT) ,
        CAST(to_json(false) as BIGINT), 
        CAST(to_json(false) as LARGEINT), 
        CAST(to_json(false) as DOUBLE) ,
        CAST(to_json(false) as FLOAT) ,
        CAST(to_json(false) as DECIMAL), 
        CAST(to_json(false) as STRING),
        CAST(to_json(false) as ARRAY<BOOLEAN>),
        CAST(to_json(false) as STRUCT<s_id:int>);
    """

    qt_cast_from_json_int8 """
    SELECT 
        CAST(to_json(123) as BOOLEAN), 
        CAST(to_json(123) as TINYINT),
        CAST(to_json(123) as SMALLINT),
        CAST(to_json(123) as INT) ,
        CAST(to_json(123) as BIGINT), 
        CAST(to_json(123) as LARGEINT), 
        CAST(to_json(123) as DOUBLE) ,
        CAST(to_json(123) as FLOAT) ,
        CAST(to_json(123) as DECIMAL), 
        CAST(to_json(123) as STRING),
        CAST(to_json(123) as ARRAY<BOOLEAN>),
        CAST(to_json(123) as STRUCT<s_id:int>);
    """


    qt_cast_from_json_int16 """
    SELECT 
        CAST(to_json(3276) as BOOLEAN), 
        CAST(to_json(3276) as TINYINT),
        CAST(to_json(3276) as SMALLINT),
        CAST(to_json(3276) as INT) ,
        CAST(to_json(3276) as BIGINT), 
        CAST(to_json(3276) as LARGEINT), 
        CAST(to_json(3276) as DOUBLE) ,
        CAST(to_json(3276) as FLOAT) ,
        CAST(to_json(3276) as DECIMAL), 
        CAST(to_json(3276) as STRING),
        CAST(to_json(3276) as ARRAY<BOOLEAN>),
        CAST(to_json(3276) as STRUCT<s_id:int>);
    """


    qt_cast_from_json_int32 """
    SELECT 
        CAST(to_json(147483647) as BOOLEAN), 
        CAST(to_json(147483647) as TINYINT),
        CAST(to_json(147483647) as SMALLINT),
        CAST(to_json(147483647) as INT) ,
        CAST(to_json(147483647) as BIGINT), 
        CAST(to_json(147483647) as LARGEINT), 
        CAST(to_json(147483647) as DOUBLE) ,
        CAST(to_json(147483647) as FLOAT) ,
        CAST(to_json(147483647) as DECIMAL), 
        CAST(to_json(147483647) as STRING),
        CAST(to_json(147483647) as ARRAY<BOOLEAN>),
        CAST(to_json(147483647) as STRUCT<s_id:int>);
    """


    qt_cast_from_json_int64 """
    SELECT 
        CAST(to_json(372036854775807) as BOOLEAN), 
        CAST(to_json(372036854775807) as TINYINT),
        CAST(to_json(372036854775807) as SMALLINT),
        CAST(to_json(372036854775807) as INT) ,
        CAST(to_json(372036854775807) as BIGINT), 
        CAST(to_json(372036854775807) as LARGEINT), 
        CAST(to_json(372036854775807) as DOUBLE) ,
        CAST(to_json(372036854775807) as FLOAT) ,
        CAST(to_json(372036854775807) as DECIMAL), 
        CAST(to_json(372036854775807) as STRING),
        CAST(to_json(372036854775807) as ARRAY<BOOLEAN>),
        CAST(to_json(372036854775807) as STRUCT<s_id:int>);
    """


    qt_cast_from_json_int128 """
    SELECT 
        CAST(to_json(170141183460469231731687303715884105) as BOOLEAN), 
        CAST(to_json(170141183460469231731687303715884105) as TINYINT),
        CAST(to_json(170141183460469231731687303715884105) as SMALLINT),
        CAST(to_json(170141183460469231731687303715884105) as INT) ,
        CAST(to_json(170141183460469231731687303715884105) as BIGINT), 
        CAST(to_json(170141183460469231731687303715884105) as LARGEINT), 
        CAST(to_json(170141183460469231731687303715884105) as DOUBLE) ,
        CAST(to_json(170141183460469231731687303715884105) as FLOAT) ,
        CAST(to_json(170141183460469231731687303715884105) as DECIMAL), 
        CAST(to_json(170141183460469231731687303715884105) as STRING),
        CAST(to_json(170141183460469231731687303715884105) as ARRAY<BOOLEAN>),
        CAST(to_json(170141183460469231731687303715884105) as STRUCT<s_id:int>);
    """


    qt_cast_from_json_array_int """
    SELECT 
        CAST(to_json(array(1,2,3,4,5)) as BOOLEAN), 
        CAST(to_json(array(1,2,3,4,5)) as TINYINT),
        CAST(to_json(array(1,2,3,4,5)) as SMALLINT),
        CAST(to_json(array(1,2,3,4,5)) as INT) ,
        CAST(to_json(array(1,2,3,4,5)) as BIGINT), 
        CAST(to_json(array(1,2,3,4,5)) as LARGEINT), 
        CAST(to_json(array(1,2,3,4,5)) as DOUBLE) ,
        CAST(to_json(array(1,2,3,4,5)) as FLOAT) ,
        CAST(to_json(array(1,2,3,4,5)) as DECIMAL), 
        CAST(to_json(array(1,2,3,4,5)) as STRING),
        CAST(to_json(array(1,2,3,4,5)) as ARRAY<BOOLEAN>),
        CAST(to_json(array(1,2,3,4,5)) as STRUCT<s_id:int>);
    """


    qt_cast_from_json_array_str """
    SELECT 
        CAST(to_json(array("123","456")) as BOOLEAN), 
        CAST(to_json(array("123","456")) as TINYINT),
        CAST(to_json(array("123","456")) as SMALLINT),
        CAST(to_json(array("123","456")) as INT) ,
        CAST(to_json(array("123","456")) as BIGINT), 
        CAST(to_json(array("123","456")) as LARGEINT), 
        CAST(to_json(array("123","456")) as DOUBLE) ,
        CAST(to_json(array("123","456")) as FLOAT) ,
        CAST(to_json(array("123","456")) as DECIMAL), 
        CAST(to_json(array("123","456")) as STRING),
        CAST(to_json(array("123","456")) as ARRAY<INT>),
        CAST(to_json(array("123","456")) as STRUCT<s_id:int>);
    """


    qt_cast_from_json_object """
    SELECT 
        CAST(to_json(named_struct("s_id" , 114514)) as BOOLEAN), 
        CAST(to_json(named_struct("s_id" , 114514)) as TINYINT),
        CAST(to_json(named_struct("s_id" , 114514)) as SMALLINT),
        CAST(to_json(named_struct("s_id" , 114514)) as INT) ,
        CAST(to_json(named_struct("s_id" , 114514)) as BIGINT), 
        CAST(to_json(named_struct("s_id" , 114514)) as LARGEINT), 
        CAST(to_json(named_struct("s_id" , 114514)) as DOUBLE) ,
        CAST(to_json(named_struct("s_id" , 114514)) as FLOAT) ,
        CAST(to_json(named_struct("s_id" , 114514)) as DECIMAL), 
        CAST(to_json(named_struct("s_id" , 114514)) as STRING),
        CAST(to_json(named_struct("s_id" , 114514)) as ARRAY<INT>),
        CAST(to_json(named_struct("s_id" , 114514)) as STRUCT<s_id:int>);
    """


    qt_cast_from_json_null """
    SELECT 
        CAST(cast('null' as json) as BOOLEAN), 
        CAST(cast('null' as json) as TINYINT),
        CAST(cast('null' as json) as SMALLINT),
        CAST(cast('null' as json) as INT) ,
        CAST(cast('null' as json) as BIGINT), 
        CAST(cast('null' as json) as LARGEINT), 
        CAST(cast('null' as json) as DOUBLE) ,
        CAST(cast('null' as json) as FLOAT) ,
        CAST(cast('null' as json) as DECIMAL), 
        CAST(cast('null' as json) as STRING),
        CAST(cast('null' as json) as ARRAY<INT>),
        CAST(cast('null' as json) as STRUCT<s_id:int>);
    """


    qt_cast_from_json """
    SELECT 
        CAST(cast('{"key1" : [123.45,678.90] , "key2" : [12312313]}' as json) as STRUCT<key1:ARRAY<DOUBLE>, key2:ARRAY<BIGINT>>);
    """



    sql "set enable_strict_cast=true;"



    qt_cast_from_json_bool """
    SELECT 
        CAST(to_json(true) as BOOLEAN), 
        CAST(to_json(true) as TINYINT),
        CAST(to_json(true) as SMALLINT),
        CAST(to_json(true) as INT) ,
        CAST(to_json(true) as BIGINT), 
        CAST(to_json(true) as LARGEINT), 
        CAST(to_json(true) as DOUBLE) ,
        CAST(to_json(true) as FLOAT) ,
        CAST(to_json(true) as DECIMAL), 
        CAST(to_json(true) as STRING);
    """

    test {
        sql """
        SELECT CAST(to_json(true) as ARRAY<BOOLEAN>);
        """
        exception "INVALID_ARGUMENT"
    }

    test {
        sql """
        SELECT CAST(to_json(true) as STRUCT<s_id:int>);
        """
        exception "INVALID_ARGUMENT"
    }

    qt_cast_from_json_bool """
    SELECT 
        CAST(to_json(false) as BOOLEAN), 
        CAST(to_json(false) as TINYINT),
        CAST(to_json(false) as SMALLINT),
        CAST(to_json(false) as INT) ,
        CAST(to_json(false) as BIGINT), 
        CAST(to_json(false) as LARGEINT), 
        CAST(to_json(false) as DOUBLE) ,
        CAST(to_json(false) as FLOAT) ,
        CAST(to_json(false) as DECIMAL), 
        CAST(to_json(false) as STRING);
    """

    test {
        sql """
        SELECT CAST(to_json(false) as ARRAY<BOOLEAN>);
        """
        exception "INVALID_ARGUMENT"
    }

    test {
        sql """
        SELECT CAST(to_json(false) as STRUCT<s_id:int>);
        """
        exception "INVALID_ARGUMENT"
    }

    qt_cast_from_json_int8 """
    SELECT 
        CAST(to_json(123) as BOOLEAN), 
        CAST(to_json(123) as TINYINT),
        CAST(to_json(123) as SMALLINT),
        CAST(to_json(123) as INT) ,
        CAST(to_json(123) as BIGINT), 
        CAST(to_json(123) as LARGEINT), 
        CAST(to_json(123) as DOUBLE) ,
        CAST(to_json(123) as FLOAT) ,
        CAST(to_json(123) as DECIMAL), 
        CAST(to_json(123) as STRING);
    """

    test {
        sql """
        SELECT CAST(to_json(123) as ARRAY<BOOLEAN>);
        """
        exception "INVALID_ARGUMENT"
    }

    test {
        sql """
        SELECT CAST(to_json(123) as STRUCT<s_id:int>);
        """
        exception "INVALID_ARGUMENT"
    }

    qt_cast_from_json_int16 """
    SELECT 
        CAST(to_json(3276) as BOOLEAN), 
        CAST(to_json(3276) as SMALLINT),
        CAST(to_json(3276) as INT) ,
        CAST(to_json(3276) as BIGINT), 
        CAST(to_json(3276) as LARGEINT), 
        CAST(to_json(3276) as DOUBLE) ,
        CAST(to_json(3276) as FLOAT) ,
        CAST(to_json(3276) as DECIMAL), 
        CAST(to_json(3276) as STRING);
    """

    test {
        sql """
        SELECT CAST(to_json(3276) as TINYINT);
        """
        exception "INVALID_ARGUMENT"
    }

    test {
        sql """
        SELECT CAST(to_json(3276) as ARRAY<BOOLEAN>);
        """
        exception "INVALID_ARGUMENT"
    }

    test {
        sql """
        SELECT CAST(to_json(3276) as STRUCT<s_id:int>);
        """
        exception "INVALID_ARGUMENT"
    }

    qt_cast_from_json_int32 """
    SELECT 
        CAST(to_json(147483647) as BOOLEAN), 
        CAST(to_json(147483647) as INT),
        CAST(to_json(147483647) as BIGINT), 
        CAST(to_json(147483647) as LARGEINT), 
        CAST(to_json(147483647) as DOUBLE) ,
        CAST(to_json(147483647) as FLOAT) ,
        CAST(to_json(147483647) as DECIMAL), 
        CAST(to_json(147483647) as STRING);
    """

    test {
        sql """
        SELECT CAST(to_json(147483647) as TINYINT);
        """
        exception "INVALID_ARGUMENT"
    }

    test {
        sql """
        SELECT CAST(to_json(147483647) as SMALLINT);
        """
        exception "INVALID_ARGUMENT"
    }

    test {
        sql """
        SELECT CAST(to_json(147483647) as ARRAY<BOOLEAN>);
        """
        exception "INVALID_ARGUMENT"
    }

    test {
        sql """
        SELECT CAST(to_json(147483647) as STRUCT<s_id:int>);
        """
        exception "INVALID_ARGUMENT"
    }

    qt_cast_from_json_int64 """
    SELECT 
        CAST(to_json(372036854775807) as BOOLEAN), 
        CAST(to_json(372036854775807) as BIGINT), 
        CAST(to_json(372036854775807) as LARGEINT), 
        CAST(to_json(372036854775807) as DOUBLE) ,
        CAST(to_json(372036854775807) as FLOAT) ,
        CAST(to_json(372036854775807) as DECIMAL), 
        CAST(to_json(372036854775807) as STRING);
    """

    test {
        sql """
        SELECT CAST(to_json(372036854775807) as TINYINT);
        """
        exception "INVALID_ARGUMENT"
    }

    test {
        sql """
        SELECT CAST(to_json(372036854775807) as SMALLINT);
        """
        exception "INVALID_ARGUMENT"
    }

    test {
        sql """
        SELECT CAST(to_json(372036854775807) as INT);
        """
        exception "INVALID_ARGUMENT"
    }

    test {
        sql """
        SELECT CAST(to_json(372036854775807) as ARRAY<BOOLEAN>);
        """
        exception "INVALID_ARGUMENT"
    }

    test {
        sql """
        SELECT CAST(to_json(372036854775807) as STRUCT<s_id:int>);
        """
        exception "INVALID_ARGUMENT"
    }

    qt_cast_from_json_int128 """
    SELECT 
        CAST(to_json(170141183460469231731687303715884105) as BOOLEAN), 
        CAST(to_json(170141183460469231731687303715884105) as LARGEINT), 
        CAST(to_json(170141183460469231731687303715884105) as DOUBLE) ,
        CAST(to_json(170141183460469231731687303715884105) as FLOAT) ,
        CAST(to_json(170141183460469231731687303715884105) as STRING);
    """

    test {
        sql """
        SELECT CAST(to_json(170141183460469231731687303715884105) as TINYINT);
        """
        exception "INVALID_ARGUMENT"
    }

    test {
        sql """
        SELECT CAST(to_json(170141183460469231731687303715884105) as SMALLINT);
        """
        exception "INVALID_ARGUMENT"
    }

    test {
        sql """
        SELECT CAST(to_json(170141183460469231731687303715884105) as INT);
        """
        exception "INVALID_ARGUMENT"
    }

    test {
        sql """
        SELECT CAST(to_json(170141183460469231731687303715884105) as BIGINT);
        """
        exception "INVALID_ARGUMENT"
    }

    test {
        sql """
        SELECT CAST(to_json(170141183460469231731687303715884105) as ARRAY<BOOLEAN>);
        """
        exception "INVALID_ARGUMENT"
    }

    test {
        sql """
        SELECT CAST(to_json(170141183460469231731687303715884105) as STRUCT<s_id:int>);
        """
        exception "INVALID_ARGUMENT"
    }

    test {
        sql """
        SELECT 
            CAST(to_json(array(1,2,3,4,5)) as BOOLEAN), 
            CAST(to_json(array(1,2,3,4,5)) as TINYINT),
            CAST(to_json(array(1,2,3,4,5)) as SMALLINT),
            CAST(to_json(array(1,2,3,4,5)) as INT),
            CAST(to_json(array(1,2,3,4,5)) as BIGINT),
            CAST(to_json(array(1,2,3,4,5)) as LARGEINT),
            CAST(to_json(array(1,2,3,4,5)) as DOUBLE),
            CAST(to_json(array(1,2,3,4,5)) as FLOAT),
            CAST(to_json(array(1,2,3,4,5)) as DECIMAL),
            CAST(to_json(array(1,2,3,4,5)) as STRING);
        """
        exception "INVALID_ARGUMENT"
    }

    qt_cast_from_json_array_int """
    SELECT 
        CAST(to_json(array(1,2,3,4,5)) as ARRAY<INT>);
    """

    test {
        sql """
        SELECT CAST(to_json(array(1,2,3,4,5)) as STRUCT<s_id:int>);
        """
        exception "INVALID_ARGUMENT"
    }

    test {
        sql """
        SELECT 
            CAST(to_json(array("123","456")) as BOOLEAN), 
            CAST(to_json(array("123","456")) as TINYINT),
            CAST(to_json(array("123","456")) as SMALLINT),
            CAST(to_json(array("123","456")) as INT),
            CAST(to_json(array("123","456")) as BIGINT),
            CAST(to_json(array("123","456")) as LARGEINT),
            CAST(to_json(array("123","456")) as DOUBLE),
            CAST(to_json(array("123","456")) as FLOAT),
            CAST(to_json(array("123","456")) as DECIMAL),
            CAST(to_json(array("123","456")) as STRING);
        """
        exception "INVALID_ARGUMENT"
    }

    qt_cast_from_json_array_str """
    SELECT 
        CAST(to_json(array("123","456")) as ARRAY<STRING>),
         CAST(to_json(array("123","456")) as ARRAY<INT>);
    """

    test {
        sql """
        SELECT CAST(to_json(array("123","456")) as STRUCT<s_id:int>);
        """
        exception "INVALID_ARGUMENT"
    }

    test {
        sql """
        SELECT 
            CAST(to_json(named_struct("s_id" , 114514)) as BOOLEAN), 
            CAST(to_json(named_struct("s_id" , 114514)) as TINYINT),
            CAST(to_json(named_struct("s_id" , 114514)) as SMALLINT),
            CAST(to_json(named_struct("s_id" , 114514)) as INT),
            CAST(to_json(named_struct("s_id" , 114514)) as BIGINT),
            CAST(to_json(named_struct("s_id" , 114514)) as LARGEINT),
            CAST(to_json(named_struct("s_id" , 114514)) as DOUBLE),
            CAST(to_json(named_struct("s_id" , 114514)) as FLOAT),
            CAST(to_json(named_struct("s_id" , 114514)) as DECIMAL),
            CAST(to_json(named_struct("s_id" , 114514)) as STRING);
        """
        exception "INVALID_ARGUMENT"
    }

    test {
        sql """
        SELECT CAST(to_json(named_struct("s_id" , 114514)) as ARRAY<INT>);
        """
        exception "INVALID_ARGUMENT"
    }

    qt_cast_from_json_object """
    SELECT 
        CAST(to_json(named_struct("s_id" , 114514)) as STRUCT<s_id:int>);
    """

    qt_cast_from_json_null """
    SELECT 
        CAST(cast('null' as json) as BOOLEAN), 
        CAST(cast('null' as json) as TINYINT),
        CAST(cast('null' as json) as SMALLINT),
        CAST(cast('null' as json) as INT) ,
        CAST(cast('null' as json) as BIGINT), 
        CAST(cast('null' as json) as LARGEINT), 
        CAST(cast('null' as json) as DOUBLE) ,
        CAST(cast('null' as json) as FLOAT) ,
        CAST(cast('null' as json) as DECIMAL), 
        CAST(cast('null' as json) as STRING),
        CAST(cast('null' as json) as ARRAY<INT>),
        CAST(cast('null' as json) as STRUCT<s_id:int>);
    """

    qt_cast_from_json """
    SELECT 
        CAST(cast('{"key1" : [123.45,678.90] , "key2" : [12312313]}' as json) as STRUCT<key1:ARRAY<DOUBLE>, key2:ARRAY<BIGINT>>);
    """

    test {
        sql """
        SELECT CAST(cast('{"key1" : [123.45,678.90] , "key2" : [12312313]}' as json) as STRUCT<key1:ARRAY<INT>>);
        """
        exception "INVALID_ARGUMENT"
    }

    test {
        sql """
        SELECT CAST(cast('{"key1" : [123.45,678.90] , "key3" : [12312313]}' as json) as STRUCT<key1:ARRAY<DOUBLE>, key2:ARRAY<BIGINT>>);
        """
        exception "INVALID_ARGUMENT"
    }

}
