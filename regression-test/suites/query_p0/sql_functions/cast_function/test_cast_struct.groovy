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

suite("test_cast_struct") {
    // cast NULL to struct type
    qt_sql1 "select cast(NULL as struct<f1:string,f2:int>)"

    // string with invalid struct literal format cast to struct type
    qt_sql2 "select cast('' as struct<f1:char>)"
    qt_sql3 "select cast(cast('' as char) as struct<f1:char>)"
    qt_sql4 "select cast(cast('x' as char) as struct<f1:char>)"
    qt_sql5 "select cast(cast('x' as string) as struct<f1:string>)"

    // valid string format cast to struct type
    qt_sql6 "select cast('{}' as struct<f1:int>)"
    qt_sql7 "select cast('{1,2}' as struct<f1:int,f2:string>)"
    qt_sql8 """select cast('{"a", "b"}' as struct<f1:int,f2:int>)"""
    qt_sql9 """select cast('{"a", "b"}' as struct<f1:string,f2:string>)"""

    // struct literal cast to struct
    qt_sql10 """select cast({1,2} as struct<f1:int,f2:string>)"""
    qt_sql11 """select cast({'1','2'} as struct<f1:int,f2:string>)"""
    qt_sql12 """select cast({"1","2"} as struct<f1:int,f2:string>)"""
    qt_sql13 """select cast({1,'2022-10-10'} as struct<f1:int,f2:date>)"""

    // struct type cast to struct
    qt_sql14 "select cast(cast({1,'2022-10-10'} as struct<f1:int,f2:date>) as struct<f1:double,f2:datetime>)"

    // struct type cast to struct with different field name
    qt_sql15 """select cast('{"a":1,"b":"1","c":"1","d":"1"}' as struct<a:int, b:int>)"""

    // basic types except string can not cast to struct 
    test {
        sql "select cast(cast(1 as int) as struct<f1:int>)"
        exception "errCode = 2,"
    }
    test {
        sql "select cast(cast(999.999 as double) as struct<f1:double>)"
        exception "errCode = 2,"
    }

    // struct literal can not cast to basic types
    test {
        sql "select cast({1,2} as string)"
        exception "errCode = 2,"
    }

    // struct literal cast to struct MUST with same field number
    test {
        sql "select cast({1,2} as struct<f1:int,f2:string,f3:string>)"
        exception "errCode = 2,"
    }
    test {
        sql "select cast({1,2} as struct<f1:int>)"
        exception "errCode = 2,"
    }
    
    // struct type cast to struct MUST with same field number
    test {
        sql "select cast(cast({1,'2022-10-10'} as struct<f1:int,f2:date>) as struct<f1:double,f2:datetime,f3:string>)"
        exception "errCode = 2,"
    }
    test {
        sql "select cast(cast({1,'2022-10-10'} as struct<f1:int,f2:date>) as struct<f1:double>)"
        exception "errCode = 2,"
    }
}
