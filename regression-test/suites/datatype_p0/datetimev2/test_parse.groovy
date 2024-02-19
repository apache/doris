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

suite("test_datetimev2_parse") {

    def table1 = "test_datetimev2_parse"

    sql "drop table if exists ${table1}"

    sql """
    CREATE TABLE ${table1} (
        `Id` varchar(108) NOT NULL,
        `AccountingSubjectCode` varchar(765) NULL,
        `AccountingVoucherId` varchar(108) NOT NULL,
        `Date` datetime NOT NULL DEFAULT "0001-01-01 00:00:00.000000"
        ) ENGINE=OLAP
        UNIQUE KEY(`Id`)
        DISTRIBUTED BY HASH(`Id`) BUCKETS AUTO
        PROPERTIES ("replication_num"="1");
        """
        
    sql """
    insert into ${table1} values("78f08ea9-bb45-7bbb-f128-3a108f747237", "5da4c6d1-35da-0594-5d3f-3a108f747234", "6001", "2024-02-06 07:17:41");
    """
    
    // max digit number of microseconds is 9
    qt_select """
    select * from ${table1} where  Date = '2024-02-06 07:17:41.000000000';
    """

    // use T as seperator
    qt_select """
    select * from ${table1} where  Date = '2024-02-06T07:17:41.000000000';
    """

    // 10 digits, throw exception
    test {
        sql """
        select * from ${table1} where Date = '2024-02-06T07:17:41.0000000000';
        """
        
        exception "Text '2024-02-06T07:17:41.0000000000' could not be parsed, unparsed text found at index 29"
    }
    
}
