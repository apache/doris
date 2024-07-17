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

suite("test_set_operation_adjust_nullable") {

    sql """
        DROP TABLE IF EXISTS t1
    """
    sql """
        DROP TABLE IF EXISTS t2
    """

    sql """
        CREATE TABLE t1(c1 varchar) DISTRIBUTED BY hash(c1) PROPERTIES ("replication_num" = "1");
    """

    sql """
        CREATE TABLE t2(c2 date) DISTRIBUTED BY hash(c2) PROPERTIES ("replication_num" = "1");
    """

    sql """
        insert into t1 values('+06-00');
    """

    sql """
        insert into t2 values('1990-11-11');
    """

    sql """
        SELECT c1, c1 FROM t1 MINUS SELECT c2, c2 FROM t2;
    """
}
