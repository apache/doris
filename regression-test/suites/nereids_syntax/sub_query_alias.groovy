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

suite("sub_query_alias") {

    sql """
        use regression_test_nereids_syntax
    """

    sql """
        SET enable_vectorized_engine=true
    """

    sql """
        SET enable_nereids_planner=true
    """

    sql """
        SELECT * FROM customer c join lineorder l on c.c_custkey = l.lo_custkey
    """

    sql """
        select * from (select l.lo_tax, l.lo_custkey from lineorder l) l1
    """

    sql """
        select * from customer c, lineorder l where c.c_custkey = l.lo_custkey
    """

    sql """
        SELECT * FROM customer c JOIN (SELECT * FROM lineorder l) l2 ON c.c_custkey = l2.lo_custkey
    """
}