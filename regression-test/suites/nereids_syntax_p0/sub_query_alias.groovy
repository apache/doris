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
        SET enable_nereids_planner=true
    """

    sql "SET enable_fallback_to_original_planner=false"

    test {
        sql """ select * 
        from (
            select * 
            from customer, lineorder 
            where customer.c_custkey = lineorder.lo_custkey
        )  """
        check {result, exception, startTime, endTime ->
            assertTrue(exception != null)
            logger.info(exception.message)
        }
    }

    qt_select_1 """
        select t.c_custkey, t.lo_custkey 
        from (
            select * 
            from customer, lineorder 
            where customer.c_custkey = lineorder.lo_custkey
        ) t
        order by t.c_custkey
    """

    qt_select_2 """
        select c.c_custkey, l.lo_custkey 
        from customer c, lineorder l 
        where c.c_custkey = l.lo_custkey
        order by c.c_custkey
    """

    qt_select_3 """
        select t.c_custkey, t.lo_custkey 
        from (
            select * 
            from customer c, lineorder l 
            where c.c_custkey = l.lo_custkey
        ) t
        order by t.c_custkey
    """

    qt_select_4 """
        select * 
        from customer c 
        join customer c1 
        on c.c_custkey = c1.c_custkey
        order by c.c_custkey
    """

    qt_select_5 """
        select * 
        from customer c 
        join (
            select * 
            from lineorder l
        ) t on c.c_custkey = t.lo_custkey
        order by c.c_custkey,lo_tax
    """
}

