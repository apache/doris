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

suite("view") {
    sql """
        SET enable_vectorized_engine=true
    """

    sql """
        SET enable_nereids_planner=true
    """

    create_view_1 """
        create view if not exists v1 as select c_custkey, c_name, c_address, c_city from customer
    """

    create_view_2 """
        create view if not exists v2 as select lo_custkey, lo_orderkey, lo_linenumber from lineorder
    """

    create_view_3 """
        create view if not exists v3 as select v1.c_custkey, t.lo_custkey from v1 join (select lo_custkey from v2) t on v1.c_custkey = t.lo_custkey;
    """

    qt_view """
    """
}