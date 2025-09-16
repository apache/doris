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

suite("join_input_tuple_id") {
    sql """
        drop table if exists t1;
        create table t1(k int, v int) properties("replication_num"="1");
        insert into t1 values (1, 1), (2, 2), (3,3);

        drop table if exists t2;
        create table t2(k int, v int) properties("replication_num"="1");
        insert into t2 values (1, 1), (2, 2), (3,3);

        drop table if exists t3;
        create table t3(k int, v int) properties("replication_num"="1");
        insert into t3 values (1, 1), (2, 2), (3,3);
        set disable_join_reorder=true;
    """

    explain {
        sql """
            verbose select * 
            from ((select k, v from t1) union all (select k, v from t2)) as u
                join t3 on u.k+1 = t3.k
        """
        // verify that join's input tuple is union's output tuple id (5) not input tuple (4)
        contains "tuple ids: 5 1N"

//   7:VHASH JOIN(293)
//   |  join op: INNER JOIN(BROADCAST)[]
//   |  equal join conjunct: (expr_cast(k as BIGINT)[#13] = expr_(cast(k as BIGINT) - 1)[#4])
//   |  cardinality=2
//   |  vec output tuple id: 7
//   |  output tuple id: 7
//   |  vIntermediate tuple ids: 6 
//   |  hash output slot ids: 2 3 11 12 
//   |  isMarkJoin: false
//   |  final projections: k[#14], v[#15], k[#17], v[#18]
//   |  final project output tuple id: 7
//   |  distribute expr lists: 
//   |  distribute expr lists: 
//   |  tuple ids: 5 1N 
//   |  
//   |----1:VEXCHANGE
//   |       offset: 0
//   |       distribute expr lists: 
//   |       tuple ids: 1N 
//   |    
//   6:VUNION(276)
//   |  child exprs: 
//   |      k[#5] | v[#6]
//   |      k[#7] | v[#8]
//   |  final projections: k[#9], v[#10], CAST(k[#9] AS bigint)
//   |  final project output tuple id: 5
//   |  tuple ids: 4 
    }
}