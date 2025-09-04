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


suite("fold_match_be") {
    sql """
    drop table if exists t1;
    create table t1 (k int, v varchar) properties('replication_num'='1');
    insert into t1 values (1, 'a');
    drop table if exists t2;
    create table t2 (k int, v varchar) properties('replication_num'='1');
    insert into t2 values (1, 'got really out');
    
    set disable_join_reorder=true;
    set enable_fold_constant_by_be=true;
    """

    // null MATCH_REGEXP 'got really out' 
    //    => null (by const fold)
    //    => t2.v is not null (infer predicate)
    //    => outer join eliminated
    //    => t2.v  MATCH_REGEXP 'got really out' can be pushed down to storage layer
    qt_exe """
    select * from t1 left join t2 on t1.k=t2.k where t2.v MATCH_REGEXP 'got really out';
    """

}