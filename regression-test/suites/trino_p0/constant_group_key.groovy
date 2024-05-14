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

// This suit test remove constant group_by_key 
suite("constant_group_key") {
    //remove constant key
    explain {
        sql("select 'oneline' from nation group by n_nationkey, 'constant1'")
        contains "group by: n_nationkey"
    }

    //reserve constant key in group by
    explain {
        sql("select 'oneline' from nation group by 'constant1'")
        contains "group by: 'constant1'"
    }

    explain {
        sql("select 'oneline', sum(n_nationkey) from nation group by 'constant1', 'constant2'")
        contains "group by: 'constant2'"
    }

    sql "drop table if exists cgk_tbl"
    sql """
    create table cgk_tbl (a int null) 
    engine=olap duplicate key(a) 
    distributed by hash(a) buckets 1 
    properties ('replication_num'='1');"""

    qt_scalar_count 'select count(*) from cgk_tbl'
    qt_agg_count "select count(*) from cgk_tbl group by 'any const str'"
    qt_scalar_sum 'select sum(a) from cgk_tbl'
    qt_agg_sum "select sum(a) from cgk_tbl group by 'any const str'"
}
