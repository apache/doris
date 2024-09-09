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

suite("delta_row") {
    String database = context.config.getDbNameByFile(context.file)
    sql """
        drop database if exists ${database};
        create database ${database};
        use ${database};
        CREATE TABLE IF NOT EXISTS t (
            k int(11) null comment "",
            v string replace null comment "",
        ) engine=olap
        DISTRIBUTED BY HASH(k) BUCKETS 5 properties("replication_num" = "1");
    
        insert into t values (1, "a"),(2, "b"),(3, 'c'),(4,'d');
        analyze table t with sync;
    """
    explain {
        sql "physical plan select * from t where k > 6"
        contains("stats=0,")
        contains("stats=4 ")
        // PhysicalResultSink[75] ( outputExprs=[k#0, v#1] )
        //     +--PhysicalFilter[72]@1 ( stats=0, predicates=(k#0 > 6) )
        //         +--PhysicalOlapScan[t]@0 ( stats=4 )
    }

    sql "set global enable_auto_analyze=false;"

    sql "insert into t values (10, 'c');"
    explain {
        sql "physical plan select * from t where k > 6"
        contains("stats=0.5,")
        contains("stats=5(1)")
        notContains("stats=0,")
        notContains("stats=4 ")
// PhysicalResultSink[75] ( outputExprs=[k#0, v#1] )
// +--PhysicalFilter[72]@1 ( stats=0.5, predicates=(k#0 > 6) )
//    +--PhysicalOlapScan[t]@0 ( stats=5(1) )
    }
}