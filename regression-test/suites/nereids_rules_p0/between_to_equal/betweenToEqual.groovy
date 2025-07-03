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

suite("between_to_equal") {
    sql """
    create table datebetween (
    guid int,
    dt DATE,
    first_visit_time varchar
     )Engine=Olap
     DUPLICATE KEY(guid)
     distributed by hash(dt) buckets 3
    properties('replication_num' = '1');
    
    insert into datebetween values (1, '2021-01-01', 'abc');
    """
    explain {
        sql " select * from datebetween where dt between '2021-01-01' and '2021-01-01 11:11:11';"
        contains("PREDICATES: (dt[#1] = '2021-01-01')");
    }

    explain {
        sql " select * from datebetween where dt between '2021-01-01' and '2021-01-01 11:11:11' and dt < '2024-12-01';"
        contains("PREDICATES: (dt[#1] = '2021-01-01')")
    }

    explain {
        sql "select * from datebetween where dt between '2021-01-01' and '2021-01-01 11:11:11' and dt < '2024-12-01' or guid =1;"
        contains("PREDICATES: ((dt[#1] = '2021-01-01') OR (guid[#0] = 1))")
    }

}
