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

suite("or_expansion") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    def db = "nereids_test_query_db"
    sql "use ${db}"

    explain {
        sql("""select * from bigtable 
            join baseall 
            on baseall.k0 = bigtable.k0 
            or baseall.k1 = bigtable.k1""")
        contains "VHASH JOIN"
    }
    
    order_qt_nlj """select * from bigtable 
            join baseall 
            on baseall.k0 = bigtable.k0 
            or baseall.k1 = bigtable.k1
            """

    explain {
        sql("""select * from bigtable 
            join baseall 
            on baseall.k0 = bigtable.k0
            or baseall.k1 * 2 = bigtable.k1 + 1""")
        contains "VHASH JOIN"
    }

    order_qt_nlj2 """select * from bigtable 
            join baseall 
            on baseall.k0 = bigtable.k0
            or baseall.k1 * 2 = bigtable.k1 + 1
            """
}
