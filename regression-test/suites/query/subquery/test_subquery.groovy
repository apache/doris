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

suite("test_subquery", "query") {
        qt_sql1 """
            select c1, c3, m2 from 
                (select c1, c3, max(c2) m2 from 
                    (select c1, c2, c3 from 
                        (select k3 c1, k2 c2, max(k1) c3 from test_query_db.test 
                         group by 1, 2 order by 1 desc, 2 desc limit 5) x 
                    ) x2 group by c1, c3 limit 10
                ) t 
            where c1>0 order by 2 , 1 limit 3
            """
}
