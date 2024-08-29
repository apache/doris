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

suite("test_agg_state_type_adjust_nullable") {

    sql """
        DROP TABLE IF EXISTS test_agg_state_type
    """

    sql """
        CREATE TABLE test_agg_state_type(`id` INT NOT NULL, `c1` INT NOT NULL) DISTRIBUTED BY hash(id) PROPERTIES ("replication_num" = "1")
    """

    sql """
        insert into test_agg_state_type values (1, 1)
    """

    sql """
        select * from (select sum_state(c1) as c2, 1 as c1 from (select avg(id) as c1 from test_agg_state_type) v) v join test_agg_state_type on v.c1 = test_agg_state_type.id
    """

    sql """
        DROP TABLE IF EXISTS test_agg_state_type
    """
}
