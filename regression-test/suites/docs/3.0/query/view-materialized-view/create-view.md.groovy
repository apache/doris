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

import org.junit.jupiter.api.Assertions;

suite("docs/3.0/query/view-materialized-view/create-view.md") {
    try {
        multi_sql """
        create database demo;
        create table demo.example_table(
            k1  int,
            k2  int,
            k3  int,
            v1  int
        );
        CREATE VIEW demo.example_view (k1, k2, k3, v1)
        AS
        SELECT k1, k2, k3, SUM(v1) FROM demo.example_table
        WHERE k1 = 20160112 GROUP BY k1,k2,k3;
        
        select * from demo.example_view;
        """

    } catch (Throwable t) {
        Assertions.fail("examples in docs/3.0/query/view-materialized-view/create-view.md failed to exec, please fix it", t)
    }
}
