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

suite("recursive_100_number_test", "rec_cte") {

    def level = 100
    sql """
        WITH recursive RecursiveCounter (
            N,
            Depth
        )
        AS
        (
            SELECT 
                cast(1 as bigint) AS N, 
                cast(1 as bigint) AS Depth
            UNION ALL
            SELECT 
                cast(N + 1 as bigint) AS N, 
                cast(Depth + 1 as bigint) AS Depth 
            FROM 
                RecursiveCounter           
            WHERE 
                Depth < ${level}
        )
        SELECT 
            N,
            Depth
        FROM 
            RecursiveCounter
        ORDER BY
            Depth;"""

    level = 101
    test {
        sql """WITH recursive RecursiveCounter (
            N,
            Depth
        )
        AS
        (
            SELECT 
                cast(1 as bigint) AS N, 
                cast(1 as bigint) AS Depth
            UNION ALL
            SELECT 
                cast(N + 1 as bigint) AS N, 
                cast(Depth + 1 as bigint) AS Depth 
            FROM 
                RecursiveCounter           
            WHERE 
                Depth < ${level}
        )
        SELECT 
            N,
            Depth
        FROM 
            RecursiveCounter
        ORDER BY
            Depth;"""
        exception "reach cte_max_recursion_depth 100"
    }

    sql "set cte_max_recursion_depth=101"
    sql """
        WITH recursive RecursiveCounter (
            N,
            Depth
        )
        AS
        (
            SELECT 
                cast(1 as bigint) AS N, 
                cast(1 as bigint) AS Depth
            UNION ALL
            SELECT 
                cast(N + 1 as bigint) AS N, 
                cast(Depth + 1 as bigint) AS Depth 
            FROM 
                RecursiveCounter           
            WHERE 
                Depth < ${level}
        )
        SELECT 
            N,
            Depth
        FROM 
            RecursiveCounter
        ORDER BY
            Depth;"""

}
