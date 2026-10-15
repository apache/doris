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

suite("test_condition_cache_complex_literal") {
    sql "set enable_sql_cache=false"
    sql "drop table if exists condition_cache_complex_literal"
    sql """
        create table condition_cache_complex_literal (id int)
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num"="1")
    """
    sql "insert into condition_cache_complex_literal values (1), (2)"

    def queries = [
        """
            select id from condition_cache_complex_literal
            where bitmap_count(if(id=1, to_bitmap(1), cast(null as bitmap))) > 0
            order by id
        """,
        """
            select id from condition_cache_complex_literal
            where bitmap_count(case when id=1 then to_bitmap(1)
                              else cast(null as bitmap) end) > 0
            order by id
        """,
        """
            select id from condition_cache_complex_literal
            where md5sum(X'61', to_binary(cast(id as string))) <> ''
            order by id
        """,
        // FE folds this array into a literal, while the row-dependent index
        // keeps it in the scan predicate. Even its NULL bitmap element lacks xxHash.
        """
            select id from condition_cache_complex_literal
            where bitmap_count(element_at(array(cast(null as bitmap)), id)) = 0
            order by id
        """,
        """
            select id from condition_cache_complex_literal
            where element_at(array(1, 2), id) = id
            order by id
        """,
        """
            select id, bitmap_count(if(id=1, to_bitmap(1), cast(null as bitmap)))
            from condition_cache_complex_literal order by id
        """
    ]

    // Compare each query with uncached execution to verify that cache transitions
    // and repeated enabled execution preserve the row results.
    sql "set enable_condition_cache=false"
    def uncachedResults = queries.collect { query -> sql(query) }
    for (boolean enabled : [true, true, false, true]) {
        sql "set enable_condition_cache=${enabled}"
        queries.eachWithIndex { query, index ->
            assertEquals(uncachedResults[index], sql(query))
        }
    }
}
