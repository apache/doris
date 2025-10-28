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


suite("test_json_hash") {
    qt_sql """
        select json_hash(cast('{"b":1234567890123456789,"b":456,"a":789}' as json)) ,   normalize_jsonb_numbers_to_double(cast('{"b":123,"b":456,"a":789}' as json));
    """

    qt_sql """
        select json_hash(cast('[{"b":123,"b":456,"a":1234567890123456789.231231} ,{"b":1213379817829313213},{"b":456123123123123123},{"a":74124123132189} ]' as json));
    """

    qt_sql """
        select json_hash(cast('{"a":123, "b":456}' as json)) , json_hash(cast('{"b":456, "a":123}' as json));
    """

    qt_sql """
        select json_hash(cast('{"a":123}' as json)) , json_hash(cast('{"a":456}' as json)) , json_hash(cast('{"a":123, "a":456}' as json)) ;
    """

    qt_sql """
        select json_hash(to_json(cast('123' as int))) , json_hash(to_json(cast('123' as tinyint)));
    """


    qt_sql """
         select json_hash(to_json(cast('1231231322.23123' as Decimal)) );
    """

    sql "DROP TABLE IF EXISTS test_json_json_hash_table"

    sql """
    CREATE TABLE IF NOT EXISTS test_json_json_hash_table (
        id INT,
        j JSON
    )
    DISTRIBUTED BY HASH(id) BUCKETS 3
    PROPERTIES (
        "replication_num" = "1"
    )
    """

    sql """
    INSERT INTO test_json_json_hash_table VALUES
    (1, '{"b":123,"b":456,"a":789}'),
    (2, '{"d":123,"c":456,"b":789,"a":1011}'),
    (3, '{"x":123,"y":456,"z":789}'),
    (4, '[{"b":123,"b":456,"a":789} ,{"b":123},{"b":456},{"a":789}]'),
    (5, null),
    (6, '{"Data":[{"a_1":"value1","B2":{"zZ":123,"A_1":"value2","b2":"should_sort_last","b2":"should_sort_first","__key__":[{"dupKey":"first","Dupkey":"second","dupKey":"third","1dupKey":"fourth"},{"mix_key":"foo","Mix_Key":"bar","mix_key":"baz"}]},"B2":"anotherB2","b2":"duplicateB2","A_1":"anotherA_1"},{"b2":[{"k":1,"K":2,"k":3},{"b2":"array_dup","B2":"array_B2"}],"randomKey_3":{"foo":"test","Foo":"TEST","foo":"again"}}],"meta_9":{"info":"example","Info":"EXAMPLE","info":"duplicate"}}')
    """

    qt_sql """
    SELECT id, json_hash(j) as sorted_json
    FROM test_json_json_hash_table
    ORDER BY id
    """

    qt_sql """
    SELECT id, normalize_jsonb_numbers_to_double(cast(j as jsonb)) as sorted_jsonb
    FROM test_json_json_hash_table
    ORDER BY id
    """

    
    
    sql "DROP TABLE IF EXISTS test_json_json_hash_table"

    sql """
    CREATE TABLE IF NOT EXISTS test_json_json_hash_table (
        id INT,
        j JSON NOT NULL
    )
    DISTRIBUTED BY HASH(id) BUCKETS 3
    PROPERTIES (
        "replication_num" = "1"
    )
    """

    sql """
    INSERT INTO test_json_json_hash_table VALUES
    (1, '{"b":123,"b":456,"a":789}'),
    (2, '{"d":123,"c":456,"b":789,"a":1011}'),
    (3, '{"x":123,"y":456,"z":789}'),
    (4, '[{"b":123,"b":456,"a":789} ,{"b":123},{"b":456},{"a":789}]'),
    (6, '{"Data":[{"a_1":"value1","B2":{"zZ":123,"A_1":"value2","b2":"should_sort_last","b2":"should_sort_first","__key__":[{"dupKey":"first","Dupkey":"second","dupKey":"third","1dupKey":"fourth"},{"mix_key":"foo","Mix_Key":"bar","mix_key":"baz"}]},"B2":"anotherB2","b2":"duplicateB2","A_1":"anotherA_1"},{"b2":[{"k":1,"K":2,"k":3},{"b2":"array_dup","B2":"array_B2"}],"randomKey_3":{"foo":"test","Foo":"TEST","foo":"again"}}],"meta_9":{"info":"example","Info":"EXAMPLE","info":"duplicate"}}')
    """

    qt_sql """
    SELECT id, json_hash(j) as sorted_json
    FROM test_json_json_hash_table
    ORDER BY id
    """

    qt_sql """
    SELECT id, normalize_jsonb_numbers_to_double(cast(j as jsonb)) as sorted_jsonb
    FROM test_json_json_hash_table
    ORDER BY id
    """

}
