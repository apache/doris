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

suite("test_parse_url_key") {

    sql """
        drop table if exists test_parse_url_key;
    """

    sql """
        CREATE TABLE `test_parse_url_key` (
            `id` int NULL,
            `url` text NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY RANDOM BUCKETS AUTO
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        insert into test_parse_url_key values
        (1, 'http://h/p#f?k=v'),
        (2, 'http://h/p&k=v?x=1'),
        (3, 'http://h/p#f/?#k=v'),
        (4, 'http://h/p?x=1#f&k=v'),
        (5, 'http://h/p&k=v?k=1#f&k=2'),
        (6, 'http://h/p#f&k=v?k=2'),
        (7, 'http://h/p'),
        (8, 'http://h/p?a=1&k=2')
    """

    // The query key must only be looked up inside the query component, which is located
    // between the first '?' and the following '#'. Keys appearing in the path or in the
    // fragment must not be returned.
    qt_sql """
        select id, parse_url(url, 'QUERY', 'k') as query_k from test_parse_url_key order by id
    """

    qt_sql """
        select id, parse_url(url, 'QUERY', 'x') as query_x from test_parse_url_key order by id
    """

    sql """
        drop table if exists test_parse_url_key;
    """

}
