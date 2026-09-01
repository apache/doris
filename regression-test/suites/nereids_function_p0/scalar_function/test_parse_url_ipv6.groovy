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

suite("test_parse_url_ipv6") {
    order_qt_parse_url_ipv6_constant """
        select parse_url('http://[2001:db8::1]:8080/a?x=1#r', 'HOST'),
               parse_url('http://[2001:db8::1]:8080/a?x=1#r', 'PORT'),
               parse_url('http://user:pw@[2001:db8::1]:8080/a', 'HOST'),
               parse_url('http://[2001:db8::1]/a', 'PORT')
    """

    sql "drop table if exists test_parse_url_ipv6"
    sql """
        create table test_parse_url_ipv6 (
            id int,
            url string
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties ("replication_num" = "1")
    """
    sql """
        insert into test_parse_url_ipv6 values
            (1, 'http://[2001:db8::1]:8080/a?x=1#r'),
            (2, 'http://[2001:db8::1]/a'),
            (3, 'http://user:pw@[2001:db8::1]:8080/a')
    """

    order_qt_parse_url_ipv6_column """
        select id, parse_url(url, 'HOST'), parse_url(url, 'PORT')
        from test_parse_url_ipv6
        order by id
    """
}
