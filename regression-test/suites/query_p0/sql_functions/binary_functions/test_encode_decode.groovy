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

suite("test_encode_decode") {
    sql "drop table if exists test_encode_decode"
    sql """
        create table test_encode_decode (
            id int,
            plain_text string,
            binary_value string,
            charset varchar(32)
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties ("replication_num" = "1")
    """

    sql """
        insert into test_encode_decode values
            (1, 'A', unhex('41'), 'US-ASCII'),
            (2, 'é', unhex('E9'), 'ISO-8859-1'),
            (3, '中', unhex('E4B8AD'), 'UTF-8'),
            (4, '中', unhex('4E2D'), 'UTF-16BE'),
            (5, '中', unhex('2D4E'), 'UTF-16LE'),
            (6, '中', unhex('FEFF4E2D'), 'UTF-16'),
            (7, '😀', unhex('D83DDE00'), 'UTF-16BE'),
            (8, '', unhex(''), 'UTF-16'),
            (9, '中', unhex('FFFE2D4E'), 'utf-16'),
            (10, '中', unhex('4E2D'), 'UTF-16'),
            (11, null, null, 'UTF-8'),
            (12, 'text', unhex('74657874'), null)
    """

    // Vectorized CASE evaluates every branch for every row, so mixed-charset
    // CASE encode/decode queries fail on strict conversion. Filter each
    // constant charset down to compatible rows instead.
    order_qt_encode_supported_charsets """
        select * from (
            select id, hex(encode(plain_text, 'US-ASCII')) as encoded from test_encode_decode where id = 1
            union all
            select id, hex(encode(plain_text, 'ISO-8859-1')) from test_encode_decode where id = 2
            union all
            select id, hex(encode(plain_text, 'UTF-8')) from test_encode_decode where id = 3
            union all
            select id, hex(encode(plain_text, 'UTF-16BE')) from test_encode_decode where id = 4
            union all
            select id, hex(encode(plain_text, 'UTF-16LE')) from test_encode_decode where id = 5
            union all
            select id, hex(encode(plain_text, 'UTF-16')) from test_encode_decode where id = 6
            union all
            select id, hex(encode(plain_text, 'UTF-16BE')) from test_encode_decode where id = 7
            union all
            select id, hex(encode(plain_text, 'UTF-16')) from test_encode_decode where id = 8
            union all
            select id, hex(encode(plain_text, 'utf-16')) from test_encode_decode where id = 9
            union all
            select id, hex(encode(plain_text, 'UTF-16')) from test_encode_decode where id = 10
            union all
            select id, hex(encode(plain_text, 'UTF-8')) from test_encode_decode where id = 11
            union all
            select id, hex(encode(plain_text, cast(null as string))) from test_encode_decode where id = 12
        ) t
        order by id
    """

    order_qt_decode_supported_charsets """
        select * from (
            select id, decode(cast(binary_value as varbinary), 'US-ASCII') as decoded from test_encode_decode where id = 1
            union all
            select id, decode(cast(binary_value as varbinary), 'ISO-8859-1') from test_encode_decode where id = 2
            union all
            select id, decode(cast(binary_value as varbinary), 'UTF-8') from test_encode_decode where id = 3
            union all
            select id, decode(cast(binary_value as varbinary), 'UTF-16BE') from test_encode_decode where id = 4
            union all
            select id, decode(cast(binary_value as varbinary), 'UTF-16LE') from test_encode_decode where id = 5
            union all
            select id, decode(cast(binary_value as varbinary), 'UTF-16') from test_encode_decode where id = 6
            union all
            select id, decode(cast(binary_value as varbinary), 'UTF-16BE') from test_encode_decode where id = 7
            union all
            select id, decode(cast(binary_value as varbinary), 'UTF-16') from test_encode_decode where id = 8
            union all
            select id, decode(cast(binary_value as varbinary), 'utf-16') from test_encode_decode where id = 9
            union all
            select id, decode(cast(binary_value as varbinary), 'UTF-16') from test_encode_decode where id = 10
            union all
            select id, decode(cast(binary_value as varbinary), 'UTF-8') from test_encode_decode where id = 11
            union all
            select id, decode(cast(binary_value as varbinary), cast(null as string)) from test_encode_decode where id = 12
        ) t
        order by id
    """

    qt_encode_constant_expr "select hex(encode('中', upper('utf-8')))"
    qt_decode_constant_expr "select decode(X'E4B8AD', upper('utf-8'))"
    qt_encode_null_valid_charset "select encode(null, 'UTF-8')"
    qt_decode_null_valid_charset "select decode(null, 'UTF-8')"

    test {
        sql "select encode('text', 'GBK')"
        exception "Unsupported character set"
    }

    test {
        sql "select encode('中', 'US-ASCII')"
        exception "Character conversion using 'US-ASCII' failed"
    }

    test {
        sql "select decode(X'E4B8', 'UTF-8')"
        exception "Character conversion using 'UTF-8' failed"
    }

    test {
        sql "select encode('A', 'Uſ-ASCII')"
        exception "Unsupported character set"
    }

    test {
        sql "select encode(plain_text, charset) from test_encode_decode where id = 1"
        exception "second argument of function encode must be constant"
    }

    test {
        sql "select decode(cast(binary_value as varbinary), charset) from test_encode_decode where id = 1"
        exception "second argument of function decode must be constant"
    }

    test {
        sql "select encode(null, 'GBK')"
        exception "Unsupported character set"
    }

    test {
        sql "select encode(cast(null as string), 'GBK')"
        exception "Unsupported character set"
    }

    test {
        sql "select encode(plain_text, 'GBK') from test_encode_decode where id = 11"
        exception "Unsupported character set"
    }

    test {
        sql "select decode(null, 'GBK')"
        exception "Unsupported character set"
    }

    test {
        sql "select decode(cast(null as varbinary), 'GBK')"
        exception "Unsupported character set"
    }

    test {
        sql "select decode(cast(binary_value as varbinary), 'GBK') from test_encode_decode where id = 11"
        exception "Unsupported character set"
    }
}
