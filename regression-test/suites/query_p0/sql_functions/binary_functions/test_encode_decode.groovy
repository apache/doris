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

    order_qt_encode_supported_charsets """
        select id, hex(encode(plain_text, charset))
        from test_encode_decode
        order by id
    """

    order_qt_decode_supported_charsets """
        select id, decode(cast(binary_value as varbinary), charset)
        from test_encode_decode
        order by id
    """

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

    sql "insert into test_encode_decode values (13, 'A', unhex('41'), 'Uſ-ASCII')"

    test {
        sql "select encode(plain_text, charset) from test_encode_decode where id = 13"
        exception "Unsupported character set"
    }
}
