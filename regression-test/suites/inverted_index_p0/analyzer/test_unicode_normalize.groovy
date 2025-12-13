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

suite("test_unicode_normalize", "p0") {
    sql "set batch_size = 4096;"

    qt_unicode_normalize_nfc_1 """
        select hex(unicode_normalize(cast(unhex('43616665CC81') as string), 'NFC'));
    """
    qt_unicode_normalize_nfc_2 """
        select hex(unicode_normalize(cast(unhex('436166C3A9') as string), 'NFC'));
    """

    qt_unicode_normalize_nfd """
        select hex(unicode_normalize(cast(unhex('436166C3A9') as string), '  nFd  '));
    """

    qt_unicode_normalize_nfkc_cf """
        select unicode_normalize('ABC 123', ' nfkc_cf ');
    """

    qt_unicode_normalize_nfkd_ascii """
        select unicode_normalize('plain-ascii', 'NFKD');
    """

    qt_unicode_normalize_nulls """
        select
            unicode_normalize(NULL, 'NFC'),
            unicode_normalize('', 'NFC');
    """

    sql "DROP TABLE IF EXISTS test_unicode_normalize_not_const"

    sql """
        CREATE TABLE test_unicode_normalize_not_const (
            id   int,
            col  string,
            mode string
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES("replication_allocation" = "tag.location.default: 1");
    """

    sql """
        INSERT INTO test_unicode_normalize_not_const VALUES
        (1, 'Abc', 'NFKC_CF'),
        (2, 'Def', 'NFD');
    """

    test {
        sql """
            select unicode_normalize(col, mode)
            from test_unicode_normalize_not_const;
        """
        exception "must be constant"
    }

    test {
        sql """
            select unicode_normalize('abc', 'INVALID_MODE');
        """
        exception "Invalid normalization mode"
    }
}