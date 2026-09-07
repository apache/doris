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

suite("test_uuid_variant_identity", "nonConcurrent") {
    setFeConfigTemporary([enable_variant_v2: true]) {
        qt_native_uuid_variant_type """
            SELECT variant_type(CAST(CAST('00112233-4455-6677-8899-aabbccddeeff' AS UUID) AS VARIANT)),
                   CAST(CAST(CAST('00112233-4455-6677-8899-aabbccddeeff' AS UUID) AS VARIANT) AS UUID),
                   variant_type(CAST(CAST(NULL AS UUID) AS VARIANT))
        """
        sql "DROP TABLE IF EXISTS uuid_variant_identity"
        sql """
            CREATE TABLE uuid_variant_identity (id INT, v VARIANT<'u':UUID>)
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES('replication_num'='1')
        """
        sql """
            INSERT INTO uuid_variant_identity VALUES
            (1, parse_to_variant('{"u":"00112233-4455-6677-8899-aabbccddeeff"}')),
            (2, parse_to_variant('{"u":null}'))
        """
        order_qt_persisted_uuid_variant_type """
            SELECT id, variant_type(v['u']), CAST(v['u'] AS UUID)
            FROM uuid_variant_identity ORDER BY id
        """
    }
}
