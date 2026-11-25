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

import java.nio.ByteBuffer
import java.nio.ByteOrder

suite("test_hll_sparse_return_binary") {
    def normalizeHllBytes = { byte[] bytes ->
        // HLL_DATA_EXPLICIT serializes hashes in flat_hash_set bucket order.
        if (bytes.length >= 2 && bytes[0] == (byte) 1) {
            int count = bytes[1] & 0xff
            assertEquals(2 + count * 8, bytes.length)

            long[] hashes = new long[count]
            def buffer = ByteBuffer.wrap(bytes, 2, count * 8).order(ByteOrder.LITTLE_ENDIAN)
            for (int i = 0; i < count; i++) {
                hashes[i] = buffer.getLong()
            }
            Arrays.sort(hashes)

            byte[] normalized = Arrays.copyOf(bytes, bytes.length)
            def out = ByteBuffer.wrap(normalized, 2, count * 8).order(ByteOrder.LITTLE_ENDIAN)
            for (long hash : hashes) {
                out.putLong(hash)
            }
            return normalized
        }
        return bytes
    }

    sql "DROP TABLE IF EXISTS test_hll_sparse_return_binary"
    sql """
        CREATE TABLE test_hll_sparse_return_binary (
            id INT,
            h HLL HLL_UNION
        ) AGGREGATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql "INSERT INTO test_hll_sparse_return_binary VALUES (0, hll_empty())"
    sql 'INSERT INTO test_hll_sparse_return_binary SELECT 1, hll_hash(number) FROM numbers("number" = "10")'
    sql 'INSERT INTO test_hll_sparse_return_binary SELECT 2, hll_hash(number) FROM numbers("number" = "200")'
    sql 'INSERT INTO test_hll_sparse_return_binary SELECT 3, hll_hash(number) FROM numbers("number" = "8000")'

    def rows = []
    context.getConn().createStatement().withCloseable { statement ->
        statement.execute("SET return_object_data_as_binary = true")
        statement.executeQuery("""
            SELECT id, h, hll_to_base64(h), hll_cardinality(h)
            FROM test_hll_sparse_return_binary ORDER BY id
        """).withCloseable { rs ->
            while (rs.next()) {
                rows.add([id: rs.getInt(1), rawBytes: rs.getBytes(2),
                          base64Str: rs.getString(3), card: rs.getLong(4)])
            }
        }
    }

    assertEquals(4, rows.size())
    rows.eachWithIndex { row, id ->
        assertEquals(id, row.id)
        byte[] expectedBytes = Base64.getDecoder().decode(row.base64Str)
        assertEquals(expectedBytes.length, row.rawBytes.length, "HLL byte length differs for id ${id}")
        assertTrue(Arrays.equals(normalizeHllBytes(row.rawBytes), normalizeHllBytes(expectedBytes)),
                "HLL bytes differ for id ${id}")

        if (id == 0) {
            assertEquals(1, row.rawBytes.length)
        } else if (id == 1) {
            assertTrue(row.rawBytes.length < 16385)
        } else if (id == 2) {
            assertEquals((byte) 2, row.rawBytes[0]) // HLL_DATA_SPARSE
            assertTrue(row.rawBytes.length > 5)
            assertTrue(row.rawBytes.length < 16385)
        } else {
            assertEquals(16385, row.rawBytes.length)
        }

        def encoded = Base64.getEncoder().encodeToString(row.rawBytes)
        def result = sql "SELECT hll_cardinality(hll_from_base64('${encoded}'))"
        assertEquals(row.card as long, result[0][0] as long)
    }
}
