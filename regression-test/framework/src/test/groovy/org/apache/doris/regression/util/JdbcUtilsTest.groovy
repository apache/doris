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

package org.apache.doris.regression.util

import org.apache.arrow.driver.jdbc.ArrowFlightJdbcArray
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.memory.RootAllocator
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.UuidVector
import org.junit.jupiter.api.Test

import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.Types

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue

class JdbcUtilsTest {
    @Test
    void materializesArraysBeforeAdvancingOrClosingResultSet() {
        checkArrayLifetime { JdbcUtils.toList(it) }
    }

    @Test
    void materializesArraysInStringResults() {
        checkArrayLifetime { JdbcUtils.toStringList(it, false) }
    }

    @Test
    void materializesArraysInPreparedStringResults() {
        checkArrayLifetime { JdbcUtils.toStringList(it, true) }
    }

    private static void checkArrayLifetime(Closure readRows) {
        UUID first = UUID.fromString('00112233-4455-6677-8899-aabbccddeeff')
        UUID second = UUID.fromString('ffffffff-ffff-ffff-ffff-ffffffffffff')
        new RootAllocator(Long.MAX_VALUE).withCloseable { allocator ->
            new UuidVector('uuid', allocator).withCloseable { vector ->
                vector.allocateNew(2)
                int row = -1
                boolean closed = false
                ResultSetMetaData metadata = [
                    getColumnCount: { 1 },
                    getColumnType: { int index -> Types.ARRAY }
                ] as ResultSetMetaData
                ResultSet resultSet = [
                    getMetaData: { metadata },
                    next: {
                        ++row
                        // Reuse the same Arrow buffers for a new batch, then release them.
                        if (row < 2) {
                            vector.reset()
                            vector.setSafe(0, row == 0 ? first : second)
                            vector.setNull(1)
                            vector.setValueCount(2)
                        } else {
                            vector.clear()
                        }
                        row < 4
                    },
                    getObject: { int index ->
                        row == 3 ? null : new ArrowFlightJdbcArray(vector, 0, row == 2 ? 0 : 2)
                    },
                    close: {
                        vector.clear()
                        closed = true
                    }
                ] as ResultSet

                def (rows, meta) = readRows(resultSet)
                assertTrue(closed)
                assertEquals([["[${first}, null]".toString()], ["[${second}, null]".toString()],
                              ['[]'], [null]], rows)
                assertEquals(metadata, meta)
            }
        }
    }
}
