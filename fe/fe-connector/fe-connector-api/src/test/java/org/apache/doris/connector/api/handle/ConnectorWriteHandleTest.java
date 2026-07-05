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

package org.apache.doris.connector.api.handle;

import org.apache.doris.connector.api.ConnectorColumn;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Pins the {@link ConnectorWriteHandle#getWriteOperation()} default (P6.3-T03, deferred from T01).
 *
 * <p><b>WHY this matters:</b> the {@code writeOperation} is the SPI vocabulary on which the iceberg
 * write adopter selects its SDK op (T04 AppendFiles/RowDelta/…) and its Thrift sink dialect (T06
 * {@code TIcebergTableSink}/{@code TIcebergDeleteSink}/{@code TIcebergMergeSink}). The default MUST be
 * {@code INSERT} so every existing write handle (jdbc/maxcompute) — none of which override it — keeps
 * plain-append semantics and is byte-compatible (RFC §6 "default INSERT, 向后兼容").</p>
 */
public class ConnectorWriteHandleTest {

    /** Minimal handle that overrides nothing write-op related, to read the SPI default. */
    private static final class BareWriteHandle implements ConnectorWriteHandle {
        @Override
        public ConnectorTableHandle getTableHandle() {
            return null;
        }

        @Override
        public List<ConnectorColumn> getColumns() {
            return Collections.emptyList();
        }

        @Override
        public boolean isOverwrite() {
            return false;
        }

        @Override
        public Map<String, String> getWriteContext() {
            return Collections.emptyMap();
        }
    }

    @Test
    public void writeOperationDefaultsToInsert() {
        Assertions.assertEquals(WriteOperation.INSERT, new BareWriteHandle().getWriteOperation(),
                "a write handle that does not declare an operation must default to INSERT so existing "
                        + "connectors (jdbc/maxcompute) keep plain-append semantics");
    }

    @Test
    public void writeOperationEnumCoversAllDmlKinds() {
        // Guards parity-by-omission: the iceberg op-selection matrix (T04) and the sink-dialect switch depend on
        // exactly these kinds existing. REWRITE (P6.4-T06) maps rewrite_data_files onto the SDK RewriteFiles op.
        Assertions.assertArrayEquals(
                new WriteOperation[] {
                        WriteOperation.INSERT, WriteOperation.OVERWRITE, WriteOperation.DELETE,
                        WriteOperation.UPDATE, WriteOperation.MERGE, WriteOperation.REWRITE},
                WriteOperation.values());
    }

    @Test
    public void sortInfoDefaultsToNull() {
        // WHY: a write handle carries an engine-built TSortInfo only when the connector declares
        // write-sort columns (T06 getWriteSortColumns, iceberg WRITE ORDERED BY). The default MUST be
        // null so every existing write handle (jdbc/maxcompute, which never sets it) keeps its
        // byte-identical unsorted sink output — the engine sets sort_info only for sorted iceberg tables.
        Assertions.assertNull(new BareWriteHandle().getSortInfo(),
                "a write handle that declares no write sort must default to a null TSortInfo");
    }

    @Test
    public void branchNameDefaultsToEmpty() {
        // WHY: an INSERT INTO t@branch threads the target branch onto the write handle so a
        // versioned-table connector (iceberg/paimon) points the commit at the branch. The default MUST
        // be empty so every existing write handle (jdbc/maxcompute, which never sets it) keeps its
        // byte-identical default-ref write. MUTATION: a non-empty default would make a branchless write
        // appear branch-targeted.
        Assertions.assertEquals(Optional.empty(), new BareWriteHandle().getBranchName(),
                "a write handle that declares no branch must default to Optional.empty()");
    }
}
