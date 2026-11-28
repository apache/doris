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

package org.apache.doris.cdcclient.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

/** Unit tests for {@link BatchRecordBuffer}. */
class BatchRecordBufferTest {

    private static final byte[] DELIM = "\n".getBytes(StandardCharsets.UTF_8);

    @Test
    void accountsAppendsAndReturnsBytesOnceWhenClosed() {
        AtomicLong bytes = new AtomicLong();
        BatchRecordBuffer buffer = new BatchRecordBuffer("db", "t", DELIM, bytes::addAndGet);
        buffer.insert("ab".getBytes(StandardCharsets.UTF_8));
        buffer.insert("cde".getBytes(StandardCharsets.UTF_8));
        assertEquals(6, bytes.get());

        buffer.close();
        buffer.close();

        assertEquals(0, bytes.get());
        assertTrue(buffer.isEmpty());
        assertTrue(buffer.getBuffer().isEmpty());
    }

    @Test
    void mergingTransfersTheChargeAndOnlyAddsTheJoiningDelimiter() throws Exception {
        AtomicLong bytes = new AtomicLong();
        BatchRecordBuffer target = new BatchRecordBuffer("db", "t", DELIM, bytes::addAndGet);
        BatchRecordBuffer source = new BatchRecordBuffer("db", "t", DELIM, bytes::addAndGet);
        target.insert("ab".getBytes(StandardCharsets.UTF_8));
        source.insert("cd".getBytes(StandardCharsets.UTF_8));
        source.insert("e".getBytes(StandardCharsets.UTF_8));
        assertEquals(6, bytes.get());

        target.mergeFrom(source);
        source.close();
        target.mergeFrom(source);

        assertEquals(7, bytes.get());
        assertEquals(7, target.getBufferSizeBytes());
        assertEquals(3, target.getNumOfRecords());
        assertTrue(source.isEmpty());
        try (BatchBufferStream stream = new BatchBufferStream(target.getBuffer())) {
            assertEquals("ab\ncd\ne", new String(stream.readAllBytes(), StandardCharsets.UTF_8));
        }
        target.close();
        assertEquals(0, bytes.get());
    }

    @Test
    void mergingIntoEmptyBufferTransfersChargeWithoutLeadingDelimiter() {
        AtomicLong bytes = new AtomicLong();
        BatchRecordBuffer target = new BatchRecordBuffer("db", "t", DELIM, bytes::addAndGet);
        BatchRecordBuffer source = new BatchRecordBuffer("db", "t", DELIM, bytes::addAndGet);
        source.insert("ab".getBytes(StandardCharsets.UTF_8));
        target.mergeFrom(source);
        source.close();
        target.insert("c".getBytes(StandardCharsets.UTF_8));
        assertEquals(4, bytes.get());
        target.clear();
        assertEquals(0, bytes.get());
        target.insert("xy".getBytes(StandardCharsets.UTF_8));
        assertEquals(2, bytes.get());
        target.close();
        assertEquals(0, bytes.get());
    }

    @Test
    void firstInsert_noDelimiterPrefix() {
        BatchRecordBuffer buffer = new BatchRecordBuffer("db", "t", DELIM);
        byte[] rec = "abc".getBytes(StandardCharsets.UTF_8);

        long written = buffer.insert(rec);

        assertEquals(3, written);
        assertEquals(1, buffer.getNumOfRecords());
        assertEquals(3, buffer.getBufferSizeBytes());
        assertEquals(1, buffer.getBuffer().size());
        assertFalse(buffer.isEmpty());
    }

    @Test
    void secondInsert_addsDelimiter() {
        BatchRecordBuffer buffer = new BatchRecordBuffer("db", "t", DELIM);
        buffer.insert("ab".getBytes(StandardCharsets.UTF_8));

        long written = buffer.insert("cde".getBytes(StandardCharsets.UTF_8));

        // returned size includes the delimiter that precedes the second record
        assertEquals(3 + DELIM.length, written);
        assertEquals(2, buffer.getNumOfRecords());
        // 2 + delim(1) + 3
        assertEquals(2 + DELIM.length + 3, buffer.getBufferSizeBytes());
        // record, delimiter, record
        assertEquals(3, buffer.getBuffer().size());
    }

    @Test
    void nullDelimiter_neverInsertsSeparator() {
        BatchRecordBuffer buffer = new BatchRecordBuffer("db", "t", null);
        buffer.insert("ab".getBytes(StandardCharsets.UTF_8));
        buffer.insert("cd".getBytes(StandardCharsets.UTF_8));

        assertEquals(2, buffer.getNumOfRecords());
        assertEquals(4, buffer.getBufferSizeBytes());
        assertEquals(2, buffer.getBuffer().size());
    }

    @Test
    void clear_resetsState() {
        BatchRecordBuffer buffer = new BatchRecordBuffer("db", "t", DELIM);
        buffer.insert("ab".getBytes(StandardCharsets.UTF_8));
        buffer.setLabelName("lbl");

        buffer.clear();

        assertTrue(buffer.isEmpty());
        assertEquals(0, buffer.getNumOfRecords());
        assertEquals(0, buffer.getBufferSizeBytes());
        assertNull(buffer.getLabelName());
        assertTrue(buffer.getBuffer().isEmpty());
        // after clear, the next insert is treated as the first again (no leading delimiter)
        assertEquals(2, buffer.insert("xy".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void tableIdentifier() {
        assertEquals("db.t", new BatchRecordBuffer("db", "t", DELIM).getTableIdentifier());
        assertNull(new BatchRecordBuffer().getTableIdentifier());
    }
}
