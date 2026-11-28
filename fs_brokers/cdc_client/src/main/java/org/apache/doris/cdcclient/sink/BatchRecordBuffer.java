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

import java.util.LinkedList;
import java.util.function.LongConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** buffer to queue. */
public class BatchRecordBuffer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(BatchRecordBuffer.class);
    public static final String LINE_SEPARATOR = "\n";
    private String labelName;
    private LinkedList<byte[]> buffer;
    private byte[] lineDelimiter;
    private int numOfRecords = 0;
    private long bufferSizeBytes = 0;
    private String database;
    private String table;
    private final LongConsumer onBytesChanged;

    public BatchRecordBuffer() {
        this(null, null, null);
    }

    public BatchRecordBuffer(String database, String table, byte[] lineDelimiter) {
        this(database, table, lineDelimiter, bytes -> {});
    }

    public BatchRecordBuffer(
            String database, String table, byte[] lineDelimiter, LongConsumer onBytesChanged) {
        this.database = database;
        this.table = table;
        this.lineDelimiter = lineDelimiter;
        this.buffer = new LinkedList<>();
        this.onBytesChanged = onBytesChanged;
    }

    public long insert(byte[] record) {
        long recordSize = record.length;
        if (!isEmpty() && lineDelimiter != null) {
            this.buffer.add(this.lineDelimiter);
            recordSize += this.lineDelimiter.length;
        }
        this.buffer.add(record);
        numOfRecords++;
        bufferSizeBytes += recordSize;
        onBytesChanged.accept(recordSize);
        return recordSize;
    }

    public String getLabelName() {
        return labelName;
    }

    public void setLabelName(String labelName) {
        this.labelName = labelName;
    }

    /**
     * @return true if buffer is empty
     */
    public boolean isEmpty() {
        return numOfRecords == 0;
    }

    public void clear() {
        long bytes = bufferSizeBytes;
        clearRecords();
        if (bytes != 0) {
            onBytesChanged.accept(-bytes);
        }
    }

    @Override
    public void close() {
        clear();
    }

    /** Transfer a batch from the same memory account without releasing its charge. */
    void mergeFrom(BatchRecordBuffer source) {
        if (source.isEmpty()) {
            return;
        }
        int separatorBytes = 0;
        if (!isEmpty() && lineDelimiter != null) {
            buffer.add(lineDelimiter);
            separatorBytes = lineDelimiter.length;
        }
        buffer.addAll(source.buffer);
        numOfRecords += source.numOfRecords;
        bufferSizeBytes += source.bufferSizeBytes + separatorBytes;
        source.clearRecords();
        if (separatorBytes != 0) {
            onBytesChanged.accept(separatorBytes);
        }
    }

    private void clearRecords() {
        this.buffer.clear();
        this.numOfRecords = 0;
        this.bufferSizeBytes = 0;
        this.labelName = null;
    }

    public LinkedList<byte[]> getBuffer() {
        return buffer;
    }

    /**
     * @return Number of records in this buffer
     */
    public int getNumOfRecords() {
        return numOfRecords;
    }

    /**
     * @return Buffer size in bytes
     */
    public long getBufferSizeBytes() {
        return bufferSizeBytes;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getTableIdentifier() {
        if (database != null && table != null) {
            return database + "." + table;
        }
        return null;
    }

    public byte[] getLineDelimiter() {
        return lineDelimiter;
    }
}
