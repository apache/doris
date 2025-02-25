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

package org.pentaho.di.trans.steps.dorisstreamloader.load;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

/** buffer to queue. */
public class BatchRecordBuffer {
    private static final Logger LOG = LoggerFactory.getLogger(BatchRecordBuffer.class);
    public static final String LINE_SEPARATOR = "\n";
    private String labelName;
    private LinkedList<byte[]> buffer;
    private byte[] lineDelimiter;
    private int numOfRecords = 0;
    private long bufferSizeBytes = 0;
    private boolean loadBatchFirstRecord = true;
    private String database;
    private String table;
    private final long createTime = System.currentTimeMillis();
    private long retainTime = 0;

    public BatchRecordBuffer() {
        this.buffer = new LinkedList<>();
    }

    public BatchRecordBuffer(String database, String table, byte[] lineDelimiter, long retainTime) {
        super();
        this.database = database;
        this.table = table;
        this.lineDelimiter = lineDelimiter;
        this.buffer = new LinkedList<>();
        this.retainTime = retainTime;
    }

    public int insert(byte[] record) {
        int recordSize = record.length;
        if (loadBatchFirstRecord) {
            loadBatchFirstRecord = false;
        } else if (lineDelimiter != null) {
            this.buffer.add(this.lineDelimiter);
            setBufferSizeBytes(this.bufferSizeBytes + this.lineDelimiter.length);
            recordSize += this.lineDelimiter.length;
        }
        this.buffer.add(record);
        setNumOfRecords(this.numOfRecords + 1);
        setBufferSizeBytes(this.bufferSizeBytes + record.length);
        return recordSize;
    }

    public String getLabelName() {
        return labelName;
    }

    public void setLabelName(String labelName) {
        this.labelName = labelName;
    }

    /** @return true if buffer is empty */
    public boolean isEmpty() {
        return numOfRecords == 0;
    }

    public void clear() {
        this.buffer.clear();
        this.numOfRecords = 0;
        this.bufferSizeBytes = 0;
        this.labelName = null;
        this.loadBatchFirstRecord = true;
    }

    public LinkedList<byte[]> getBuffer() {
        return buffer;
    }

    /** @return Number of records in this buffer */
    public int getNumOfRecords() {
        return numOfRecords;
    }

    /** @return Buffer size in bytes */
    public long getBufferSizeBytes() {
        return bufferSizeBytes;
    }

    /** @param numOfRecords Updates number of records (Usually by 1) */
    public void setNumOfRecords(int numOfRecords) {
        this.numOfRecords = numOfRecords;
    }

    /** @param bufferSizeBytes Updates sum of size of records present in this buffer (Bytes) */
    public void setBufferSizeBytes(long bufferSizeBytes) {
        this.bufferSizeBytes = bufferSizeBytes;
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

    public boolean shouldFlush() {
        // When the buffer create time is later than the first interval trigger,
        // the write will not be triggered in the next interval,
        // so multiply it by 1.5 to trigger it as early as possible.
        return (System.currentTimeMillis() - createTime) * 1.5 > retainTime;
    }
}
