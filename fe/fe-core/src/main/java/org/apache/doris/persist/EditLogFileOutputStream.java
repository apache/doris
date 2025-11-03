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

package org.apache.doris.persist;

import org.apache.doris.common.io.DataOutputBuffer;
import org.apache.doris.common.io.Writable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * An implementation of the abstract class {@link EditLogOutputStream},
 * which stores edits in a local file.
 */
public class EditLogFileOutputStream extends EditLogOutputStream {
    private File file;
    private FileOutputStream fp;           // file stream for storing edit logs
    private FileChannel fc;                // channel of the file stream for sync
    private DataOutputBuffer bufCurrent;   // current buffer for writing
    private DataOutputBuffer bufReady;     // buffer ready for flushing

    static ByteBuffer fill = ByteBuffer.allocateDirect(512); // pre-allocation
    private static int sizeFlushBuffer = 512 * 1024;

    public EditLogFileOutputStream(File name) throws IOException {
        super();
        file = name;
        bufCurrent = new DataOutputBuffer(sizeFlushBuffer);
        bufReady = new DataOutputBuffer(sizeFlushBuffer);
        RandomAccessFile rp = new RandomAccessFile(name, "rw");
        fp = new FileOutputStream(rp.getFD()); // open for append
        fc = rp.getChannel();
        fc.position(fc.size());
    }

    String getName() {
        return file.getPath();
    }

    public void write(int b) throws IOException {
        bufCurrent.write(b);
    }

    public void write(short op, Writable writable) throws IOException {
        bufCurrent.writeShort(op);
        writable.write(bufCurrent);
    }

    public void write(short op, byte[] data) throws IOException {
        bufCurrent.writeShort(op);
        bufCurrent.write(data);
    }

    // Create empty edits logs file.
    void create() throws IOException {
        fc.truncate(0);
        fc.position(0);
        setReadyToFlush();
        flush();
    }

    public void close() throws IOException {
        // close should have been called after all pending transactions
        // have been flushed & synced.
        int bufSize = bufCurrent.size();
        if (bufSize != 0) {
            throw new IOException("EditStream has " + bufSize
                    + " bytes still to be flushed and cannot "
                    + "be closed.");
        }
        bufCurrent.close();
        bufReady.close();

        // remove the last INVALID marker from transaction log.
        fc.truncate(fc.position());
        fp.close();

        bufCurrent = null;
        bufReady = null;
    }

    /**
     * All data that has been written to the stream so far will be flushed.
     * New data can be still written to the stream while flushing is performed.
     */
    public void setReadyToFlush() throws IOException {
        assert bufReady.size() == 0 : "previous data is not flushed yet";
        write(OperationType.OP_LOCAL_EOF); // insert end-of-file marker
        DataOutputBuffer tmp = bufReady;
        bufReady = bufCurrent;
        bufCurrent = tmp;
    }

    /**
     * Flush ready buffer to persistent store. currentBuffer is not flushed
     * as it accumulates new log records while readyBuffer will be flushed and synced.
     */
    protected void flushAndSync() throws IOException {
        preallocate();             // preallocate file if necessary
        bufReady.writeTo(fp);      // write data to file
        bufReady.reset();          // erase all data in the buffer
        fc.force(false);           // metadata updates not needed because of preallocation
        fc.position(fc.position() - 1); // skip back the end-of-file marker
    }

    // Return the size of the current edit log including buffered data.
    long length() throws IOException {
        return fc.size() + bufReady.size() + bufCurrent.size();
    }

    // Allocate a big chunk of data
    private void preallocate() throws IOException {
        long position = fc.position();
        if (position + 4096 >= fc.size()) {
            // use pre allocate , then file size will not be changed very time,
            // so fsync needn't update file size every time. This is an optimization.
            long newSize = position + 1024 * 1024; // 1MB
            fill.position(0);
            fc.write(fill, newSize);
        }
    }

    File getFile() {
        return file;
    }

}
