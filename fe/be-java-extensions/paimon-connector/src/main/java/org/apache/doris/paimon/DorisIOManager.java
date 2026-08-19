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

package org.apache.doris.paimon;

import org.apache.paimon.disk.BufferFileReader;
import org.apache.paimon.disk.BufferFileWriter;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.memory.Buffer;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Paimon IOManager adapter which charges temporary channel I/O to Doris spill management. */
final class DorisIOManager implements IOManager {
    interface SpillAccountant {
        void reserve(String path, long bytes) throws IOException;

        void rollback(String path, long bytes);

        void commitWrite(String path, long bytes);

        void recordRead(String path, long bytes);

        void release(String path, long bytes);
    }

    private final IOManager delegate;
    private final SpillAccountant accountant;
    private final Map<String, Long> channelBytes = new ConcurrentHashMap<>();
    private final Map<String, Integer> activeChannelWriters = new ConcurrentHashMap<>();

    static DorisIOManager create(String[] tempDirs, long nativeSpillDirectory) {
        return new DorisIOManager(
                IOManager.create(tempDirs), new NativeSpillAccountant(nativeSpillDirectory));
    }

    DorisIOManager(IOManager delegate, SpillAccountant accountant) {
        this.delegate = delegate;
        this.accountant = accountant;
    }

    @Override
    public FileIOChannel.ID createChannel() {
        return delegate.createChannel();
    }

    @Override
    public FileIOChannel.ID createChannel(String prefix) {
        return delegate.createChannel(prefix);
    }

    @Override
    public String[] tempDirs() {
        return delegate.tempDirs();
    }

    @Override
    public String pickTempDir() {
        return delegate.pickTempDir();
    }

    @Override
    public FileIOChannel.Enumerator createChannelEnumerator() {
        return delegate.createChannelEnumerator();
    }

    @Override
    public BufferFileWriter createBufferFileWriter(FileIOChannel.ID channelID) throws IOException {
        // Paimon ExternalBuffer.reset() deletes old channel files directly instead of calling the
        // IOManager deletion methods. Reconcile once per writer instead of scanning all historical
        // channels before every block written by the same writer.
        releaseDeletedChannels();
        return new AccountingBufferFileWriter(delegate.createBufferFileWriter(channelID), this);
    }

    @Override
    public BufferFileReader createBufferFileReader(FileIOChannel.ID channelID) throws IOException {
        return new AccountingBufferFileReader(delegate.createBufferFileReader(channelID), this);
    }

    @Override
    public void close() throws Exception {
        try {
            delegate.close();
        } finally {
            releaseDeletedChannels();
        }
    }

    private boolean releaseDeletedChannels() {
        boolean releasedAny = false;
        for (Map.Entry<String, Long> entry : channelBytes.entrySet()) {
            if (!activeChannelWriters.containsKey(entry.getKey())
                    && !new File(entry.getKey()).exists()
                    && channelBytes.remove(entry.getKey(), entry.getValue())) {
                accountant.release(entry.getKey(), entry.getValue());
                releasedAny = true;
            }
        }
        return releasedAny;
    }

    private void reserveWrite(FileIOChannel.ID channelID, long bytes) throws IOException {
        String path = channelID.getPath();
        activeChannelWriters.merge(path, 1, Integer::sum);
        boolean accounted = false;
        boolean tracked = false;
        try {
            try {
                accountant.reserve(path, bytes);
            } catch (IOException reserveFailure) {
                // A different ExternalBuffer may have directly deleted an old channel after this
                // writer was created. Retry only when reconciliation actually released capacity.
                if (!releaseDeletedChannels()) {
                    throw reserveFailure;
                }
                accountant.reserve(path, bytes);
            }
            accounted = true;
            channelBytes.merge(path, bytes, Long::sum);
            tracked = true;
        } finally {
            if (!tracked) {
                if (accounted) {
                    accountant.rollback(path, bytes);
                }
                finishWrite(channelID);
            }
        }
    }

    private void finishWrite(FileIOChannel.ID channelID) {
        activeChannelWriters.computeIfPresent(channelID.getPath(), (ignored, writers) ->
                writers == 1 ? null : writers - 1);
    }

    private void releaseChannel(FileIOChannel.ID channelID) {
        Long released = channelBytes.remove(channelID.getPath());
        if (released != null) {
            accountant.release(channelID.getPath(), released);
        }
    }

    private static final class NativeSpillAccountant implements SpillAccountant {
        private final long nativeSpillDirectory;

        private NativeSpillAccountant(long nativeSpillDirectory) {
            this.nativeSpillDirectory = nativeSpillDirectory;
        }

        @Override
        public void reserve(String path, long bytes) throws IOException {
            PaimonJniWriter.reservePaimonSpill(nativeSpillDirectory, path, bytes);
        }

        @Override
        public void rollback(String path, long bytes) {
            PaimonJniWriter.updatePaimonSpillAccounting(
                    nativeSpillDirectory, path, -bytes, 0, 0);
        }

        @Override
        public void commitWrite(String path, long bytes) {
            PaimonJniWriter.updatePaimonSpillAccounting(
                    nativeSpillDirectory, path, 0, bytes, 0);
        }

        @Override
        public void recordRead(String path, long bytes) {
            PaimonJniWriter.updatePaimonSpillAccounting(
                    nativeSpillDirectory, path, 0, 0, bytes);
        }

        @Override
        public void release(String path, long bytes) {
            PaimonJniWriter.updatePaimonSpillAccounting(
                    nativeSpillDirectory, path, -bytes, 0, 0);
        }
    }

    private static final class AccountingBufferFileWriter implements BufferFileWriter {
        private final BufferFileWriter delegate;
        private final DorisIOManager manager;

        private AccountingBufferFileWriter(BufferFileWriter delegate, DorisIOManager manager) {
            this.delegate = delegate;
            this.manager = manager;
        }

        @Override
        public void writeBlock(Buffer buffer) throws IOException {
            long bytes = Integer.BYTES + buffer.getSize();
            manager.reserveWrite(getChannelID(), bytes);
            try {
                delegate.writeBlock(buffer);
                manager.accountant.commitWrite(getChannelID().getPath(), bytes);
            } finally {
                manager.finishWrite(getChannelID());
            }
        }

        @Override
        public FileIOChannel.ID getChannelID() {
            return delegate.getChannelID();
        }

        @Override
        public long getSize() throws IOException {
            return delegate.getSize();
        }

        @Override
        public boolean isClosed() {
            return delegate.isClosed();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public void deleteChannel() {
            delegate.deleteChannel();
            if (!getChannelID().getPathFile().exists()) {
                manager.releaseChannel(getChannelID());
            }
        }

        @Override
        public FileChannel getNioFileChannel() {
            return delegate.getNioFileChannel();
        }

        @Override
        public void closeAndDelete() throws IOException {
            delegate.closeAndDelete();
            if (!getChannelID().getPathFile().exists()) {
                manager.releaseChannel(getChannelID());
            }
        }
    }

    private static final class AccountingBufferFileReader implements BufferFileReader {
        private final BufferFileReader delegate;
        private final DorisIOManager manager;

        private AccountingBufferFileReader(BufferFileReader delegate, DorisIOManager manager) {
            this.delegate = delegate;
            this.manager = manager;
        }

        @Override
        public void readInto(Buffer buffer) throws IOException {
            long position = delegate.getNioFileChannel().position();
            delegate.readInto(buffer);
            manager.accountant.recordRead(
                    getChannelID().getPath(), delegate.getNioFileChannel().position() - position);
        }

        @Override
        public boolean hasReachedEndOfFile() {
            return delegate.hasReachedEndOfFile();
        }

        @Override
        public FileIOChannel.ID getChannelID() {
            return delegate.getChannelID();
        }

        @Override
        public long getSize() throws IOException {
            return delegate.getSize();
        }

        @Override
        public boolean isClosed() {
            return delegate.isClosed();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public void deleteChannel() {
            delegate.deleteChannel();
            if (!getChannelID().getPathFile().exists()) {
                manager.releaseChannel(getChannelID());
            }
        }

        @Override
        public FileChannel getNioFileChannel() {
            return delegate.getNioFileChannel();
        }

        @Override
        public void closeAndDelete() throws IOException {
            delegate.closeAndDelete();
            if (!getChannelID().getPathFile().exists()) {
                manager.releaseChannel(getChannelID());
            }
        }
    }
}
