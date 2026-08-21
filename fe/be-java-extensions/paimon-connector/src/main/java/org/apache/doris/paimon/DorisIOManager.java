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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;

/** Paimon IOManager adapter which charges temporary channel I/O to one Doris spill session. */
final class DorisIOManager implements IOManager {
    interface SpillAccountant {
        String getSpillDirectory() throws IOException;

        void reserve(long bytes) throws IOException;

        void rollback(long bytes);

        void commitWrite(long bytes);

        void recordRead(long bytes);

        void release(long bytes);
    }

    private final SpillAccountant accountant;
    private volatile IOManager delegate;

    static DorisIOManager create(long nativeSpillSession) {
        return new DorisIOManager(new NativeSpillAccountant(nativeSpillSession));
    }

    DorisIOManager(SpillAccountant accountant) {
        this.accountant = accountant;
    }

    DorisIOManager(IOManager delegate, SpillAccountant accountant) {
        this.accountant = accountant;
        this.delegate = delegate;
    }

    private IOManager delegate() throws IOException {
        if (delegate == null) {
            synchronized (this) {
                if (delegate == null) {
                    String spillDirectory = accountant.getSpillDirectory();
                    if (spillDirectory == null || spillDirectory.isEmpty()) {
                        throw new IOException("Doris spill manager returned no available directory");
                    }
                    delegate = IOManager.create(spillDirectory);
                }
            }
        }
        return delegate;
    }

    private IOManager uncheckedDelegate() {
        try {
            return delegate();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to initialize the Doris spill directory", e);
        }
    }

    @Override
    public FileIOChannel.ID createChannel() {
        return uncheckedDelegate().createChannel();
    }

    @Override
    public FileIOChannel.ID createChannel(String prefix) {
        return uncheckedDelegate().createChannel(prefix);
    }

    @Override
    public String[] tempDirs() {
        return uncheckedDelegate().tempDirs();
    }

    @Override
    public String pickTempDir() {
        return uncheckedDelegate().pickTempDir();
    }

    @Override
    public FileIOChannel.Enumerator createChannelEnumerator() {
        return uncheckedDelegate().createChannelEnumerator();
    }

    @Override
    public BufferFileWriter createBufferFileWriter(FileIOChannel.ID channelID) throws IOException {
        return new AccountingBufferFileWriter(delegate().createBufferFileWriter(channelID), this);
    }

    @Override
    public BufferFileReader createBufferFileReader(FileIOChannel.ID channelID) throws IOException {
        return new AccountingBufferFileReader(delegate().createBufferFileReader(channelID), this);
    }

    @Override
    public void close() throws Exception {
        IOManager initializedDelegate = delegate;
        if (initializedDelegate != null) {
            initializedDelegate.close();
        }
    }

    private static long channelSize(FileIOChannel.ID channelID) {
        return channelID.getPathFile().isFile() ? channelID.getPathFile().length() : 0;
    }

    private void releaseIfDeleted(FileIOChannel.ID channelID, long bytes) {
        if (bytes > 0 && !channelID.getPathFile().exists()) {
            accountant.release(bytes);
        }
    }

    private static final class NativeSpillAccountant implements SpillAccountant {
        private final long nativeSpillSession;

        private NativeSpillAccountant(long nativeSpillSession) {
            this.nativeSpillSession = nativeSpillSession;
        }

        @Override
        public String getSpillDirectory() throws IOException {
            return PaimonJniWriter.getPaimonSpillDirectory(nativeSpillSession);
        }

        @Override
        public void reserve(long bytes) throws IOException {
            PaimonJniWriter.reservePaimonSpill(nativeSpillSession, bytes);
        }

        @Override
        public void rollback(long bytes) {
            PaimonJniWriter.updatePaimonSpillAccounting(nativeSpillSession, -bytes, 0, 0);
        }

        @Override
        public void commitWrite(long bytes) {
            PaimonJniWriter.updatePaimonSpillAccounting(nativeSpillSession, 0, bytes, 0);
        }

        @Override
        public void recordRead(long bytes) {
            PaimonJniWriter.updatePaimonSpillAccounting(nativeSpillSession, 0, 0, bytes);
        }

        @Override
        public void release(long bytes) {
            PaimonJniWriter.updatePaimonSpillAccounting(nativeSpillSession, -bytes, 0, 0);
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
            manager.accountant.reserve(bytes);
            try {
                delegate.writeBlock(buffer);
                manager.accountant.commitWrite(bytes);
            } catch (IOException | RuntimeException writeFailure) {
                try {
                    delegate.closeAndDelete();
                } catch (IOException | RuntimeException cleanupFailure) {
                    writeFailure.addSuppressed(cleanupFailure);
                }
                if (!getChannelID().getPathFile().exists()) {
                    manager.accountant.rollback(bytes);
                }
                throw writeFailure;
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
            long bytes = channelSize(getChannelID());
            try {
                delegate.deleteChannel();
            } finally {
                manager.releaseIfDeleted(getChannelID(), bytes);
            }
        }

        @Override
        public FileChannel getNioFileChannel() {
            return delegate.getNioFileChannel();
        }

        @Override
        public void closeAndDelete() throws IOException {
            long bytes = channelSize(getChannelID());
            try {
                delegate.closeAndDelete();
            } finally {
                manager.releaseIfDeleted(getChannelID(), bytes);
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
            manager.accountant.recordRead(delegate.getNioFileChannel().position() - position);
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
            long bytes = channelSize(getChannelID());
            try {
                delegate.deleteChannel();
            } finally {
                manager.releaseIfDeleted(getChannelID(), bytes);
            }
        }

        @Override
        public FileChannel getNioFileChannel() {
            return delegate.getNioFileChannel();
        }

        @Override
        public void closeAndDelete() throws IOException {
            long bytes = channelSize(getChannelID());
            try {
                delegate.closeAndDelete();
            } finally {
                manager.releaseIfDeleted(getChannelID(), bytes);
            }
        }
    }
}
