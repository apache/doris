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
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/** Paimon IOManager adapter which charges temporary I/O to Doris spill management. */
final class DorisIOManager implements IOManager {
    interface SpillAccountant {
        String[] getSpillDirectories() throws IOException;

        void reserve(String path, long bytes) throws IOException;

        void rollback(String path, long bytes);

        void commitWrite(String path, long bytes);

        void recordRead(String path, long bytes);

        void release(String path, long bytes);

        void reconcile(boolean allowRelease) throws IOException;
    }

    static final class SpillDirectoryCleanupException extends IOException {
        private SpillDirectoryCleanupException(Exception cause) {
            super("Failed to eagerly clean a Paimon spill directory", cause);
        }
    }

    private static final long RAW_FILE_RECONCILE_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(5);
    private final SpillAccountant accountant;
    private final LongSupplier nanoTime;
    private final long rawFileReconcileIntervalNanos;
    private final AtomicLong lastRawFileReconcileNanos = new AtomicLong(Long.MIN_VALUE);
    private final Map<String, Long> channelBytes = new ConcurrentHashMap<>();
    private final Map<String, Integer> activeChannelWriters = new ConcurrentHashMap<>();
    private volatile IOManager delegate;

    static DorisIOManager create(long nativeSpillSession) {
        return new DorisIOManager(new NativeSpillAccountant(nativeSpillSession));
    }

    DorisIOManager(SpillAccountant accountant) {
        this(null, accountant, System::nanoTime, RAW_FILE_RECONCILE_INTERVAL_NANOS);
    }

    DorisIOManager(IOManager delegate, SpillAccountant accountant) {
        this(delegate, accountant, System::nanoTime, RAW_FILE_RECONCILE_INTERVAL_NANOS);
    }

    DorisIOManager(IOManager delegate, SpillAccountant accountant,
            LongSupplier nanoTime, long rawFileReconcileIntervalNanos) {
        this.accountant = accountant;
        this.delegate = delegate;
        this.nanoTime = nanoTime;
        this.rawFileReconcileIntervalNanos = rawFileReconcileIntervalNanos;
    }

    private IOManager delegate() throws IOException {
        if (delegate == null) {
            synchronized (this) {
                if (delegate == null) {
                    String[] spillDirectories = accountant.getSpillDirectories();
                    if (spillDirectories == null || spillDirectories.length == 0) {
                        throw new IOException("Doris spill manager returned no available directories");
                    }
                    delegate = IOManager.create(spillDirectories);
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
        // ExternalBuffer clears old channels with File.delete(), bypassing IOManager deletion.
        // Reconcile once per writer so those files do not retain Doris spill quota indefinitely.
        releaseDeletedChannels();
        return new AccountingBufferFileWriter(delegate().createBufferFileWriter(channelID), this);
    }

    @Override
    public BufferFileReader createBufferFileReader(FileIOChannel.ID channelID) throws IOException {
        return new AccountingBufferFileReader(delegate().createBufferFileReader(channelID), this);
    }

    @Override
    public void close() throws Exception {
        IOManager initializedDelegate = delegate;
        if (initializedDelegate == null) {
            return;
        }

        Exception cleanupFailure = null;
        try {
            initializedDelegate.close();
        } catch (Exception e) {
            cleanupFailure = e;
        }
        releaseDeletedChannels();
        try {
            // Writer, compaction and global-index resources are already quiescent. This exact pass
            // can safely release disappeared raw files as well as charging any residual files.
            accountant.reconcile(true);
        } catch (IOException reconciliationFailure) {
            if (cleanupFailure != null) {
                reconciliationFailure.addSuppressed(cleanupFailure);
            }
            throw reconciliationFailure;
        }
        if (cleanupFailure != null) {
            throw new SpillDirectoryCleanupException(cleanupFailure);
        }
    }

    void reconcileIfDue() throws IOException {
        if (delegate == null) {
            return;
        }
        long now = nanoTime.getAsLong();
        while (true) {
            long previous = lastRawFileReconcileNanos.get();
            if (previous != Long.MIN_VALUE
                    && now - previous < rawFileReconcileIntervalNanos) {
                return;
            }
            if (lastRawFileReconcileNanos.compareAndSet(previous, now)) {
                try {
                    reconcileNow(false);
                } catch (IOException e) {
                    lastRawFileReconcileNanos.compareAndSet(now, previous);
                    throw e;
                }
                return;
            }
        }
    }

    void reconcileNow(boolean allowRelease) throws IOException {
        releaseDeletedChannels();
        accountant.reconcile(allowRelease);
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
        activeChannelWriters.computeIfPresent(channelID.getPath(),
                (ignored, writers) -> writers == 1 ? null : writers - 1);
    }

    private void releaseChannel(FileIOChannel.ID channelID) {
        Long released = channelBytes.remove(channelID.getPath());
        if (released != null) {
            accountant.release(channelID.getPath(), released);
        }
    }

    private static final class NativeSpillAccountant implements SpillAccountant {
        private final long nativeSpillSession;

        private NativeSpillAccountant(long nativeSpillSession) {
            this.nativeSpillSession = nativeSpillSession;
        }

        @Override
        public String[] getSpillDirectories() throws IOException {
            return PaimonJniWriter.getPaimonSpillDirectories(nativeSpillSession);
        }

        @Override
        public void reserve(String path, long bytes) throws IOException {
            PaimonJniWriter.reservePaimonSpill(nativeSpillSession, path, bytes);
        }

        @Override
        public void rollback(String path, long bytes) {
            PaimonJniWriter.updatePaimonSpillAccounting(
                    nativeSpillSession, path, -bytes, 0, 0);
        }

        @Override
        public void commitWrite(String path, long bytes) {
            PaimonJniWriter.updatePaimonSpillAccounting(
                    nativeSpillSession, path, 0, bytes, 0);
        }

        @Override
        public void recordRead(String path, long bytes) {
            PaimonJniWriter.updatePaimonSpillAccounting(
                    nativeSpillSession, path, 0, 0, bytes);
        }

        @Override
        public void release(String path, long bytes) {
            PaimonJniWriter.updatePaimonSpillAccounting(
                    nativeSpillSession, path, -bytes, 0, 0);
        }

        @Override
        public void reconcile(boolean allowRelease) throws IOException {
            PaimonJniWriter.reconcilePaimonSpill(nativeSpillSession, allowRelease);
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
            } catch (IOException | RuntimeException writeFailure) {
                try {
                    delegate.closeAndDelete();
                } catch (IOException | RuntimeException cleanupFailure) {
                    writeFailure.addSuppressed(cleanupFailure);
                }
                if (!getChannelID().getPathFile().exists()) {
                    manager.releaseChannel(getChannelID());
                }
                throw writeFailure;
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
            try {
                delegate.deleteChannel();
            } finally {
                if (!getChannelID().getPathFile().exists()) {
                    manager.releaseChannel(getChannelID());
                }
            }
        }

        @Override
        public FileChannel getNioFileChannel() {
            return delegate.getNioFileChannel();
        }

        @Override
        public void closeAndDelete() throws IOException {
            try {
                delegate.closeAndDelete();
            } finally {
                if (!getChannelID().getPathFile().exists()) {
                    manager.releaseChannel(getChannelID());
                }
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
            try {
                delegate.deleteChannel();
            } finally {
                if (!getChannelID().getPathFile().exists()) {
                    manager.releaseChannel(getChannelID());
                }
            }
        }

        @Override
        public FileChannel getNioFileChannel() {
            return delegate.getNioFileChannel();
        }

        @Override
        public void closeAndDelete() throws IOException {
            try {
                delegate.closeAndDelete();
            } finally {
                if (!getChannelID().getPathFile().exists()) {
                    manager.releaseChannel(getChannelID());
                }
            }
        }
    }
}
