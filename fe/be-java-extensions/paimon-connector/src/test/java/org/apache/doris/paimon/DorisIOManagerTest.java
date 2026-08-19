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
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.memory.Buffer;
import org.apache.paimon.memory.MemorySegment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class DorisIOManagerTest {
    @TempDir
    private Path tempDir;

    @Test
    public void testSpillWriteReadAndDeleteAccounting() throws Exception {
        RecordingSpillAccountant accountant = new RecordingSpillAccountant();
        Path managedSpillPath = tempDir.resolve("query/paimon");
        Assertions.assertFalse(Files.exists(managedSpillPath));
        try (DorisIOManager manager = new DorisIOManager(
                IOManager.create(managedSpillPath.toString()), accountant)) {
            FileIOChannel.ID channel = manager.createChannel();
            Assertions.assertTrue(Files.isDirectory(managedSpillPath));
            Buffer buffer = Buffer.create(MemorySegment.wrap(new byte[16]), 16);

            BufferFileWriter writer = manager.createBufferFileWriter(channel);
            writer.writeBlock(buffer);
            writer.close();

            Assertions.assertEquals(20, accountant.reservedBytes);
            Assertions.assertEquals(20, accountant.currentBytes);
            Assertions.assertEquals(20, accountant.writtenBytes);

            BufferFileReader reader = manager.createBufferFileReader(channel);
            reader.readInto(Buffer.create(MemorySegment.wrap(new byte[16]), 0));
            Assertions.assertEquals(20, accountant.readBytes);
            reader.closeAndDelete();

            Assertions.assertEquals(0, accountant.currentBytes);
            Assertions.assertEquals(20, accountant.releasedBytes);
        }
    }

    @Test
    public void testManagerCloseReleasesUndeletedChannels() throws Exception {
        RecordingSpillAccountant accountant = new RecordingSpillAccountant();
        DorisIOManager manager = new DorisIOManager(IOManager.create(tempDir.toString()), accountant);
        FileIOChannel.ID channel = manager.createChannel();
        BufferFileWriter writer = manager.createBufferFileWriter(channel);
        writer.writeBlock(Buffer.create(MemorySegment.wrap(new byte[8]), 8));
        writer.close();

        manager.close();

        Assertions.assertEquals(0, accountant.currentBytes);
        Assertions.assertEquals(12, accountant.releasedBytes);
    }

    @Test
    public void testManagerCloseFailureKeepsExistingChannelAccounted() throws Exception {
        RecordingSpillAccountant accountant = new RecordingSpillAccountant();
        IOManager failingCloseManager = new IOManagerImpl(tempDir.toString()) {
            @Override
            public void close() throws Exception {
                throw new IOException("injected close failure");
            }
        };
        DorisIOManager manager = new DorisIOManager(failingCloseManager, accountant);
        FileIOChannel.ID channel = manager.createChannel();
        BufferFileWriter writer = manager.createBufferFileWriter(channel);
        writer.writeBlock(Buffer.create(MemorySegment.wrap(new byte[8]), 8));
        writer.close();

        Assertions.assertThrows(IOException.class, manager::close);
        Assertions.assertTrue(channel.getPathFile().exists());
        Assertions.assertEquals(12, accountant.currentBytes);
        Assertions.assertEquals(0, accountant.releasedBytes);

        writer.deleteChannel();
        Assertions.assertEquals(0, accountant.currentBytes);
        Assertions.assertEquals(12, accountant.releasedBytes);
    }

    @Test
    public void testNextReservationReleasesDirectlyDeletedChannel() throws Exception {
        RecordingSpillAccountant accountant = new RecordingSpillAccountant();
        try (DorisIOManager manager = new DorisIOManager(
                IOManager.create(tempDir.toString()), accountant)) {
            FileIOChannel.ID deletedChannel = manager.createChannel();
            BufferFileWriter firstWriter = manager.createBufferFileWriter(deletedChannel);
            firstWriter.writeBlock(Buffer.create(MemorySegment.wrap(new byte[8]), 8));
            firstWriter.close();
            Assertions.assertTrue(deletedChannel.getPathFile().delete());

            FileIOChannel.ID nextChannel = manager.createChannel();
            BufferFileWriter nextWriter = manager.createBufferFileWriter(nextChannel);
            nextWriter.writeBlock(Buffer.create(MemorySegment.wrap(new byte[4]), 4));
            nextWriter.close();

            Assertions.assertEquals(20, accountant.reservedBytes);
            Assertions.assertEquals(8, accountant.currentBytes);
            Assertions.assertEquals(12, accountant.releasedBytes);
        }
    }

    @Test
    public void testFailedReservationReconcilesDirectDeletionAndRetries() throws Exception {
        RecordingSpillAccountant accountant = new RecordingSpillAccountant(16);
        try (DorisIOManager manager = new DorisIOManager(
                IOManager.create(tempDir.toString()), accountant)) {
            FileIOChannel.ID oldChannel = manager.createChannel();
            BufferFileWriter oldWriter = manager.createBufferFileWriter(oldChannel);
            oldWriter.writeBlock(Buffer.create(MemorySegment.wrap(new byte[8]), 8));
            oldWriter.close();

            FileIOChannel.ID nextChannel = manager.createChannel();
            BufferFileWriter nextWriter = manager.createBufferFileWriter(nextChannel);
            Assertions.assertTrue(oldChannel.getPathFile().delete());

            nextWriter.writeBlock(Buffer.create(MemorySegment.wrap(new byte[4]), 4));
            nextWriter.close();

            Assertions.assertEquals(3, accountant.reserveAttempts);
            Assertions.assertEquals(20, accountant.reservedBytes);
            Assertions.assertEquals(8, accountant.currentBytes);
            Assertions.assertEquals(12, accountant.releasedBytes);
        }
    }

    private static final class RecordingSpillAccountant implements DorisIOManager.SpillAccountant {
        private final long limitBytes;
        private long reserveAttempts;
        private long reservedBytes;
        private long currentBytes;
        private long writtenBytes;
        private long readBytes;
        private long releasedBytes;

        private RecordingSpillAccountant() {
            this(Long.MAX_VALUE);
        }

        private RecordingSpillAccountant(long limitBytes) {
            this.limitBytes = limitBytes;
        }

        @Override
        public void reserve(String path, long bytes) throws IOException {
            reserveAttempts++;
            if (currentBytes + bytes > limitBytes) {
                throw new IOException("spill capacity exceeded");
            }
            reservedBytes += bytes;
            currentBytes += bytes;
        }

        @Override
        public void rollback(String path, long bytes) {
            currentBytes -= bytes;
        }

        @Override
        public void commitWrite(String path, long bytes) {
            writtenBytes += bytes;
        }

        @Override
        public void recordRead(String path, long bytes) {
            readBytes += bytes;
        }

        @Override
        public void release(String path, long bytes) {
            releasedBytes += bytes;
            currentBytes -= bytes;
        }
    }
}
