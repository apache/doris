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
    public void testSpillDirectoryIsLazyAndVisibleIoIsAccounted() throws Exception {
        Path managedSpillPath = tempDir.resolve("query/paimon");
        RecordingSpillAccountant accountant = new RecordingSpillAccountant(managedSpillPath);
        Assertions.assertFalse(Files.exists(managedSpillPath));

        try (DorisIOManager manager = new DorisIOManager(accountant)) {
            Assertions.assertEquals(0, accountant.directoryRequests);
            Assertions.assertFalse(Files.exists(managedSpillPath));

            FileIOChannel.ID channel = manager.createChannel();
            Assertions.assertEquals(1, accountant.directoryRequests);
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
    public void testCloseBeforeUseDoesNotRequestSpillDirectory() throws Exception {
        Path managedSpillPath = tempDir.resolve("query/paimon");
        RecordingSpillAccountant accountant = new RecordingSpillAccountant(managedSpillPath);

        new DorisIOManager(accountant).close();

        Assertions.assertEquals(0, accountant.directoryRequests);
        Assertions.assertFalse(Files.exists(managedSpillPath));
    }

    @Test
    public void testDirectDeletionIsConservativelyAccountedUntilSessionEnds() throws Exception {
        RecordingSpillAccountant accountant = new RecordingSpillAccountant(tempDir);
        try (DorisIOManager manager = new DorisIOManager(accountant)) {
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
            Assertions.assertEquals(20, accountant.currentBytes);
            Assertions.assertEquals(0, accountant.releasedBytes);
        }
    }

    @Test
    public void testWriteFailureRollsBackWhenChannelIsDeleted() throws Exception {
        RecordingSpillAccountant accountant = new RecordingSpillAccountant(tempDir);
        IOManager delegate = IOManager.create(tempDir.toString());
        FileIOChannel.ID channel = delegate.createChannel();
        BufferFileWriter closedWriter = delegate.createBufferFileWriter(channel);
        closedWriter.close();
        IOManager failingWriteManager = new IOManagerImpl(tempDir.toString()) {
            @Override
            public BufferFileWriter createBufferFileWriter(FileIOChannel.ID ignored) {
                return closedWriter;
            }
        };

        try (DorisIOManager manager = new DorisIOManager(failingWriteManager, accountant)) {
            BufferFileWriter writer = manager.createBufferFileWriter(channel);
            Assertions.assertThrows(IOException.class,
                    () -> writer.writeBlock(Buffer.create(MemorySegment.wrap(new byte[8]), 8)));
            Assertions.assertFalse(channel.getPathFile().exists());
            Assertions.assertEquals(12, accountant.reservedBytes);
            Assertions.assertEquals(0, accountant.currentBytes);
        } finally {
            delegate.close();
        }
    }

    @Test
    public void testManagerCloseFailureKeepsExistingChannelAccounted() throws Exception {
        RecordingSpillAccountant accountant = new RecordingSpillAccountant(tempDir);
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

        writer.deleteChannel();
        Assertions.assertEquals(0, accountant.currentBytes);
        Assertions.assertEquals(12, accountant.releasedBytes);
    }

    private static final class RecordingSpillAccountant implements DorisIOManager.SpillAccountant {
        private final Path spillDirectory;
        private long directoryRequests;
        private long reservedBytes;
        private long currentBytes;
        private long writtenBytes;
        private long readBytes;
        private long releasedBytes;

        private RecordingSpillAccountant(Path spillDirectory) {
            this.spillDirectory = spillDirectory;
        }

        @Override
        public String getSpillDirectory() {
            directoryRequests++;
            return spillDirectory.toString();
        }

        @Override
        public void reserve(long bytes) {
            reservedBytes += bytes;
            currentBytes += bytes;
        }

        @Override
        public void rollback(long bytes) {
            currentBytes -= bytes;
        }

        @Override
        public void commitWrite(long bytes) {
            writtenBytes += bytes;
        }

        @Override
        public void recordRead(long bytes) {
            readBytes += bytes;
        }

        @Override
        public void release(long bytes) {
            releasedBytes += bytes;
            currentBytes -= bytes;
        }
    }
}
