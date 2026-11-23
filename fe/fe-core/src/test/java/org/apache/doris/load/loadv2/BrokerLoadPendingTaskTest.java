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

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.property.fileformat.AvroFileFormatProperties;
import org.apache.doris.datasource.property.fileformat.DeferredFileFormatProperties;
import org.apache.doris.datasource.property.fileformat.FileFormatProperties;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.Location;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import org.apache.doris.thrift.TBrokerFileStatus;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BrokerLoadPendingTaskTest {

    /** Minimal FileSystem stub that returns a fixed listing for any location. */
    private static final class ListingFileSystem implements org.apache.doris.filesystem.FileSystem {
        private final List<FileEntry> entries;

        ListingFileSystem(long fileSize) {
            this(List.of(new FileEntry(Location.of("s3://bucket/rows.csv"), fileSize, false, 0L, null)));
        }

        ListingFileSystem(List<FileEntry> entries) {
            this.entries = entries;
        }

        @Override
        public FileIterator list(Location location) {
            Iterator<FileEntry> iterator = entries.iterator();
            return new FileIterator() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public FileEntry next() {
                    return iterator.next();
                }

                @Override
                public void close() {
                }
            };
        }

        @Override
        public boolean exists(Location location) {
            return true;
        }

        @Override
        public void mkdirs(Location location) {
        }

        @Override
        public void delete(Location location, boolean recursive) {
        }

        @Override
        public void deleteFiles(Collection<Location> locations) {
        }

        @Override
        public void rename(Location src, Location dst) {
        }

        @Override
        public org.apache.doris.filesystem.DorisInputFile newInputFile(Location location) {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.apache.doris.filesystem.DorisOutputFile newOutputFile(Location location) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void inferredAndExplicitAvroIgnoreZeroByteFiles() throws Exception {
        for (boolean inferred : new boolean[] {false, true}) {
            for (boolean onlyEmpty : new boolean[] {false, true}) {
                BrokerFileGroup group = Deencapsulation.newInstance(BrokerFileGroup.class);
                FileFormatProperties properties = inferred
                        ? new DeferredFileFormatProperties() : new AvroFileFormatProperties();
                properties.analyzeFileFormatProperties(Maps.newHashMap(), false);
                Deencapsulation.setField(group, "fileFormatProperties", properties);
                Deencapsulation.setField(group, "filePaths", List.of("s3://bucket/*.avro"));
                List<FileEntry> entries = Lists.newArrayList(
                        new FileEntry(Location.of("s3://bucket/empty.avro"), 0, false, 0L, null));
                if (!onlyEmpty) {
                    entries.add(new FileEntry(Location.of("s3://bucket/rows.avro"), 100, false, 0L, null));
                }
                Env env = Mockito.mock(Env.class);
                BrokerDesc desc = Mockito.mock(BrokerDesc.class);
                try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
                        MockedStatic<FileSystemFactory> fsMock = Mockito.mockStatic(FileSystemFactory.class)) {
                    envMock.when(Env::getCurrentEnv).thenReturn(env);
                    fsMock.when(() -> FileSystemFactory.getFileSystem(Mockito.any(BrokerDesc.class)))
                            .thenReturn(new ListingFileSystem(entries));
                    FileGroupAggKey key = new FileGroupAggKey(1L, null);
                    BrokerLoadPendingTask task = new BrokerLoadPendingTask(Mockito.mock(BrokerLoadJob.class),
                            Map.of(key, List.of(group)), desc, LoadTask.Priority.NORMAL);
                    task.executeTask();
                    BrokerPendingTaskAttachment attachment = Deencapsulation.getField(task, "attachment");
                    Assertions.assertEquals(onlyEmpty ? 0 : 1, attachment.getFileNumByTable(key),
                            "inferred=" + inferred + ", onlyEmpty=" + onlyEmpty);
                    List<TBrokerFileStatus> statuses =
                            attachment.getFileStatusByTable(key).get(0);
                    if (!onlyEmpty) {
                        Assertions.assertEquals("s3://bucket/rows.avro", statuses.get(0).getPath());
                    }
                }
            }
        }
    }

    @Test
    public void testExecuteTask() throws UserException {
        BrokerLoadJob brokerLoadJob = Mockito.mock(BrokerLoadJob.class);
        BrokerFileGroup brokerFileGroup = Mockito.mock(BrokerFileGroup.class);
        BrokerDesc brokerDesc = Mockito.mock(BrokerDesc.class);
        Env env = Mockito.mock(Env.class);

        MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
        MockedStatic<FileSystemFactory> mockedFsFactory = Mockito.mockStatic(FileSystemFactory.class);
        try {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getNextId()).thenReturn(1L);
            Mockito.when(brokerFileGroup.getFilePaths())
                    .thenReturn(Lists.newArrayList("hdfs://localhost:8900/test_column"));

            mockedFsFactory.when(() -> FileSystemFactory.getFileSystem(Mockito.any(BrokerDesc.class)))
                    .thenReturn(new ListingFileSystem(1L));

            Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToFileGroups = Maps.newHashMap();
            List<BrokerFileGroup> brokerFileGroups = Lists.newArrayList();
            brokerFileGroups.add(brokerFileGroup);
            FileGroupAggKey aggKey = new FileGroupAggKey(1L, null);
            aggKeyToFileGroups.put(aggKey, brokerFileGroups);

            BrokerLoadPendingTask brokerLoadPendingTask = new BrokerLoadPendingTask(brokerLoadJob,
                    aggKeyToFileGroups, brokerDesc, LoadTask.Priority.NORMAL);
            brokerLoadPendingTask.executeTask();
            BrokerPendingTaskAttachment brokerPendingTaskAttachment =
                    Deencapsulation.getField(brokerLoadPendingTask, "attachment");
            Assertions.assertEquals(1, brokerPendingTaskAttachment.getFileNumByTable(aggKey));
            Assertions.assertEquals(1L,
                    brokerPendingTaskAttachment.getFileStatusByTable(aggKey).get(0).get(0).size);
        } finally {
            mockedFsFactory.close();
            mockedEnv.close();
        }
    }
}
