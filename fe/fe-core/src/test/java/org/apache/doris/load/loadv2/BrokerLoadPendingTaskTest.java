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
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.Location;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.BrokerFileGroupAggInfo.FileGroupAggKey;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class BrokerLoadPendingTaskTest {

    /** Minimal FileSystem stub that lists a single file entry for any location. */
    private static final class SingleFileSystem implements org.apache.doris.filesystem.FileSystem {
        private final long fileSize;

        SingleFileSystem(long fileSize) {
            this.fileSize = fileSize;
        }

        @Override
        public FileIterator list(Location location) {
            FileEntry entry = new FileEntry(location, fileSize, false, 0L, null);
            return new FileIterator() {
                boolean consumed = false;

                @Override
                public boolean hasNext() {
                    return !consumed;
                }

                @Override
                public FileEntry next() {
                    consumed = true;
                    return entry;
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
    public void testExecuteTask(@Injectable BrokerLoadJob brokerLoadJob,
                                @Injectable BrokerFileGroup brokerFileGroup,
                                @Injectable BrokerDesc brokerDesc,
                                @Mocked Env env) throws UserException {
        Map<FileGroupAggKey, List<BrokerFileGroup>> aggKeyToFileGroups = Maps.newHashMap();
        List<BrokerFileGroup> brokerFileGroups = Lists.newArrayList();
        brokerFileGroups.add(brokerFileGroup);
        FileGroupAggKey aggKey = new FileGroupAggKey(1L, null);
        aggKeyToFileGroups.put(aggKey, brokerFileGroups);
        new Expectations() {
            {
                env.getNextId();
                result = 1L;
                brokerFileGroup.getFilePaths();
                result = "hdfs://localhost:8900/test_column";
            }
        };

        new MockUp<FileSystemFactory>() {
            @Mock
            public org.apache.doris.filesystem.FileSystem getFileSystem(BrokerDesc desc) {
                return new SingleFileSystem(1L);
            }
        };

        BrokerLoadPendingTask brokerLoadPendingTask = new BrokerLoadPendingTask(brokerLoadJob, aggKeyToFileGroups, brokerDesc, LoadTask.Priority.NORMAL);
        brokerLoadPendingTask.executeTask();
        BrokerPendingTaskAttachment brokerPendingTaskAttachment = Deencapsulation.getField(brokerLoadPendingTask, "attachment");
        Assert.assertEquals(1, brokerPendingTaskAttachment.getFileNumByTable(aggKey));
        Assert.assertEquals(1L, brokerPendingTaskAttachment.getFileStatusByTable(aggKey).get(0).get(0).size);
    }
}
