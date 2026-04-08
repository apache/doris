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

package org.apache.doris.cloud.stage;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.datasource.CloudInternalCatalog;
import org.apache.doris.cloud.proto.Cloud.ObjectFilePB;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB.Provider;
import org.apache.doris.cloud.storage.ObjectInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.filesystem.spi.ObjFileSystem;
import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.thrift.TBrokerFileStatus;

import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class StageUtilTest {
    public static final Logger LOG = LogManager.getLogger(StageUtilTest.class);

    public List<String> readMockedOssUrl() throws Exception {
        List<String> keys = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(StageUtilTest.class.getResourceAsStream("/stageUtilTest.txt")))) {
            String line = "";
            while ((line = reader.readLine()) != null) {
                keys.add(line);
            }
        } catch (Exception e) {
            LOG.warn("readMockedOssUrl:", e);
            throw e;
        }
        LOG.info("keys.size():{}", keys.size());
        return keys;
    }

    @Test
    public void testListAndFilterFilesV2() throws Exception {
        List<String> keys = readMockedOssUrl();
        Assert.assertEquals(4956, keys.size());

        // Mock FileSystemFactory to return an ObjFileSystem stub.
        // The MockUp<ObjFileSystem> below will replace listObjectsWithPrefix and close.
        new MockUp<FileSystemFactory>() {
            @Mock
            public org.apache.doris.filesystem.FileSystem getFileSystem(StorageProperties sp) throws IOException {
                return new ObjFileSystem("test", null) {
                    @Override
                    public org.apache.doris.filesystem.FileIterator list(
                            org.apache.doris.filesystem.Location location) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void mkdirs(org.apache.doris.filesystem.Location location) {
                    }

                    @Override
                    public void delete(org.apache.doris.filesystem.Location location, boolean r) {
                    }

                    @Override
                    public void rename(org.apache.doris.filesystem.Location s,
                            org.apache.doris.filesystem.Location d) {
                    }

                    @Override
                    public org.apache.doris.filesystem.DorisInputFile newInputFile(
                            org.apache.doris.filesystem.Location location) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public org.apache.doris.filesystem.DorisOutputFile newOutputFile(
                            org.apache.doris.filesystem.Location location) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void close() {
                    }
                };
            }
        };

        // Mock ObjFileSystem.listObjectsWithPrefix to return all test keys
        new MockUp<ObjFileSystem>() {
            @Mock
            public RemoteObjects listObjectsWithPrefix(String prefix, String subPrefix, String continuationToken)
                    throws IOException {
                List<RemoteObject> objectFiles = new ArrayList<>();
                for (String key : keys) {
                    objectFiles.add(new RemoteObject(key, key, "fake-etag-for-test", 100, 0));
                }
                return new RemoteObjects(objectFiles, false, null);
            }

            @Mock
            public void close() throws IOException {
                // no-op
            }
        };

        // Mock CloudInternalCatalog.filterCopyFiles to pass through all files
        new MockUp<CloudInternalCatalog>() {
            @Mock
            public List<ObjectFilePB> filterCopyFiles(String stageId, long tableId, List<RemoteObject> objectFiles)
                    throws DdlException {
                List<ObjectFilePB> objectFilePbs = new ArrayList<ObjectFilePB>();
                for (RemoteObject objectFile : objectFiles) {
                    objectFilePbs.add(ObjectFilePB.newBuilder().setRelativePath(objectFile.getRelativePath())
                            .setEtag(objectFile.getEtag()).setSize(objectFile.getSize()).build());
                }
                return objectFilePbs;
            }
        };

        // Mock Env.getCurrentInternalCatalog to return a CloudInternalCatalog
        new MockUp<Env>() {
            @Mock
            public InternalCatalog getCurrentInternalCatalog() {
                return new CloudInternalCatalog();
            }
        };

        String copyId = "copyId";
        String filePattern = "mc_holo/ext_traditional/jd_09/item_part1*.orc";
        String stageId = "stageId";
        long tableId = 1;
        long sizeLimit = 100000000;

        ObjectInfo objectInfo = new ObjectInfo(Provider.OSS, "test_ak", "test_sk", "test_bucket",
                "test_endpoint", "test_region", "test_prefix");
        List<Pair<TBrokerFileStatus, ObjectFilePB>> fileStatus = new ArrayList();

        Triple<Integer, Integer, String> triple = StageUtil.listAndFilterFilesV2(
                objectInfo, filePattern, copyId, stageId, tableId, false, sizeLimit,
                1000, Config.max_meta_size_per_copy_into_job, fileStatus);
        LOG.info("triple:{}, fileStatus.size():{}", triple, fileStatus.size());
        // All 4956 test keys match the pattern, but the meta size limit (51200 bytes)
        // caps the result at 500 files (5 batches of cloud_filter_copy_file_num_limit=100).
        Assert.assertEquals(500, fileStatus.size());
    }
}
