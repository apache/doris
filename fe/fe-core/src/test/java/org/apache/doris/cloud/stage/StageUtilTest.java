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

import org.apache.doris.cloud.proto.Cloud.ObjectFilePB;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB.Provider;
import org.apache.doris.cloud.storage.ListObjectsResult;
import org.apache.doris.cloud.storage.ObjectFile;
import org.apache.doris.cloud.storage.OssRemote;
import org.apache.doris.cloud.storage.RemoteBase.ObjectInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.thrift.TBrokerFileStatus;

import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
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

    @Ignore
    @Test
    public void testListAndFilterFilesV2() throws Exception {
        List<String> keys = readMockedOssUrl();
        Assert.assertEquals(4956, keys.size());
        new MockUp<InternalCatalog>(InternalCatalog.class) {
            @Mock
            public List<ObjectFilePB> filterCopyFiles(String stageId, long tableId, List<ObjectFile> objectFiles)
                    throws DdlException {
                List<ObjectFilePB> objectFilePbs = new ArrayList<ObjectFilePB>();
                for (ObjectFile objectFile : objectFiles) {
                    objectFilePbs.add(ObjectFilePB.newBuilder().setRelativePath(objectFile.getRelativePath())
                            .setEtag(objectFile.getEtag()).setSize(objectFile.getSize()).build());
                }
                return objectFilePbs;
            }
        };

        new MockUp<OssRemote>(OssRemote.class) {
            @Mock
            public ListObjectsResult listObjects(String prefix, String continuationToken) throws DdlException {
                List<ObjectFile> objectFiles = new ArrayList<>();
                for (String key : keys) {
                    objectFiles.add(new ObjectFile(key, key, "7741995E5B849F911ADF926A4C5747D3-3", 100));
                }
                return new ListObjectsResult(objectFiles, false, "");
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
        Assert.assertEquals(400, fileStatus.size());
    }
}
