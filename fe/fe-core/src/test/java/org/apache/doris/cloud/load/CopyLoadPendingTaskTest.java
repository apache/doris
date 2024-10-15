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

package org.apache.doris.cloud.load;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.datasource.CloudInternalCatalog;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.ObjectFilePB;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB.Provider;
import org.apache.doris.cloud.stage.StageUtil;
import org.apache.doris.cloud.storage.ListObjectsResult;
import org.apache.doris.cloud.storage.MockRemote;
import org.apache.doris.cloud.storage.ObjectFile;
import org.apache.doris.cloud.storage.RemoteBase;
import org.apache.doris.cloud.storage.RemoteBase.ObjectInfo;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.utframe.TestWithFeService;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CopyLoadPendingTaskTest extends TestWithFeService {

    private static final String STORAGE_PREFIX = "test_prefix";

    private ConnectContext ctx;
    private ObjectInfo objectInfo = new ObjectInfo(Provider.OSS, "test_ak", "test_sk", "test_bucket", "test_endpoint",
            "test_region", STORAGE_PREFIX);
    @Mocked
    RemoteBase remote;
    MockInternalCatalog mockInternalCatalog = new MockInternalCatalog();

    private class MockInternalCatalog extends CloudInternalCatalog {
        @Override
        public List<ObjectFilePB> filterCopyFiles(String stageId, long tableId, List<ObjectFile> objectFiles)
                throws DdlException {
            if (tableId == 200) {
                return objectFiles.stream().filter(f -> !f.getRelativePath().equals("dir1/file_1.csv"))
                        .map(f -> ObjectFilePB.newBuilder().setRelativePath(f.getRelativePath()).setEtag(f.getEtag())
                                .build()).collect(Collectors.toList());
            }
            return objectFiles.stream()
                    .map(f -> ObjectFilePB.newBuilder().setRelativePath(f.getRelativePath()).setEtag(f.getEtag())
                            .build()).collect(Collectors.toList());
        }
    }

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testGlob() {
        String globPrefix = "glob:";
        // file name contains "," which is a special character in Glob
        String fileName = "1,csv";
        Assert.assertEquals(true, matchGlob(fileName, globPrefix + "1,csv"));
        Assert.assertEquals(true, matchGlob(fileName, globPrefix + "1\\,csv"));
        Assert.assertEquals(false, matchGlob(fileName, globPrefix + "{1,csv}"));
        Assert.assertEquals(true, matchGlob(fileName, globPrefix + "{1\\,csv}"));
        Assert.assertEquals(true, matchGlob(fileName, globPrefix + "{1\\,csv,2\\,csv}"));
        fileName = "1?csv";
        String fileName2 = "12csv";
        Assert.assertEquals(true, matchGlob(fileName, globPrefix + "1?csv"));
        Assert.assertEquals(true, matchGlob(fileName2, globPrefix + "1?csv"));
        Assert.assertEquals(true, matchGlob(fileName, globPrefix + "1\\?csv"));
        Assert.assertEquals(false, matchGlob(fileName2, globPrefix + "1\\?csv"));
        Assert.assertEquals(true, matchGlob(fileName, globPrefix + "{1?csv}"));
        Assert.assertEquals(true, matchGlob(fileName2, globPrefix + "{1?csv}"));
        Assert.assertEquals(true, matchGlob(fileName, globPrefix + "{1\\?csv}"));
        Assert.assertEquals(false, matchGlob(fileName2, globPrefix + "{1\\?csv}"));
    }

    private boolean matchGlob(String file, String pattern) {
        PathMatcher pathMatcher = FileSystems.getDefault().getPathMatcher(pattern);
        Path path = Paths.get(file);
        return pathMatcher.matches(path);
    }

    @Test
    public void testParseFileForCopyJob() throws Exception {
        MockRemote mockRemote = new MockRemote(objectInfo);
        ctx = UtFrameUtils.createDefaultCtx();
        List<String> subPrefixes = Lists.newArrayList("", "dir1", "dir2/dir3", "dir4/dir5/dir6");
        // list object files
        for (int i = 0; i < subPrefixes.size(); i++) {
            for (int j = 0; j < i + 1; j++) {
                String relativePath = subPrefixes.get(i) + (subPrefixes.get(i).isEmpty() ? "" : "/") + "file_" + j
                        + ".csv";
                String etag = "";
                ObjectFile objectFile = new ObjectFile(
                        STORAGE_PREFIX + (STORAGE_PREFIX.isEmpty() ? "" : "/") + relativePath, relativePath, etag,
                        (j + 1) * 10);
                mockRemote.addObjectFile(objectFile);
                System.out.println(
                        "object file=" + objectFile.getKey() + ", " + objectFile.getRelativePath() + ", size: "
                                + objectFile.getSize());
            }
        }
        new Expectations(ctx.getEnv(), ctx.getEnv().getInternalCatalog(), remote) {
            {
                Env.getCurrentInternalCatalog();
                minTimes = 0;
                result = mockInternalCatalog;

                RemoteBase.newInstance(objectInfo);
                minTimes = 0;
                result = mockRemote;
            }
        };

        String stageId = "1";
        long tableId = 100;
        long sizeLimit = 0;
        int fileNumLimit = 0;
        int fileMetaSizeLimit = 0;

        CopyLoadPendingTask task = new CopyLoadPendingTask(null, null, null);
        // test pattern
        List<Pair<String, Integer>> patternAndMatchNum = Lists.newArrayList(Pair.of(null, 10), Pair.of("file*csv", 1),
                Pair.of("**/file*csv", 9), Pair.of("file_0.csv", 1), Pair.of("*/file_[0-9].csv", 2));
        for (Pair<String, Integer> pair : patternAndMatchNum) {
            String pattern = pair.first;
            List<Pair<TBrokerFileStatus, ObjectFilePB>> fileStatus = new ArrayList<>();
            task.parseFileForCopyJob(stageId, tableId, "q1", pattern, sizeLimit, fileNumLimit, fileMetaSizeLimit,
                    fileStatus, objectInfo, false);
            Assert.assertEquals(pair.second.intValue(), fileStatus.size());
        }
        // test loaded files is not empty
        do {
            String pattern = null;
            List<Pair<TBrokerFileStatus, ObjectFilePB>> fileStatus = new ArrayList<>();
            task.parseFileForCopyJob(stageId, 200, "q1", pattern, sizeLimit, fileNumLimit, fileMetaSizeLimit,
                    fileStatus, objectInfo, false);
            Assert.assertEquals(9, fileStatus.size());
        } while (false);
        // test size limit
        do {
            String pattern = null;
            List<Pair<TBrokerFileStatus, ObjectFilePB>> fileStatus = new ArrayList<>();
            task.parseFileForCopyJob(stageId, tableId, "q1", pattern, 100, fileNumLimit, fileMetaSizeLimit, fileStatus,
                    objectInfo, false);
            Assert.assertEquals(10, fileStatus.size()); // 4, files limit are filtered in begin_copy
        } while (false);
        // test file num limit
        do {
            String pattern = null;
            List<Pair<TBrokerFileStatus, ObjectFilePB>> fileStatus = new ArrayList<>();
            task.parseFileForCopyJob(stageId, tableId, "q1", pattern, sizeLimit, 6, fileMetaSizeLimit, fileStatus,
                    objectInfo, false);
            Assert.assertEquals(10, fileStatus.size()); // 6
        } while (false);
        // test file meta size limit
        do {
            String pattern = null;
            List<Pair<TBrokerFileStatus, ObjectFilePB>> fileStatus = new ArrayList<>();
            task.parseFileForCopyJob(stageId, tableId, "q1", pattern, sizeLimit, fileNumLimit, 60, fileStatus,
                    objectInfo, false);
            Assert.assertEquals(10, fileStatus.size()); // 2
        } while (false);
        // test size and file num limit
        do {
            String pattern = null;
            List<Pair<TBrokerFileStatus, ObjectFilePB>> fileStatus = new ArrayList<>();
            task.parseFileForCopyJob(stageId, tableId, "q1", pattern, 100, fileNumLimit, fileMetaSizeLimit, fileStatus,
                    objectInfo, false);
            Assert.assertEquals(10, fileStatus.size()); // 4
        } while (false);
    }

    @Ignore
    @Test
    public void testContinuationToken() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        String prefix = "prefix1";
        List<String> subPrefixes = Lists.newArrayList("", "dir1", "dir2/dir3", "dir4/dir5/dir6");
        // list object files
        List<ObjectFile> objectFiles = new ArrayList<>();
        for (int i = 0; i < subPrefixes.size(); i++) {
            for (int j = 0; j < i + 1; j++) {
                String relativePath = subPrefixes.get(i) + (subPrefixes.get(i).isEmpty() ? "" : "/") + "file_" + j
                        + ".csv";
                String etag = "";
                ObjectFile objectFile = new ObjectFile(prefix + (prefix.isEmpty() ? "" : "/") + relativePath,
                        relativePath, etag, (j + 1) * 10);
                objectFiles.add(objectFile);
                System.out.println(
                        "object file=" + objectFile.getKey() + ", " + objectFile.getRelativePath() + ", size: "
                                + objectFile.getSize());
            }
        }
        ListObjectsResult listObjectsResult = new ListObjectsResult(objectFiles, true, "abc");
        ListObjectsResult listObjectsResult2 = new ListObjectsResult(objectFiles, false, null);
        // loading or loaded files
        List<Cloud.ObjectFilePB> files = new ArrayList<>();
        new Expectations(ctx.getEnv(), ctx.getEnv().getInternalCatalog(), remote) {
            {
                ((CloudInternalCatalog) Env.getCurrentInternalCatalog()).getCopyFiles(anyString, 100);
                minTimes = 0;
                result = files;

                RemoteBase.newInstance(objectInfo);
                minTimes = 0;
                result = remote;

                remote.listObjects(null);
                minTimes = 0;
                result = listObjectsResult;

                remote.listObjects("abc");
                minTimes = 0;
                result = listObjectsResult2;
            }
        };

        String stageId = "1";
        long tableId = 100;
        long sizeLimit = 0;
        int fileNumLimit = 0;
        int fileMetaSizeLimit = 0;

        CopyLoadPendingTask task = new CopyLoadPendingTask(null, null, null);
        // test pattern
        List<Pair<String, Integer>> patternAndMatchNum = Lists.newArrayList(Pair.of(null, 10), Pair.of("file*csv", 1),
                Pair.of("**/file*csv", 9), Pair.of("file_0.csv", 1), Pair.of("*/file_[0-9].csv", 2));
        for (Pair<String, Integer> pair : patternAndMatchNum) {
            String pattern = pair.first;
            List<Pair<TBrokerFileStatus, ObjectFilePB>> fileStatus = new ArrayList<>();
            task.parseFileForCopyJob(stageId, tableId, "q1", pattern, sizeLimit, fileNumLimit, fileMetaSizeLimit,
                    fileStatus, objectInfo, false);
            Assert.assertTrue("expected: " + pair.second * 2 + ", real: " + fileStatus.size(),
                    pair.second * 2 == fileStatus.size());
        }
    }

    @Test
    public void testParseFileForCopyJobV2() throws Exception {
        MockRemote mockRemote = new MockRemote(objectInfo);
        ctx = UtFrameUtils.createDefaultCtx();
        // add object files
        List<String> subPrefixes = Lists.newArrayList("", "dir1", "dir2", "dir3/dir4");
        for (int i = 0; i < subPrefixes.size(); i++) {
            String subPrefix = subPrefixes.get(i);
            for (int j = 0; j < 10; j++) {
                String relativePath = subPrefix + (subPrefix.isEmpty() ? "" : "/") + "file" + j + ".csv";
                ObjectFile objectFile = new ObjectFile(STORAGE_PREFIX + "/" + relativePath, relativePath, "",
                        (j + 1) * 10);
                mockRemote.addObjectFile(objectFile);
                System.out.println("Add " + objectFile);
            }
        }
        // file with special filename
        List<String> specialNames = Lists.newArrayList("sf,csv", "sd/sf,csv", "sf?csv", "sd/sf?csv", "sf*csv", "sf-csv",
                "sf[csv", "sf]csv", "sf{csv", "sf}csv");
        for (String specialName : specialNames) {
            ObjectFile objectFile = new ObjectFile(STORAGE_PREFIX + "/" + specialName, specialName, "", 1);
            mockRemote.addObjectFile(objectFile);
        }

        new Expectations(ctx.getEnv(), ctx.getEnv().getInternalCatalog(), remote) {
            {
                Env.getCurrentInternalCatalog();
                minTimes = 0;
                result = mockInternalCatalog;

                RemoteBase.newInstance(objectInfo);
                minTimes = 0;
                result = mockRemote;
            }
        };

        String stageId = "1";
        long tableId = 300;
        long sizeLimit = 0;
        int fileNumLimit = 0;
        int fileMetaSizeLimit = 0;

        CopyLoadPendingTask task = new CopyLoadPendingTask(null, null, null);
        // --------- glob without wildcard ---------
        // test pattern
        List<Pair<String, Integer>> patternAndMatchNum = Lists.newArrayList(Pair.of(null, 40 + specialNames.size()));
        // single file
        patternAndMatchNum.addAll(Lists.newArrayList(Pair.of("file1.csv", 1), Pair.of("{file1.csv}", 1)));
        // single file with directory
        patternAndMatchNum.addAll(Lists.newArrayList(Pair.of("dir1/file1.csv", 1), Pair.of("{dir1/file1.csv}", 1),
                Pair.of("dir1/{file1.csv}", 1), Pair.of("dir1{/file1.csv}", 1)));
        // multiple file
        patternAndMatchNum.addAll(
                Lists.newArrayList(Pair.of("{file1.csv,file2.csv}", 2), Pair.of("{file1.csv,dir1/file1.csv}", 2),
                        Pair.of("dir1/{file1.csv,file2.csv}", 2), Pair.of("dir1{/file1.csv,/file2.csv}", 2),
                        Pair.of("{dir1,dir2}/file1.csv", 2)));
        // --------- glob with wildcard ---------
        patternAndMatchNum.addAll(Lists.newArrayList(Pair.of("file*csv", 10), Pair.of("*file1.csv", 1)));
        patternAndMatchNum.addAll(Lists.newArrayList(Pair.of("dir*/file1.csv", 2), Pair.of("dir**file1.csv", 3)));
        patternAndMatchNum.addAll(Lists.newArrayList(Pair.of("dir1/file*csv", 10), Pair.of("dir1/*file1.csv", 1),
                Pair.of("dir1/file?.csv", 10), Pair.of("dir1/file[1234].csv", 4), Pair.of("dir1/file[1-4].csv", 4),
                Pair.of("dir1/file[1-4,6-9].csv", 8)));
        // --------- glob with escape character ---------
        patternAndMatchNum.addAll(
                Lists.newArrayList(Pair.of("sf,csv", 1), Pair.of("sf\\,csv", 1), Pair.of("{sf,csv}", 0),
                        Pair.of("{sf\\,csv}", 1), Pair.of("sd/sf,csv", 1), Pair.of("{sd/sf,csv}", 0),
                        Pair.of("{sd/sf\\,csv}", 1), Pair.of("sd/{sf\\,csv}", 1)));
        patternAndMatchNum.addAll(
                Lists.newArrayList(Pair.of("sf?csv", 8), Pair.of("sf\\?csv", 1), Pair.of("{sf?csv}", 8),
                        Pair.of("{sf\\?csv}", 1)));
        patternAndMatchNum.addAll(Lists.newArrayList(Pair.of("sf*csv", 8), Pair.of("sf\\*csv", 1),
                Pair.of("{sf*csv}", 8), Pair.of("{sf\\*csv}", 1)));
        patternAndMatchNum.addAll(
                Lists.newArrayList(Pair.of("sf-csv", 1), Pair.of("sf\\-csv", 1), Pair.of("{sf-csv}", 1),
                        Pair.of("{sf\\-csv}", 1)));
        patternAndMatchNum.addAll(Lists.newArrayList(/*"file[csv",*/ Pair.of("sf\\[csv", 1)));
        patternAndMatchNum.addAll(Lists.newArrayList(Pair.of("sf]csv", 1), Pair.of("sf\\]csv", 1)));
        patternAndMatchNum.addAll(Lists.newArrayList(/*"file{csv",*/ Pair.of("sf\\{csv", 1)));
        patternAndMatchNum.addAll(Lists.newArrayList(Pair.of("sf}csv", 1), Pair.of("sf\\}csv", 1)));

        for (Pair<String, Integer> pair : patternAndMatchNum) {
            String pattern = pair.first;
            List<Pair<String, Boolean>> globList = StageUtil.analyzeGlob("q1", pattern);
            System.out.println("\nglob: " + pattern + ", size: " + globList.size());
            for (Pair<String, Boolean> glob1 : globList) {
                System.out.println("----: " + glob1.first + ", " + glob1.second);
            }

            List<Pair<TBrokerFileStatus, ObjectFilePB>> fileStatus = new ArrayList<>();
            task.parseFileForCopyJob(stageId, tableId, "q1", pattern, sizeLimit, fileNumLimit, fileMetaSizeLimit,
                    fileStatus, objectInfo, false);
            Assert.assertEquals("pattern=" + pattern, pair.second.intValue(), fileStatus.size());
        }
    }
}
