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

package org.apache.doris.backup;

import mockit.*;
import org.apache.doris.analysis.ShowRepositoriesStmt;
import org.apache.doris.catalog.BrokerMgr;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.service.FrontendOptions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;


public class RepositoryTest {

    private Repository repo;
    private long repoId = 10000;
    private String name = "repo";
    private String location = "bos://backup-cmy";
    private String brokerName = "broker";

    private SnapshotInfo info;

    @Mocked
    private BlobStorage storage;


    @Before
    public void setUp() {
        List<String> files = Lists.newArrayList();
        files.add("1.dat");
        files.add("1.hdr");
        files.add("1.idx");
        info = new SnapshotInfo(1, 2, 3, 4, 5, 6, 7, "/path/to/tablet/snapshot/", files);

        new MockUp<FrontendOptions>() {
            @Mock
            String getLocalHostAddress() {
                return "127.0.0.1";
            }
        };

        new MockUp<BrokerMgr>() {
            @Mock
            public FsBroker getBroker(String name, String host) throws AnalysisException {
                return new FsBroker("10.74.167.16", 8111);
            }
        };

    }

    @Test
    public void testGet() {
        repo = new Repository(10000, "repo", false, location, storage);

        Assert.assertEquals(repoId, repo.getId());
        Assert.assertEquals(name, repo.getName());
        Assert.assertEquals(false, repo.isReadOnly());
        Assert.assertEquals(location, repo.getLocation());
        Assert.assertEquals(null, repo.getErrorMsg());
        Assert.assertTrue(System.currentTimeMillis() - repo.getCreateTime() < 1000);
    }

    @Test
    public void testInit() {
        new Expectations() {
            {
                storage.list(anyString, (List<RemoteFile>) any);
                minTimes = 0;
                result = new Delegate<Status>() {
                    public Status list(String remotePath, List<RemoteFile> result) {
                        result.clear();
                        return Status.OK;
                    }
                };

                storage.directUpload(anyString, anyString);
                minTimes = 0;
                result = Status.OK;
            }
        };

        repo = new Repository(10000, "repo", false, location, storage);

        Status st = repo.initRepository();
        System.out.println(st);
        Assert.assertTrue(st.ok());
    }

    @Test
    public void testassemnblePath() {
        repo = new Repository(10000, "repo", false, location, storage);

        // job info
        String label = "label";
        String createTime = "2018-04-12 20:46:45";
        String createTime2 = "2018-04-12-20-46-45";
        Timestamp ts = Timestamp.valueOf(createTime);
        long creastTs = ts.getTime();
        
        // "location/__palo_repository_repo_name/__ss_my_sp1/__info_2018-01-01-08-00-00"
        String expected = location + "/" + Repository.PREFIX_REPO + name + "/" + Repository.PREFIX_SNAPSHOT_DIR
                + label + "/" + Repository.PREFIX_JOB_INFO + createTime2;
        Assert.assertEquals(expected, repo.assembleJobInfoFilePath(label, creastTs));

        // meta info
        expected = location + "/" + Repository.PREFIX_REPO + name + "/" + Repository.PREFIX_SNAPSHOT_DIR
                + label + "/" + Repository.FILE_META_INFO;
        Assert.assertEquals(expected, repo.assembleMetaInfoFilePath(label));
        
        // snapshot path
        // /location/__palo_repository_repo_name/__ss_my_ss1/__ss_content/__db_10001/__tbl_10020/__part_10031/__idx_10032/__10023/__3481721
        expected = location + "/" + Repository.PREFIX_REPO + name + "/" + Repository.PREFIX_SNAPSHOT_DIR
                + label + "/" + "__ss_content/__db_1/__tbl_2/__part_3/__idx_4/__5/__7";
        Assert.assertEquals(expected, repo.assembleRemoteSnapshotPath(label, info));
    }

    @Test
    public void testPing() {
        new Expectations() {
            {
                storage.checkPathExist(anyString);
                minTimes = 0;
                result = Status.OK;
            }
        };
        
        repo = new Repository(10000, "repo", false, location, storage);
        Assert.assertTrue(repo.ping());
        Assert.assertTrue(repo.getErrorMsg() == null);
    }

    @Test
    public void testListSnapshots() {
        new Expectations() {
            {
                storage.list(anyString, (List<RemoteFile>) any);
                minTimes = 0;
                result = new Delegate() {
                    public Status list(String remotePath, List<RemoteFile> result) {
                        result.add(new RemoteFile(Repository.PREFIX_SNAPSHOT_DIR + "a", false, 100));
                        result.add(new RemoteFile("_ss_b", true, 100));
                        return Status.OK;
                    }
                };
            }
        };

        repo = new Repository(10000, "repo", false, location, storage);
        List<String> snapshotNames = Lists.newArrayList();
        Status st = repo.listSnapshots(snapshotNames);
        Assert.assertTrue(st.ok());
        Assert.assertEquals(1, snapshotNames.size());
        Assert.assertEquals("a", snapshotNames.get(0));
    }

    @Test
    public void testUpload() {
        new Expectations() {
            {
                storage.upload(anyString, anyString);
                minTimes = 0;
                result = Status.OK;

                storage.rename(anyString, anyString);
                minTimes = 0;
                result = Status.OK;

                storage.delete(anyString);
                minTimes = 0;
                result = Status.OK;
            }
        };

        repo = new Repository(10000, "repo", false, location, storage);
        String localFilePath = "./tmp_" + System.currentTimeMillis();
        try (PrintWriter out = new PrintWriter(localFilePath)) {
            out.print("a");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            Assert.fail();
        }
        try {
            String remoteFilePath = location + "/remote_file";
            Status st = repo.upload(localFilePath, remoteFilePath);
            Assert.assertTrue(st.ok());
        } finally {
            File file = new File(localFilePath);
            file.delete();
        }
    }

    @Test
    public void testDownload() {
        String localFilePath = "./tmp_" + System.currentTimeMillis();
        File localFile = new File(localFilePath);
        try {
            try (PrintWriter out = new PrintWriter(localFile)) {
                out.print("a");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                Assert.fail();
            }

            new Expectations() {
                {
                    storage.list(anyString, (List<RemoteFile>) any);
                    minTimes = 0;
                    result = new Delegate() {
                        public Status list(String remotePath, List<RemoteFile> result) {
                            result.add(new RemoteFile("remote_file.0cc175b9c0f1b6a831c399e269772661", true, 100));
                            return Status.OK;
                        }
                    };

                    storage.downloadWithFileSize(anyString, anyString, anyLong);
                    minTimes = 0;
                    result = Status.OK;
                }
            };

            repo = new Repository(10000, "repo", false, location, storage);
            String remoteFilePath = location + "/remote_file";
            Status st = repo.download(remoteFilePath, localFilePath);
            Assert.assertTrue(st.ok());
        } finally {
            localFile.delete();
        }
    }

    @Test
    public void testGetInfo() {
        repo = new Repository(10000, "repo", false, location, storage);
        List<String> infos = repo.getInfo();
        Assert.assertTrue(infos.size() == ShowRepositoriesStmt.TITLE_NAMES.size());
    }

    @Test
    public void testGetSnapshotInfo() {
        new Expectations() {
            {
                storage.list(anyString, (List<RemoteFile>) any);
                minTimes = 0;
                result = new Delegate() {
                    public Status list(String remotePath, List<RemoteFile> result) {
                        if (remotePath.contains(Repository.PREFIX_JOB_INFO)) {
                            result.add(new RemoteFile(" __info_2018-04-18-20-11-00.12345678123456781234567812345678",
                                    true,
                                    100));
                        } else {
                            result.add(new RemoteFile(Repository.PREFIX_SNAPSHOT_DIR + "s1", false, 100));
                            result.add(new RemoteFile(Repository.PREFIX_SNAPSHOT_DIR + "s2", false, 100));
                        }
                        return Status.OK;
                    }
                };
            }
        };

        repo = new Repository(10000, "repo", false, location, storage);
        String snapshotName = "";
        String timestamp = "";
        try {
            List<List<String>> infos = repo.getSnapshotInfos(snapshotName, timestamp);
            Assert.assertEquals(2, infos.size());

        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testPersist() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("bos_endpoint", "http://gz.bcebos.com");
        properties.put("bos_accesskey", "a");
        properties.put("bos_secret_accesskey", "b");
        BlobStorage storage = new BlobStorage(brokerName, properties);
        repo = new Repository(10000, "repo", false, location, storage);

        File file = new File("./Repository");
        try {
            DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
            repo.write(out);
            out.flush();
            out.close();
            
            DataInputStream in = new DataInputStream(new FileInputStream(file));
            Repository newRepo = Repository.read(in);
            in.close();
            
            Assert.assertEquals(repo.getName(), newRepo.getName());
            Assert.assertEquals(repo.getId(), newRepo.getId());
            Assert.assertEquals(repo.getLocation(), newRepo.getLocation());
            
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            Assert.fail();
        } finally {
            file.delete();
        }
    }

}
