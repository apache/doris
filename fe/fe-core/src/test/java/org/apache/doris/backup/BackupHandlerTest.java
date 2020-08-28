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

import org.apache.doris.analysis.BackupStmt;
import org.apache.doris.analysis.CancelBackupStmt;
import org.apache.doris.analysis.CreateRepositoryStmt;
import org.apache.doris.analysis.DropRepositoryStmt;
import org.apache.doris.analysis.LabelName;
import org.apache.doris.analysis.RestoreStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.catalog.BrokerMgr;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.persist.EditLog;
import org.apache.doris.task.DirMoveTask;
import org.apache.doris.task.DownloadTask;
import org.apache.doris.task.SnapshotTask;
import org.apache.doris.task.UploadTask;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import mockit.Delegate;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;

public class BackupHandlerTest {

    private BackupHandler handler;

    @Mocked
    private Catalog catalog;
    @Mocked
    private BrokerMgr brokerMgr;
    @Mocked
    private EditLog editLog;

    private Database db;

    private long idGen = 0;

    private File rootDir;

    private String tmpPath = "./tmp" + System.currentTimeMillis();

    private TabletInvertedIndex invertedIndex = new TabletInvertedIndex();

    @Before
    public void setUp() {
        Config.tmp_dir = tmpPath;
        rootDir = new File(Config.tmp_dir);
        rootDir.mkdirs();

        new Expectations() {
            {
                catalog.getBrokerMgr();
                minTimes = 0;
                result = brokerMgr;

                catalog.getNextId();
                minTimes = 0;
                result = idGen++;

                catalog.getEditLog();
                minTimes = 0;
                result = editLog;

                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;

                Catalog.getCurrentCatalogJournalVersion();
                minTimes = 0;
                result = FeConstants.meta_version;

                Catalog.getCurrentInvertedIndex();
                minTimes = 0;
                result = invertedIndex;
            }
        };

        try {
            db = CatalogMocker.mockDb();
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }

        new Expectations() {
            {
                catalog.getDb(anyString);
                minTimes = 0;
                result = db;
            }
        };
    }

    @After
    public void done() {
        if (rootDir != null) {
            try {
                Files.walk(Paths.get(Config.tmp_dir),
                           FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testInit() {
        handler = new BackupHandler(catalog);
        handler.runAfterCatalogReady();

        File backupDir = new File(BackupHandler.BACKUP_ROOT_DIR.toString());
        Assert.assertTrue(backupDir.exists());
    }

    @Test
    public void testCreateAndDropRepository() {
        new Expectations() {
            {
                editLog.logCreateRepository((Repository) any);
                minTimes = 0;
                result = new Delegate() {
                    public void logCreateRepository(Repository repo) {

                    }
                };

                editLog.logDropRepository(anyString);
                minTimes = 0;
                result = new Delegate() {
                    public void logDropRepository(String repoName) {

                    }
                };
            }
        };

        new MockUp<Repository>() {
            @Mock
            public Status initRepository() {
                return Status.OK;
            }

            @Mock
            public Status listSnapshots(List<String> snapshotNames) {
                snapshotNames.add("ss2");
                return Status.OK;
            }

            @Mock
            public Status getSnapshotInfoFile(String label, String backupTimestamp, List<BackupJobInfo> infos) {
                OlapTable tbl = (OlapTable) db.getTable(CatalogMocker.TEST_TBL_NAME);
                List<Table> tbls = Lists.newArrayList();
                tbls.add(tbl);
                Map<Long, SnapshotInfo> snapshotInfos = Maps.newHashMap();
                for (Partition part : tbl.getPartitions()) {
                    for (MaterializedIndex idx : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
                        for (Tablet tablet : idx.getTablets()) {
                            List<String> files = Lists.newArrayList();
                            SnapshotInfo sinfo = new SnapshotInfo(db.getId(), tbl.getId(), part.getId(), idx.getId(),
                                                                  tablet.getId(), -1, 0, "./path", files);
                            snapshotInfos.put(tablet.getId(), sinfo);
                        }
                    }
                }
                
                BackupJobInfo info = BackupJobInfo.fromCatalog(System.currentTimeMillis(),
                                                               "ss2", CatalogMocker.TEST_DB_NAME, 
                                                               CatalogMocker.TEST_DB_ID, tbls, snapshotInfos);
                infos.add(info);
                return Status.OK;
            }
        };

        new Expectations() {
            {
                brokerMgr.contaisnBroker(anyString);
                minTimes = 0;
                result = true;
            }
        };

        // add repo
        handler = new BackupHandler(catalog);
        CreateRepositoryStmt stmt = new CreateRepositoryStmt(false, "repo", "broker", "bos://location",
                Maps.newHashMap());
        try {
            handler.createRepository(stmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // process backup
        List<TableRef> tblRefs = Lists.newArrayList();
        tblRefs.add(new TableRef(new TableName(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME), null));
        BackupStmt backupStmt = new BackupStmt(new LabelName(CatalogMocker.TEST_DB_NAME, "label1"), "repo", tblRefs,
                null);
        try {
            handler.process(backupStmt);
        } catch (DdlException e1) {
            e1.printStackTrace();
            Assert.fail();
        }
        
        // handleFinishedSnapshotTask
        BackupJob backupJob = (BackupJob) handler.getJob(CatalogMocker.TEST_DB_ID);
        SnapshotTask snapshotTask = new SnapshotTask(null, 0, 0, backupJob.getJobId(), CatalogMocker.TEST_DB_ID,
                                                     0, 0, 0, 0, 0, 0, 0, 1, false);
        TFinishTaskRequest request = new TFinishTaskRequest();
        List<String> snapshotFiles = Lists.newArrayList();
        request.setSnapshotFiles(snapshotFiles);
        request.setSnapshotPath("./snapshot/path");
        request.setTaskStatus(new TStatus(TStatusCode.OK));
        handler.handleFinishedSnapshotTask(snapshotTask, request);

        // handleFinishedSnapshotUploadTask
        Map<String, String> srcToDestPath = Maps.newHashMap();
        UploadTask uploadTask = new UploadTask(null, 0, 0, backupJob.getJobId(), CatalogMocker.TEST_DB_ID,
                srcToDestPath, null, null);
        request = new TFinishTaskRequest();
        Map<Long, List<String>> tabletFiles = Maps.newHashMap();
        request.setTabletFiles(tabletFiles);
        request.setTaskStatus(new TStatus(TStatusCode.OK));
        handler.handleFinishedSnapshotUploadTask(uploadTask, request);

        // test file persist
        File tmpFile = new File("./tmp" + System.currentTimeMillis());
        try {
            DataOutputStream out = new DataOutputStream(new FileOutputStream(tmpFile));
            handler.write(out);
            out.flush();
            out.close();
            DataInputStream in = new DataInputStream(new FileInputStream(tmpFile));
            BackupHandler.read(in);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            tmpFile.delete();
        }

        // cancel backup
        try {
            handler.cancel(new CancelBackupStmt(CatalogMocker.TEST_DB_NAME, false));
        } catch (DdlException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // process restore
        List<TableRef> tblRefs2 = Lists.newArrayList();
        tblRefs2.add(new TableRef(new TableName(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME), null));
        Map<String, String> properties = Maps.newHashMap();
        properties.put("backup_timestamp", "2018-08-08-08-08-08");
        RestoreStmt restoreStmt = new RestoreStmt(new LabelName(CatalogMocker.TEST_DB_NAME, "ss2"), "repo", tblRefs2,
                properties);
        try {
            restoreStmt.analyzeProperties();
        } catch (AnalysisException e2) {
            e2.printStackTrace();
            Assert.fail();
        }

        try {
            handler.process(restoreStmt);
        } catch (DdlException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // handleFinishedSnapshotTask
        RestoreJob restoreJob = (RestoreJob) handler.getJob(CatalogMocker.TEST_DB_ID);
        snapshotTask = new SnapshotTask(null, 0, 0, restoreJob.getJobId(), CatalogMocker.TEST_DB_ID,
                0, 0, 0, 0, 0, 0, 0, 1, true);
        request = new TFinishTaskRequest();
        request.setSnapshotPath("./snapshot/path");
        request.setTaskStatus(new TStatus(TStatusCode.OK));
        handler.handleFinishedSnapshotTask(snapshotTask, request);

        // handleDownloadSnapshotTask
        DownloadTask downloadTask = new DownloadTask(null, 0, 0, restoreJob.getJobId(), CatalogMocker.TEST_DB_ID,
                srcToDestPath, null, null);
        request = new TFinishTaskRequest();
        List<Long> downloadedTabletIds = Lists.newArrayList();
        request.setDownloadedTabletIds(downloadedTabletIds);
        request.setTaskStatus(new TStatus(TStatusCode.OK));
        handler.handleDownloadSnapshotTask(downloadTask, request);

        // handleDirMoveTask
        DirMoveTask dirMoveTask = new DirMoveTask(null, 0, 0, restoreJob.getJobId(), CatalogMocker.TEST_DB_ID, 0, 0, 0,
                0, "", 0, true);
        request = new TFinishTaskRequest();
        request.setTaskStatus(new TStatus(TStatusCode.OK));
        handler.handleDirMoveTask(dirMoveTask, request);

        // test file persist
        tmpFile = new File("./tmp" + System.currentTimeMillis());
        try {
            DataOutputStream out = new DataOutputStream(new FileOutputStream(tmpFile));
            handler.write(out);
            out.flush();
            out.close();
            DataInputStream in = new DataInputStream(new FileInputStream(tmpFile));
            BackupHandler.read(in);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            tmpFile.delete();
        }

        // cancel restore
        try {
            handler.cancel(new CancelBackupStmt(CatalogMocker.TEST_DB_NAME, true));
        } catch (DdlException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // drop repo
        try {
            handler.dropRepository(new DropRepositoryStmt("repo"));
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
