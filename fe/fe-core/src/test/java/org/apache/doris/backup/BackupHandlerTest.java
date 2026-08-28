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

import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.catalog.BrokerMgr;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.LocalTabletInvertedIndex;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.info.TableRefInfo;
import org.apache.doris.nereids.trees.plans.commands.BackupCommand;
import org.apache.doris.nereids.trees.plans.commands.CancelBackupCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateRepositoryCommand;
import org.apache.doris.nereids.trees.plans.commands.RestoreCommand;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
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
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class BackupHandlerTest {

    private BackupHandler handler;

    private Env env = Mockito.mock(Env.class);
    private InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
    private BrokerMgr brokerMgr = Mockito.mock(BrokerMgr.class);
    private EditLog editLog = Mockito.mock(EditLog.class);

    private MockedStatic<Env> mockedEnvStatic;

    private Database db;

    private long idGen = 0;

    private File rootDir;

    private String tmpPath = "./tmp" + System.currentTimeMillis();

    private TabletInvertedIndex invertedIndex = new LocalTabletInvertedIndex();

    @Before
    public void setUp() throws Exception {
        Config.tmp_dir = tmpPath;
        rootDir = new File(Config.tmp_dir);
        rootDir.mkdirs();
        FeConstants.runningUnitTest = true;

        mockedEnvStatic = Mockito.mockStatic(Env.class);

        Mockito.when(env.getBrokerMgr()).thenReturn(brokerMgr);
        Mockito.when(env.getNextId()).thenAnswer(inv -> idGen++);
        Mockito.when(env.getEditLog()).thenReturn(editLog);

        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnvStatic.when(Env::getCurrentEnvJournalVersion).thenReturn(FeConstants.meta_version);
        mockedEnvStatic.when(Env::getCurrentInvertedIndex).thenReturn(invertedIndex);

        db = CatalogMocker.mockDb();

        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.doReturn(db).when(catalog).getDbOrDdlException(Mockito.anyString());
    }

    @After
    public void done() {
        if (mockedEnvStatic != null) {
            mockedEnvStatic.close();
        }
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
        handler = new BackupHandler(env);
        handler.runAfterCatalogReady();

        File backupDir = new File(BackupHandler.BACKUP_ROOT_DIR.toString());
        Assert.assertTrue(backupDir.exists());
    }

    @Test
    public void testCreateAndDropRepository() throws Exception {
        Mockito.when(brokerMgr.containsBroker(Mockito.anyString())).thenReturn(true);

        try (MockedConstruction<Repository> mockedRepo = Mockito.mockConstruction(Repository.class,
                (mock, context) -> {
                    List<?> args = context.arguments();
                    if (args.size() >= 2) {
                        Mockito.when(mock.getName()).thenReturn((String) args.get(1));
                        Mockito.when(mock.getId()).thenReturn((Long) args.get(0));
                    }
                    Mockito.when(mock.initRepository()).thenReturn(Status.OK);
                    Mockito.when(mock.ping()).thenReturn(true);
                    Mockito.doAnswer(inv -> {
                        List<String> snapshotNames = inv.getArgument(0);
                        snapshotNames.add("ss2");
                        return Status.OK;
                    }).when(mock).listSnapshots(Mockito.anyList());
                    Mockito.doAnswer(inv -> {
                        List<BackupJobInfo> infos = inv.getArgument(2);
                        OlapTable tbl = (OlapTable) db.getTableOrMetaException(CatalogMocker.TEST_TBL_NAME);
                        List<Table> tbls = Lists.newArrayList();
                        tbls.add(tbl);
                        List<Resource> resources = Lists.newArrayList();
                        BackupMeta backupMeta = new BackupMeta(tbls, resources);
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
                                CatalogMocker.TEST_DB_ID, BackupCommand.BackupContent.ALL,
                                backupMeta, snapshotInfos, null);
                        infos.add(info);
                        return Status.OK;
                    }).when(mock).getSnapshotInfoFile(Mockito.anyString(), Mockito.anyString(), Mockito.anyList());
                })) {

            // add repo
            handler = new BackupHandler(env);
            StorageBackend storageBackend = new StorageBackend("broker", "bos://location",
                    StorageBackend.StorageType.BROKER, Maps.newHashMap());

            CreateRepositoryCommand command = new CreateRepositoryCommand(false, "repo", storageBackend);
            handler.createRepository(command);

            // process backup
            List<TableRefInfo> tableRefInfos = Lists.newArrayList();
            tableRefInfos.add(new TableRefInfo(new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, CatalogMocker.TEST_DB_NAME,
                    CatalogMocker.TEST_TBL_NAME), null, null, null, null, null, null, null));
            Map<String, String> properties = Maps.newHashMap();
            properties.put("backup_timestamp", "2018-08-08-08-08-08");
            boolean isExclude = false;
            BackupCommand backupCommand = new BackupCommand(new LabelNameInfo(CatalogMocker.TEST_DB_NAME, "label1"), "repo", tableRefInfos, properties, isExclude);
            handler.process(backupCommand);

            // handleFinishedSnapshotTask
            BackupJob backupJob = (BackupJob) handler.getJob(CatalogMocker.TEST_DB_ID);
            SnapshotTask snapshotTask = new SnapshotTask(null, 0, 0, backupJob.getJobId(), CatalogMocker.TEST_DB_ID, 0, 0,
                    0, 0, 0, 0, 1, false);
            TFinishTaskRequest request = new TFinishTaskRequest();
            List<String> snapshotFiles = Lists.newArrayList();
            request.setSnapshotFiles(snapshotFiles);
            request.setSnapshotPath("./snapshot/path");
            request.setTaskStatus(new TStatus(TStatusCode.OK));
            handler.handleFinishedSnapshotTask(snapshotTask, request);

            // handleFinishedSnapshotUploadTask
            Map<String, String> srcToDestPath = Maps.newHashMap();
            UploadTask uploadTask = new UploadTask(null, 0, 0, backupJob.getJobId(), CatalogMocker.TEST_DB_ID,
                    srcToDestPath, null, null, StorageBackend.StorageType.BROKER, "");
            request = new TFinishTaskRequest();
            Map<Long, List<String>> tabletFiles = Maps.newHashMap();
            request.setTabletFiles(tabletFiles);
            request.setTaskStatus(new TStatus(TStatusCode.OK));
            handler.handleFinishedSnapshotUploadTask(uploadTask, request);

            // cancel backup
            handler.cancel(new CancelBackupCommand(CatalogMocker.TEST_DB_NAME, false));

            // process restore
            List<TableRefInfo> tableRefInfos2 = Lists.newArrayList();
            tableRefInfos2.add(new TableRefInfo(new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, CatalogMocker.TEST_DB_NAME,
                    CatalogMocker.TEST_TBL_NAME), null, null, null, null, null, null, null));
            Map<String, String> properties02 = Maps.newHashMap();
            properties02.put("backup_timestamp", "2018-08-08-08-08-08");
            boolean isExclude02 = false;
            RestoreCommand restoreCommand = new RestoreCommand(new LabelNameInfo(CatalogMocker.TEST_DB_NAME, "ss2"), "repo", tableRefInfos2, properties02, isExclude02);
            restoreCommand.analyzeProperties();
            handler.process(restoreCommand);

            // handleFinishedSnapshotTask
            RestoreJob restoreJob = (RestoreJob) handler.getJob(CatalogMocker.TEST_DB_ID);
            snapshotTask = new SnapshotTask(null, 0, 0, restoreJob.getJobId(), CatalogMocker.TEST_DB_ID,
                    0, 0, 0, 0, 0, 0, 1, true);
            request = new TFinishTaskRequest();
            request.setSnapshotPath("./snapshot/path");
            request.setTaskStatus(new TStatus(TStatusCode.OK));
            handler.handleFinishedSnapshotTask(snapshotTask, request);

            // handleDownloadSnapshotTask
            DownloadTask downloadTask = new DownloadTask(null, 0, 0, restoreJob.getJobId(), CatalogMocker.TEST_DB_ID,
                    srcToDestPath, null, null, StorageBackend.StorageType.BROKER, "", "");
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

            // cancel restore
            handler.cancel(new CancelBackupCommand(CatalogMocker.TEST_DB_NAME, true));

            // drop repo
            handler.dropRepository("repo");
        }
    }
}
