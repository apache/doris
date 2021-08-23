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

package org.apache.doris.stack.service.construct;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.apache.doris.stack.model.palo.HdfsFilePreview;
import org.apache.doris.stack.model.palo.HdfsFilePreviewReq;
import org.apache.doris.stack.model.palo.LocalFileSubmitResult;
import org.apache.doris.stack.model.request.construct.FileImportReq;
import org.apache.doris.stack.model.request.construct.HdfsConnectReq;
import org.apache.doris.stack.model.request.construct.HdfsImportReq;
import org.apache.doris.stack.model.response.construct.DataImportTaskPageResp;
import org.apache.doris.stack.model.response.construct.NativeQueryResp;
import org.apache.doris.stack.component.ClusterUserComponent;
import org.apache.doris.stack.component.DatabuildComponent;
import org.apache.doris.stack.connector.PaloFileUploadClient;
import org.apache.doris.stack.connector.PaloQueryClient;
import org.apache.doris.stack.dao.CoreUserRepository;
import org.apache.doris.stack.dao.DataImportTaskRepository;
import org.apache.doris.stack.dao.ManagerTableRepository;
import org.apache.doris.stack.driver.DorisDataBuildDriver;
import org.apache.doris.stack.entity.ClusterInfoEntity;
import org.apache.doris.stack.entity.CoreUserEntity;
import org.apache.doris.stack.entity.DataImportTaskEntity;
import org.apache.doris.stack.entity.ManagerDatabaseEntity;
import org.apache.doris.stack.entity.ManagerTableEntity;
import org.apache.doris.stack.exception.InputFormatException;
import org.apache.doris.stack.exception.NameDuplicatedException;
import org.apache.doris.stack.exception.RequestFieldNullException;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@RunWith(JUnit4.class)
@Slf4j
public class DataImportServiceTest {
    @InjectMocks
    private DataImportService importService;

    @Mock
    private DataImportTaskRepository taskRepository;

    @Mock
    private ManagerTableRepository tableRepository;

    @Mock
    private CoreUserRepository userRepository;

    @Mock
    private PaloFileUploadClient fileUploadClient;

    @Mock
    private PaloQueryClient queryClient;

    @Mock
    private DorisDataBuildDriver driver;

    @Mock
    private ClusterUserComponent clusterUserComponent;

    @Mock
    private DatabuildComponent databuildComponent;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Test upload file
     */
    @Test
    public void uploadLocalFileTest() {
        log.debug("upload local file test.");
        int tableId = 1;
        int userId = 2;
        int clusterId = 3;
        int dbId = 4;

        // mock cluster
        ClusterInfoEntity clusterInfo = mockClusterInfo(clusterId);
        // mock db
        ManagerDatabaseEntity databaseEntity = mockDatabase(dbId, clusterId, "db");
        // mock table
        mockTable(tableId, dbId);

        try {
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            when(databuildComponent.checkClusterDatabase(dbId, clusterId)).thenReturn(databaseEntity);
            importService.uploadLocalFile(tableId, ",", null, userId, "type");
        } catch (Exception e) {
            log.error("create table test error.");
            e.printStackTrace();
        }
    }

    @Test
    public void submitFileImportTest() {
        log.debug("submit file import test.");
        int tableId = 1;
        int userId = 2;
        int clusterId = 3;
        int dbId = 4;

        // mock request
        FileImportReq importInfo = new FileImportReq();
        // request body exception
        // name exception
        try {
            importService.submitFileImport(tableId, importInfo, userId);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }
        // file uuid exception
        importInfo.setName("task1");
        try {
            importService.submitFileImport(tableId, importInfo, userId);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }
        // field is null
        importInfo.setFileUuid("uuid");
        try {
            importService.submitFileImport(tableId, importInfo, userId);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }
        // field is empty
        importInfo.setColumnNames(new ArrayList<>());
        try {
            importService.submitFileImport(tableId, importInfo, userId);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }

        // request
        List<String> columnNames = Lists.newArrayList("field1");
        importInfo.setColumnNames(columnNames);

        // mock cluster
        ClusterInfoEntity clusterInfo = mockClusterInfo(clusterId);
        // mock db
        ManagerDatabaseEntity databaseEntity = mockDatabase(dbId, clusterId, "db");
        // mock user
        mockUser(userId);
        // mock table
        ManagerTableEntity tableEntity = mockTable(tableId, dbId);
        // mock task upload result
        LocalFileSubmitResult result = new LocalFileSubmitResult();

        try {
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            when(databuildComponent.checkClusterDatabase(dbId, clusterId)).thenReturn(databaseEntity);
            when(fileUploadClient.submitFileImport("default_cluster", databaseEntity.getName(),
                    tableEntity.getName(), importInfo, clusterInfo)).thenReturn(result);
            importService.submitFileImport(tableId, importInfo, userId);
        } catch (Exception e) {
            log.error("create table test error.");
            e.printStackTrace();
        }
    }

    @Test
    public void nameDuplicateTest() {
        log.debug("name duplicate test.");
        int dbId = 1;

        // name empty exception
        try {
            importService.nameDuplicate(dbId, "");
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }

        // format exception
        try {
            importService.nameDuplicate(dbId, "-df_afe_dafdsdf");
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), InputFormatException.MESSAGE);
        }

        // Duplicate name exception
        // mock duplicate information
        String taskName = "task";
        DataImportTaskEntity taskEntity = new DataImportTaskEntity();
        taskEntity.setTaskName(taskName);
        List<DataImportTaskEntity> taskEntities = Lists.newArrayList(taskEntity);
        when(taskRepository.getByDbIdAndName(dbId, taskName)).thenReturn(taskEntities);

        try {
            importService.nameDuplicate(dbId, taskName);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), NameDuplicatedException.MESSAGE);
        }
    }

    @Test
    public void hdfsPreviewTest() {
        log.debug("hdfs preview test.");
        int userId = 1;
        int tableId = 2;
        int dbId = 3;
        int clusterId = 4;

        // mock request
        HdfsConnectReq info = new HdfsConnectReq();
        // request exception
        // host empty
        try {
            importService.hdfsPreview(info, userId, tableId);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }
        // fileUrl empty
        info.setHost("10.32.23.4");
        try {
            importService.hdfsPreview(info, userId, tableId);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }
        // format empty
        info.setFileUrl("/sdf/test/file");
        try {
            importService.hdfsPreview(info, userId, tableId);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }
        // Separator empty
        info.setFormat(HdfsConnectReq.Format.CSV);
        try {
            importService.hdfsPreview(info, userId, tableId);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }

        info.setColumnSeparator(",");
        // Request normal test
        ClusterInfoEntity clusterInfo = mockClusterInfo(clusterId);

        ManagerDatabaseEntity databaseEntity = mockDatabase(dbId, clusterId, "db");

        ManagerTableEntity tableEntity = mockTable(tableId, dbId);
        // mock and view broker SQL exceptions
        String sql = "show broker";
        NativeQueryResp queryResp = new NativeQueryResp();
        List<List<String>> datas = new ArrayList<>();
        List<String> data = Lists.newArrayList("broker1");
        datas.add(data);
        queryResp.setData(datas);
        // mock HDFS file to preview query results
        HdfsFilePreview filePreview = new HdfsFilePreview();
        try {
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            when(databuildComponent.checkClusterDatabase(dbId, clusterId)).thenReturn(databaseEntity);
            when(queryClient.executeSQL(sql, "default_cluster", databaseEntity.getName(),
                    clusterInfo)).thenReturn(queryResp);
            when(fileUploadClient.getHdfsPreview(any(), any())).thenReturn(filePreview);
            importService.hdfsPreview(info, userId, tableId);
        } catch (Exception e) {
            log.error("hdfs preview test error.");
            e.printStackTrace();
        }
    }

    @Test
    public void submitHdfsImportTest() {
        log.debug("submit hdfs import test.");
        int userId = 1;
        int tableId = 2;
        int dbId = 3;
        int clusterId = 4;
        // mock request
        HdfsImportReq importReq = new HdfsImportReq();
        // Request exception test
        // name empty
        try {
            importService.submitHdfsImport(importReq, userId, tableId);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }
        importReq.setName("name");
        // fileInfo empty
        try {
            importService.submitHdfsImport(importReq, userId, tableId);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }
        HdfsFilePreviewReq.FileInfo fileInfo = new HdfsFilePreviewReq.FileInfo();
        importReq.setFileInfo(fileInfo);
        // connectInfo empty
        try {
            importService.submitHdfsImport(importReq, userId, tableId);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }
        // field name empty
        importReq.setConnectInfo(new HdfsFilePreviewReq.ConnectInfo());
        try {
            importService.submitHdfsImport(importReq, userId, tableId);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }
        // column name empty
        importReq.setColumnNames(new ArrayList<>());
        try {
            importService.submitHdfsImport(importReq, userId, tableId);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }

        // Request normal test
        List<String> columnNames = Lists.newArrayList("f1", "f2");
        importReq.setColumnNames(columnNames);

        ClusterInfoEntity clusterInfo = mockClusterInfo(clusterId);

        ManagerDatabaseEntity databaseEntity = mockDatabase(dbId, clusterId, "db");

        mockUser(userId);

        mockTable(tableId, dbId);

        try {
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            when(databuildComponent.checkClusterDatabase(dbId, clusterId)).thenReturn(databaseEntity);
            importService.submitHdfsImport(importReq, userId, tableId);
        } catch (Exception e) {
            log.error("submit hdfs import test error.");
            e.printStackTrace();
        }
    }

    @Test
    public void submitHdfsImportSqlTest() {
        log.debug("get submit hdfs import sql test.");
        int userId = 1;
        int tableId = 2;
        int dbId = 3;
        int clusterId = 4;
        // mock request
        HdfsImportReq importReq = new HdfsImportReq();
        importReq.setName("name");
        HdfsFilePreviewReq.FileInfo fileInfo = new HdfsFilePreviewReq.FileInfo();
        importReq.setFileInfo(fileInfo);
        importReq.setConnectInfo(new HdfsFilePreviewReq.ConnectInfo());
        // Request normal test
        List<String> columnNames = Lists.newArrayList("f1", "f2");
        importReq.setColumnNames(columnNames);

        ClusterInfoEntity clusterInfo = mockClusterInfo(clusterId);

        ManagerDatabaseEntity databaseEntity = mockDatabase(dbId, clusterId, "db");

        mockUser(userId);

        mockTable(tableId, dbId);

        try {
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            when(databuildComponent.checkClusterDatabase(dbId, clusterId)).thenReturn(databaseEntity);
            importService.submitHdfsImportSql(importReq, userId, tableId);
        } catch (Exception e) {
            log.error("get submit hdfs import sql test error.");
            e.printStackTrace();
        }
    }

    // TODO:The paging query function has not been tested yet
    @Test
    public void getTaskListTest() {
        log.debug("get file import task test.");
        int userId = 1;
        int tableId = 2;
        int dbId = 3;
        int clusterId = 4;
        int curPage = 1;
        int pageSize = 100;

        ClusterInfoEntity clusterInfo = mockClusterInfo(clusterId);

        ManagerDatabaseEntity databaseEntity = mockDatabase(dbId, clusterId, "db");

        mockTable(tableId, dbId);
        // mock task list
        DataImportTaskEntity taskEntity1 = new DataImportTaskEntity();
        taskEntity1.setTaskName("task1");
        taskEntity1.setImportType("hdfs");
        taskEntity1.setStatus(DataImportTaskPageResp.Status.PENDING.name());
        DataImportTaskEntity taskEntity2 = new DataImportTaskEntity();
        taskEntity2.setTaskName("task2");
        taskEntity2.setImportType("hdfs");
        taskEntity2.setStatus(DataImportTaskPageResp.Status.LOADING.name());
        List<DataImportTaskEntity> taskEntities = Lists.newArrayList(taskEntity1, taskEntity2);

        Pageable pageable = PageRequest.of(curPage - 1, pageSize);
        Specification<DataImportTaskEntity> spec = new Specification() {

            @Override
            public Predicate toPredicate(Root root, CriteriaQuery query, CriteriaBuilder cb) {
                Predicate p = cb.equal(root.get("tableId"), tableId);
                return p;
            }
        };

        PageImpl<DataImportTaskEntity> pageTaskList = new PageImpl(taskEntities);
        when(taskRepository.findAll(spec, pageable)).thenReturn(pageTaskList);

        try {
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            when(databuildComponent.checkClusterDatabase(dbId, clusterId)).thenReturn(databaseEntity);
            importService.getTaskList(tableId, curPage, pageSize, userId);
        } catch (Exception e) {
            log.error("get file import task test error.");
            e.printStackTrace();
        }
    }

    // mock database
    private ManagerDatabaseEntity mockDatabase(int dbId, int clusterId, String dbName) {
        ManagerDatabaseEntity databaseEntity = new ManagerDatabaseEntity();
        databaseEntity.setClusterId(clusterId);
        databaseEntity.setId(dbId);
        databaseEntity.setName(dbName);
        databaseEntity.setDescription("test");
        return databaseEntity;
    }

    // mock cluster
    private ClusterInfoEntity mockClusterInfo(int clusterId) {
        ClusterInfoEntity clusterInfo = new ClusterInfoEntity();
        clusterInfo.setId(clusterId);
        clusterInfo.setName("doris1");
        clusterInfo.setAddress("10.23.32.32");
        clusterInfo.setHttpPort(8030);
        clusterInfo.setQueryPort(8031);
        clusterInfo.setUser("admin");
        clusterInfo.setPasswd("1234");
        clusterInfo.setTimezone("Asia/Shanghai");
        return clusterInfo;
    }

    // mock user
    private CoreUserEntity mockUser(int userId) {
        CoreUserEntity userEntity = new CoreUserEntity();
        userEntity.setId(userId);
        userEntity.setFirstName("user");
        userEntity.setSuperuser(true);
        when(userRepository.findById(userId)).thenReturn(Optional.of(userEntity));
        return userEntity;
    }

    // mock table
    private ManagerTableEntity mockTable(int tableId, int dbId) {
        ManagerTableEntity tableEntity = new ManagerTableEntity();
        tableEntity.setName("table");
        tableEntity.setId(tableId);
        tableEntity.setDbId(dbId);
        when(tableRepository.findById(tableId)).thenReturn(Optional.of(tableEntity));
        return tableEntity;
    }

}
