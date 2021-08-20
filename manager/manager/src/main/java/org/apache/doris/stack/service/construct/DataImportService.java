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

import org.apache.doris.stack.constant.ConstantDef;
import org.apache.doris.stack.model.palo.HdfsFilePreview;
import org.apache.doris.stack.model.palo.HdfsFilePreviewReq;
import org.apache.doris.stack.model.palo.HdfsImportTaskInfo;
import org.apache.doris.stack.model.palo.LocalFileInfo;
import org.apache.doris.stack.model.palo.LocalFileSubmitResult;
import org.apache.doris.stack.model.request.construct.FileImportReq;
import org.apache.doris.stack.model.request.construct.HdfsConnectReq;
import org.apache.doris.stack.model.request.construct.HdfsImportReq;
import org.apache.doris.stack.model.response.construct.DataImportTaskPageResp;
import org.apache.doris.stack.model.response.construct.HdfsPreviewResp;
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
import org.apache.doris.stack.exception.UnknownException;
import org.apache.doris.stack.service.BaseService;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class DataImportService extends BaseService {

    private static final String LABEL_REGEX = "^[a-zA-Z0-9][a-zA-Z0-9_-]*$";

    private static final String PARAM_COLUMN_SEPARATOR = "column_separator";

    private static final String PARAM_PREVIEW = "preview";

    private static final String FILE_IMPORT_TYPE = "file";

    private static final String HDFS_IMPORT_TYPE = "hdfs";

    private DataImportTaskRepository taskRepository;

    private ManagerTableRepository tableRepository;

    private CoreUserRepository userRepository;

    private PaloFileUploadClient fileUploadClient;

    private PaloQueryClient queryClient;

    private DorisDataBuildDriver driver;

    private ClusterUserComponent clusterUserComponent;

    private DatabuildComponent databuildComponent;

    @Autowired
    public DataImportService(DataImportTaskRepository taskRepository,
                             ManagerTableRepository tableRepository,
                             PaloFileUploadClient fileUploadClient,
                             CoreUserRepository userRepository,
                             PaloQueryClient queryClient,
                             DorisDataBuildDriver driver,
                             ClusterUserComponent clusterUserComponent,
                             DatabuildComponent databuildComponent) {
        this.taskRepository = taskRepository;
        this.tableRepository = tableRepository;
        this.fileUploadClient = fileUploadClient;
        this.userRepository = userRepository;
        this.queryClient = queryClient;
        this.driver = driver;
        this.clusterUserComponent = clusterUserComponent;
        this.databuildComponent = databuildComponent;
    }

    /**
     * Upload local files and return preview information
     *
     * @param tableId
     * @param columnSeparator
     * @param file
     * @param userId
     * @param contentType
     * @return
     * @throws Exception
     */
    public LocalFileInfo uploadLocalFile(int tableId, String columnSeparator, MultipartFile file,
                                         int userId, String contentType) throws Exception {
        log.debug("upload local file for table {}", tableId);
        ClusterInfoEntity clusterInfo = clusterUserComponent.getClusterByUserId(userId);
        ManagerTableEntity tableEntity = tableRepository.findById(tableId).get();
        ManagerDatabaseEntity databaseEntity =
                databuildComponent.checkClusterDatabase(tableEntity.getDbId(), clusterInfo.getId());

        Map<String, String> otherParams = Maps.newHashMap();
        otherParams.put(PARAM_COLUMN_SEPARATOR, columnSeparator);
        otherParams.put(PARAM_PREVIEW, "true");
        LocalFileInfo fileInfo = fileUploadClient.uploadLocalFile(ConstantDef.DORIS_DEFAULT_NS, databaseEntity.getName(),
                tableEntity.getName(), file, otherParams, clusterInfo, contentType);

        return fileInfo;
    }

    /**
     * Import and submit local files, and import the uploaded files into the data table
     *
     * @param tableId
     * @param importInfo
     * @param studioUserId
     * @return
     * @throws Exception
     */
    @Transactional
    public LocalFileSubmitResult submitFileImport(int tableId, FileImportReq importInfo, int studioUserId) throws Exception {
        if (StringUtils.isEmpty(importInfo.getName()) || StringUtils.isEmpty(importInfo.getFileUuid())
                || importInfo.getColumnNames() == null || importInfo.getColumnNames().isEmpty()) {
            log.error("File import task name,fileuuid or column names is null");
            throw new RequestFieldNullException();
        }

        ClusterInfoEntity clusterInfo = clusterUserComponent.getClusterByUserId(studioUserId);
        ManagerTableEntity tableEntity = tableRepository.findById(tableId).get();
        ManagerDatabaseEntity databaseEntity =
                databuildComponent.checkClusterDatabase(tableEntity.getDbId(), clusterInfo.getId());

        CoreUserEntity userEntity = userRepository.findById(studioUserId).get();

        DataImportTaskEntity taskEntity = new DataImportTaskEntity(importInfo, tableId, databaseEntity.getId(),
                userEntity.getFirstName(), clusterInfo.getId());
        taskEntity.setImportType(FILE_IMPORT_TYPE);

        try {
            LocalFileSubmitResult result = fileUploadClient.submitFileImport(ConstantDef.DORIS_DEFAULT_NS, databaseEntity.getName(),
                    tableEntity.getName(), importInfo, clusterInfo);
            log.debug("submit file import request success.");
            taskEntity.updateByLocalFileSubmitResult(result);
            taskRepository.save(taskEntity);
            log.debug("save data import task success.");

            fileUploadClient.deleteLocalFile(ConstantDef.DORIS_DEFAULT_NS, databaseEntity.getName(),
                    tableEntity.getName(), importInfo.getFileId(), importInfo.getFileUuid(), clusterInfo);
            log.info("delete palo upload file cache success.");
            return result;
        } catch (Exception e) {
            log.error("submit file import error");
            throw e;
        }
    }

    /**
     * Check whether the import tasks in the current cluster have the same name
     * only tasks within 3 days are reserved by default
     *
     * @param dbId
     * @param taskName
     * @return
     * @throws Exception
     */
    public void nameDuplicate(int dbId, String taskName) throws Exception {
        if (StringUtils.isEmpty(taskName)) {
            log.error("The task name is empty.");
            throw new RequestFieldNullException();
        }
        // Judge whether the string conforms to the specification
        if (!taskName.matches(LABEL_REGEX)) {
            log.error("Name format error. regex: " + LABEL_REGEX + ", name: " + taskName);
            throw new InputFormatException();
        }

        List<DataImportTaskEntity> taskEntities = taskRepository.getByDbIdAndName(dbId, taskName);
        if (taskEntities != null && !taskEntities.isEmpty()) {
            log.error("The task name is duplicate");
            throw new NameDuplicatedException();
        }
    }

    /**
     * Delete expired data import tasks at 0:00 every day
     * At present, the import tasks stored in the Doris cluster are those in the last three days,
     * so those in three days should be deleted
     */
    @Scheduled(cron = "0 0 * * * ?")
    @Transactional
    public void deleteExpireTask() {
        try {
            log.info("Delete expire data import task.");
            long expireMillis = 3 * 86400000L;
            Timestamp expireTime = new Timestamp(System.currentTimeMillis() - expireMillis);
            taskRepository.deleteExpireData(expireTime);
        } catch (Exception e) {
            log.error("delete expire data import task error {}.", e);
        }
    }

    /**
     * Test the connectivity of HDFS files and return preview information
     *
     * @param info
     * @param studioUserId
     * @param tableId
     * @return
     * @throws Exception
     */
    public HdfsPreviewResp hdfsPreview(HdfsConnectReq info, int studioUserId, int tableId) throws Exception {
        log.debug("User {} get hdfs file preview.", studioUserId);
        if (StringUtils.isEmpty(info.getHost()) || StringUtils.isEmpty(info.getFileUrl()) || info.getFormat() == null
                || info.getColumnSeparator() == null) {
            log.error("Hdfs import host,file url, file format or file column separator is null");
            throw new RequestFieldNullException();
        }

        ClusterInfoEntity clusterInfo = clusterUserComponent.getClusterByUserId(studioUserId);
        ManagerTableEntity tableEntity = tableRepository.findById(tableId).get();
        ManagerDatabaseEntity databaseEntity =
                databuildComponent.checkClusterDatabase(tableEntity.getDbId(), clusterInfo.getId());

        // get broker name
        String sql = "show broker";
        NativeQueryResp queryResp;
        String brokerName = null;
        try {
            queryResp = queryClient.executeSQL(sql, ConstantDef.DORIS_DEFAULT_NS, databaseEntity.getName(), clusterInfo);
            List<List<String>> datas = queryResp.getData();
            //TODO：There is only one borker by default
            List<String> data = datas.get(0);
            brokerName = data.get(0);
            log.debug("Get hdfs broker {}.", brokerName);
        } catch (Exception e) {
            log.error("Get borker exception {}", e);
            throw new UnknownException("The Palo cluster no have broker, please install.");
        }

        HdfsFilePreviewReq previewReq = new HdfsFilePreviewReq();
        // TODO:Currently only HDFS is supported
        StringBuffer fileUrlBuffer = new StringBuffer();
        fileUrlBuffer.append("hdfs://");
        fileUrlBuffer.append(info.getHost());
        fileUrlBuffer.append(":");
        fileUrlBuffer.append(info.getPort());
        fileUrlBuffer.append(info.getFileUrl());

        HdfsFilePreviewReq.FileInfo fileInfo = new HdfsFilePreviewReq.FileInfo();
        fileInfo.setColumnSeparator(info.getColumnSeparator());
        fileInfo.setFileUrl(fileUrlBuffer.toString());
        fileInfo.setFormat(info.getFormat().name());
        previewReq.setFileInfo(fileInfo);

        if (brokerName != null) {
            log.debug("broker not null, add broker {} Props.", brokerName);
            HdfsFilePreviewReq.ConnectInfo connectInfo = new HdfsFilePreviewReq.ConnectInfo();
            connectInfo.setBrokerName(brokerName);

            // TODO:Currently HDFS does not require a password
            Map<String, String> brokerProps = Maps.newHashMap();
            brokerProps.put("username", "");
            brokerProps.put("password", "");
            connectInfo.setBrokerProps(brokerProps);
            previewReq.setConnectInfo(connectInfo);
        }

        // request Doris
        HdfsFilePreview filePreview = fileUploadClient.getHdfsPreview(previewReq, clusterInfo);

        HdfsPreviewResp resp = new HdfsPreviewResp();
        resp.setPeviewStatistic(filePreview.getReviewStatistic());
        resp.setFileSample(filePreview.getFileSample());
        resp.setConnectInfo(previewReq.getConnectInfo());
        resp.setFileInfo(previewReq.getFileInfo());
        log.debug("Get hdfs file preview success.");
        return resp;
    }

    /**
     * Submit a request to import an HDFS file into a data table
     *
     * @param importReq
     * @param studioUserId
     * @param tableId
     * @return
     * @throws Exception
     */
    @Transactional
    public Map<String, Object> submitHdfsImport(HdfsImportReq importReq, int studioUserId, int tableId) throws Exception {
        log.debug("User {} submit hdfs inport for table {}.", studioUserId, tableId);
        // request check
        checkRequestBody(importReq.hasEmptyField());

        ClusterInfoEntity clusterInfo = clusterUserComponent.getClusterByUserId(studioUserId);
        ManagerTableEntity tableEntity = tableRepository.findById(tableId).get();
        ManagerDatabaseEntity databaseEntity =
                databuildComponent.checkClusterDatabase(tableEntity.getDbId(), clusterInfo.getId());
        CoreUserEntity userEntity = userRepository.findById(studioUserId).get();

        DataImportTaskEntity taskEntity = new DataImportTaskEntity(importReq, tableId, databaseEntity.getId(),
                userEntity.getFirstName(), clusterInfo.getId());
        taskEntity.setImportType(HDFS_IMPORT_TYPE);

        String importSql = driver.importHdfsFile(databaseEntity.getName(), tableEntity.getName(), importReq);

        Map<String, Object> result = Maps.newHashMap();

        try {
            queryClient.executeSQL(importSql, ConstantDef.DORIS_DEFAULT_NS, databaseEntity.getName(), clusterInfo);
            log.debug("execute hdfs file import sql success");
            taskRepository.save(taskEntity);
            log.debug("save data import task success.");
        } catch (Exception e) {
            log.error("submit hdfs file import error");
            throw e;
        }
        result.put("taskId", taskEntity.getTaskId());
        result.put("taskName", taskEntity.getTaskName());
        log.debug("submit hdfs import success.");
        return result;
    }

    public String submitHdfsImportSql(HdfsImportReq importReq, int studioUserId,
                                      int tableId) throws Exception {
        checkRequestBody(importReq.hasEmptyField());

        ClusterInfoEntity clusterInfo = clusterUserComponent.getClusterByUserId(studioUserId);
        ManagerTableEntity tableEntity = tableRepository.findById(tableId).get();
        ManagerDatabaseEntity databaseEntity =
                databuildComponent.checkClusterDatabase(tableEntity.getDbId(), clusterInfo.getId());
        CoreUserEntity userEntity = userRepository.findById(studioUserId).get();

        DataImportTaskEntity taskEntity = new DataImportTaskEntity(importReq, tableId, databaseEntity.getId(),
                userEntity.getFirstName(), clusterInfo.getId());
        taskEntity.setImportType(HDFS_IMPORT_TYPE);

        String importSql = driver.importHdfsFile(databaseEntity.getName(), tableEntity.getName(), importReq);
        return importSql;
    }

    public DataImportTaskPageResp getTaskList(int tableId, int curPage, int pageSize,
                                              int studioUserId) throws Exception {
        log.debug("User {} get table {} file import task by page.", studioUserId, tableId);
        ClusterInfoEntity clusterInfo = clusterUserComponent.getClusterByUserId(studioUserId);
        List<DataImportTaskPageResp.DataImportTaskResp> taskRespList = Lists.newArrayList();
        ManagerTableEntity tableEntity = tableRepository.findById(tableId).get();
        ManagerDatabaseEntity databaseEntity =
                databuildComponent.checkClusterDatabase(tableEntity.getDbId(), clusterInfo.getId());
        String dbName = databaseEntity.getName();

        log.debug("Paging read data import task list.");
        Pageable pageable = PageRequest.of(curPage - 1, pageSize);
        Specification<DataImportTaskEntity> spec = new Specification() {

            @Override
            public Predicate toPredicate(Root root, CriteriaQuery query, CriteriaBuilder cb) {
                Predicate p = cb.equal(root.get("tableId"), tableId);
                return p;
            }
        };
        List<DataImportTaskEntity> taskEntities = taskRepository.findAll(spec, pageable).toList();

        log.debug("list size {}", taskEntities.size());

        String showLabel = "show load where Label=";

        for (DataImportTaskEntity taskEntity : taskEntities) {
            if (taskEntity.getImportType().equals(HDFS_IMPORT_TYPE)
                    && (taskEntity.getStatus().equals(DataImportTaskPageResp.Status.PENDING.name())
                    || taskEntity.getStatus().equals(DataImportTaskPageResp.Status.LOADING.name()))) {
                String sql = showLabel + "\"" + taskEntity.getTaskName() + "\"";
                NativeQueryResp queryResult = queryClient.executeSQL(sql, ConstantDef.DORIS_DEFAULT_NS, dbName, clusterInfo);
                if (queryResult.getData().size() != 1) {
                    log.error("The task label is not exist in palo, delete it");
                    taskRepository.delete(taskEntity);
                }
                HdfsImportTaskInfo taskInfo = new HdfsImportTaskInfo(queryResult);
                if (!StringUtils.isEmpty(taskInfo.getState())
                        && !taskEntity.getStatus().equals(taskInfo.getState())) {
                    log.debug("The task status has changed, update info");
                    taskEntity.updateByHdfsTaskInfo(taskInfo);
                    taskRepository.save(taskEntity);
                }
            }
            DataImportTaskPageResp.DataImportTaskResp taskModel = taskEntity.tansToResponseModel(tableEntity.getName());
            taskRespList.add(taskModel);
        }

        DataImportTaskPageResp result = new DataImportTaskPageResp();
        result.setTaskRespList(taskRespList);

        result.setPage(curPage);
        result.setPageSize(pageSize);
        result.setTotalSize(taskRepository.countByTableId(tableId));
        return result;
    }
}
