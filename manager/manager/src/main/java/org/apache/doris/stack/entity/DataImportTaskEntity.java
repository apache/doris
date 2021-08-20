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

package org.apache.doris.stack.entity;

import org.apache.doris.stack.model.palo.HdfsImportTaskInfo;
import org.apache.doris.stack.model.palo.LocalFileSubmitResult;
import org.apache.doris.stack.model.request.construct.FileImportReq;
import org.apache.doris.stack.model.request.construct.HdfsImportReq;
import org.apache.doris.stack.model.response.construct.DataImportTaskPageResp;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;

@Entity
@Table(name = "data_import_task")
@Data
@NoArgsConstructor
public class DataImportTaskEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long taskId;

    @Column(length = 100, nullable = false)
    private String taskName;

    @Column(length = 30, nullable = false)
    private String creator;

    private Timestamp createTime;

    @Column(length = 10)
    private String importType;

    private int tableId;

    private int dbId;

    private long clusterId;

    private String status;

    @Column(columnDefinition = "TEXT")
    private String connectInfo;

    private String fileInfo;

    private long totalRows;

    private long loadRows;

    private long filteredRows;

    private long unselectedRows;

    private long loadTimeMs;

    private long loadBytes;

    private int fileNumber;

    // Bad URL for file and HDFS import
    @Column(columnDefinition = "TEXT")
    private String errorMsg;

    // HDFS import error message
    private String errorInfo;

    public DataImportTaskEntity(FileImportReq req, int tableId, int dbId, String creator, long clusterId) {
        this.taskName = req.getName();
        this.fileInfo = req.getOrginFileName();
        this.creator = creator;
        this.tableId = tableId;
        this.dbId = dbId;
        this.clusterId = clusterId;
        this.createTime = new Timestamp(System.currentTimeMillis());
        this.fileNumber = 1;
        this.status = DataImportTaskPageResp.Status.PENDING.name();
    }

    public DataImportTaskEntity(HdfsImportReq importReq, int tableId, int dbId, String creator, long clusterId) {
        this.taskName = importReq.getName();
        this.fileInfo = importReq.getFileInfo().toString();
        this.connectInfo = importReq.getConnectInfo().toString();
        this.creator = creator;
        this.tableId = tableId;
        this.dbId = dbId;
        this.clusterId = clusterId;
        this.createTime = new Timestamp(System.currentTimeMillis());
        this.status = DataImportTaskPageResp.Status.PENDING.name();
    }

    public void updateByLocalFileSubmitResult(LocalFileSubmitResult result) {
        this.status = result.getStatus();
        this.totalRows = result.getNumberLoadedRows();
        this.loadRows = result.getNumberLoadedRows();
        this.filteredRows = result.getNumberFilteredRows();
        this.unselectedRows = result.getNumberUnselectedRows();
        this.loadTimeMs = result.getLoadTimeMs();
        this.loadBytes = result.getLoadBytes();
        this.errorMsg = result.getErrorURL();
    }

    public void updateByHdfsTaskInfo(HdfsImportTaskInfo taskInfo) {
        this.status = taskInfo.getState();
        if (status.equals(DataImportTaskPageResp.Status.CANCELLED.name())) {
            this.errorMsg = taskInfo.getUrl();
            this.errorInfo = taskInfo.getErrorMsg();
        }

        if (status.equals(DataImportTaskPageResp.Status.FINISHED.name()) && taskInfo.getJobDetails() != null) {
            this.fileNumber = taskInfo.getJobDetails().getFileNumber();
            this.loadBytes = taskInfo.getJobDetails().getFileSize();
        }

        if (status.equals(DataImportTaskPageResp.Status.FINISHED.name())
                && taskInfo.getLoadFinishTime() != null
                && taskInfo.getLoadStartTime() != null) {
            this.loadTimeMs = taskInfo.getLoadFinishTime().getTime() - taskInfo.getLoadStartTime().getTime();
        }
    }

    public DataImportTaskPageResp.DataImportTaskResp tansToResponseModel(String tableName) {
        DataImportTaskPageResp.DataImportTaskResp taskResp = new DataImportTaskPageResp.DataImportTaskResp();
        taskResp.setTaskId(taskId);
        taskResp.setDestTableName(tableName);
        taskResp.setTaskName(taskName);
        taskResp.setCreator(creator);
        taskResp.setCreateTime(createTime);
        taskResp.setImportType(importType);
        taskResp.setStatus(status);
        taskResp.setFileInfo(fileInfo);
        taskResp.setConnectInfo(connectInfo);
        taskResp.setTotalRows(totalRows);
        taskResp.setLoadRows(loadRows);
        taskResp.setFilteredRows(filteredRows);
        taskResp.setUnselectedRows(unselectedRows);
        taskResp.setLoadBytes(loadBytes);
        taskResp.setErrorMsg(errorMsg);
        taskResp.setErrorInfo(errorInfo);
        taskResp.setFileNumber(fileNumber);
        return taskResp;
    }
}
