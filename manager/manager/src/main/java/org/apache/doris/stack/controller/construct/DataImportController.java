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

package org.apache.doris.stack.controller.construct;

import org.apache.doris.stack.model.request.construct.FileImportReq;
import org.apache.doris.stack.model.request.construct.HdfsConnectReq;
import org.apache.doris.stack.model.request.construct.HdfsImportReq;
import org.apache.doris.stack.controller.BaseController;
import org.apache.doris.stack.rest.ResponseEntityBuilder;
import org.apache.doris.stack.service.user.AuthenticationService;
import org.apache.doris.stack.service.construct.DataImportService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Api(tags = "Data import API")
@RestController
@RequestMapping(value = "/api/import/")
@Slf4j
public class DataImportController extends BaseController {
    @Autowired
    private AuthenticationService authenticationService;

    @Autowired
    private DataImportService dataImportService;

    private static final String PARAM_COLUMN_SEPARATOR = "column_separator";

    @ApiOperation(value = "Upload local files and return data preview information")
    @PostMapping(value = "tableId/{" + TABLE_KEY + "}/file/upload",
            produces = MediaType.APPLICATION_JSON_VALUE)
    public Object fileUpload(@PathVariable(value = TABLE_KEY) int tableId,
            @RequestParam(value = PARAM_COLUMN_SEPARATOR) String columnSeparator,
            @RequestParam("file") MultipartFile file,
            HttpServletRequest request, HttpServletResponse response) throws Exception {
        log.debug("upload local file.");
        int studioUserId = authenticationService.checkUserAuthWithCookie(request, response);
        authenticationService.checkUserIsAdmin(studioUserId);
        String contentType = request.getContentType();
        return ResponseEntityBuilder.ok(dataImportService.uploadLocalFile(tableId, columnSeparator, file,
                studioUserId, contentType));
    }

    @ApiOperation(value = "Test the connectivity between the storage engine and the HDFS file system, "
            + "and return the file list information")
    @PostMapping(value = "tableId/{" + TABLE_KEY + "}/hdfs/connect",
            produces = MediaType.APPLICATION_JSON_VALUE)
    public Object hdfsConnect(
            @PathVariable(value = TABLE_KEY) int tableId,
            @RequestBody HdfsConnectReq info,
            HttpServletRequest request, HttpServletResponse response) throws Exception {
        log.debug("Test hdfs connection.");
        int studioUserId = authenticationService.checkUserAuthWithCookie(request, response);
        authenticationService.checkUserIsAdmin(studioUserId);
        return ResponseEntityBuilder.ok(dataImportService.hdfsPreview(info, studioUserId, tableId));
    }

    @ApiOperation(value = "Import the HDFS file into the data table and return the results. "
            + "If successful, return taskid and taskname")
    @PostMapping(path = "tableId/{" + TABLE_KEY + "}/hdfs/submit",
            produces = MediaType.APPLICATION_JSON_VALUE)
    public Object hdfsSubmit(@PathVariable(value = TABLE_KEY) int tableId,
                             @RequestBody HdfsImportReq importInfo,
                             HttpServletRequest request, HttpServletResponse response) throws Exception {
        log.debug("submit hdfs import task.");
        int studioUserId = authenticationService.checkUserAuthWithCookie(request, response);
        authenticationService.checkUserIsAdmin(studioUserId);
        return ResponseEntityBuilder.ok(dataImportService.submitHdfsImport(importInfo, studioUserId, tableId));
    }

    @ApiOperation(value = "Execute the HDFS file to import into the data table and view the SQL statement")
    @PostMapping(path = "tableId/{" + TABLE_KEY + "}/hdfs/submit/sql",
            produces = MediaType.APPLICATION_JSON_VALUE)
    public Object hdfsSubmitSql(@PathVariable(value = TABLE_KEY) int tableId,
                             @RequestBody HdfsImportReq importInfo,
                             HttpServletRequest request, HttpServletResponse response) throws Exception {
        log.debug("get palo hdfs import task sql.");
        int studioUserId = authenticationService.checkUserAuthWithCookie(request, response);
        authenticationService.checkUserIsAdmin(studioUserId);
        return ResponseEntityBuilder.ok(dataImportService.submitHdfsImportSql(importInfo, studioUserId, tableId));
    }

    @ApiOperation(value = "The execution file is imported into the data table and the results are returned")
    @PutMapping(path = "tableId/{" + TABLE_KEY + "}/file/submit",
            produces = MediaType.APPLICATION_JSON_VALUE)
    public Object fileSubmit(@PathVariable(value = TABLE_KEY) int tableId,
            @RequestBody FileImportReq importInfo,
            HttpServletRequest request, HttpServletResponse response) throws Exception {
        log.debug("submit file import task.");
        int studioUserId = authenticationService.checkUserAuthWithCookie(request, response);
        authenticationService.checkUserIsAdmin(studioUserId);
        return ResponseEntityBuilder.ok(dataImportService.submitFileImport(tableId, importInfo,
                studioUserId));
    }

    @ApiOperation(value = "Verify whether the data import task name in Doris cluster is duplicate or meets the rules. "
            + "If it meets the rules, it returns normal, and if it does not, it returns error information"
            + "（TODO：Doris cluster only keeps the task information of the last three days, "
            + "so manager can only keep the task information of the last three days）")
    @GetMapping(value = "dbId/{" + DB_KEY + "}/name/check",
            produces = MediaType.APPLICATION_JSON_VALUE)
    public Object nameDuplicate(@PathVariable(value = DB_KEY) int dbId,
                                @RequestParam(value = "taskName") String taskName,
                                HttpServletRequest request, HttpServletResponse response) throws Exception {
        log.debug("check import task name.");
        int studioUserId = authenticationService.checkUserAuthWithCookie(request, response);
        authenticationService.checkUserIsAdmin(studioUserId);
        dataImportService.nameDuplicate(dbId, taskName);
        return ResponseEntityBuilder.ok();

    }

    @ApiOperation(value = "Pagination obtains the list of all data import tasks of the corresponding table. "
            + "Pagination is not supported yet. The number of pages starts from 1")
    @GetMapping(value = "tableId/{" + TABLE_KEY + "}/task/list/{page}/{pageSize}",
            produces = MediaType.APPLICATION_JSON_VALUE)
    public Object getTaskList(@PathVariable(value = TABLE_KEY) int tableId,
                              @PathVariable("page") int page,
                              @PathVariable("pageSize") int pageSize,
                              HttpServletRequest request, HttpServletResponse response) throws Exception {
        log.debug("get task list by page.");
        int studioUserId = authenticationService.checkUserAuthWithCookie(request, response);
        authenticationService.checkUserIsAdmin(studioUserId);
        return ResponseEntityBuilder.ok(dataImportService.getTaskList(tableId, page, pageSize, studioUserId));
    }
}
