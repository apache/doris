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

import org.apache.doris.stack.model.request.construct.DbCreateReq;
import org.apache.doris.stack.model.request.construct.TableCreateReq;
import org.apache.doris.stack.controller.BaseController;
import org.apache.doris.stack.rest.ResponseEntityBuilder;
import org.apache.doris.stack.service.user.AuthenticationService;
import org.apache.doris.stack.service.construct.DataManageService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Api(tags = "Data management API")
@RestController
@RequestMapping(value = "/api/build/")
@Slf4j
public class DataManageController extends BaseController {
    @Autowired
    private AuthenticationService authenticationService;

    @Autowired
    private DataManageService manageService;

    @ApiOperation(value = "create Database")
    @PostMapping(value = "nsId/{" + NS_KEY + "}/" + DATABASE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public Object createDatabase(
            @PathVariable(value = NS_KEY) int nsId,
            HttpServletRequest request, HttpServletResponse response,
            @RequestBody DbCreateReq dbInfo) throws Exception {
        log.debug("create database by object.");
        int studioUserId = authenticationService.checkUserAuthWithCookie(request, response);
        authenticationService.checkUserIsAdmin(studioUserId);
        return ResponseEntityBuilder.ok(manageService.createDatabse(nsId, dbInfo, studioUserId));
    }

    @ApiOperation(value = "delete Database")
    @DeleteMapping(value = "nsId/{" + NS_KEY + "}/" + DATABASE + "/{" + DB_KEY + "}",
            produces = MediaType.APPLICATION_JSON_VALUE)
    public Object deleteDatabase(
            @PathVariable(value = NS_KEY) int nsId,
            @PathVariable(value = DB_KEY) int dbId,
            HttpServletRequest request, HttpServletResponse response) throws Exception {
        log.debug("delete database by id.");
        int studioUserId = authenticationService.checkUserAuthWithCookie(request, response);
        authenticationService.checkUserIsAdmin(studioUserId);
        manageService.deleteDatabse(nsId, dbId, studioUserId);
        return ResponseEntityBuilder.ok();
    }

    @ApiOperation(value = "Create a table in SQL mode (note that the SQL statement "
            + "returned from the front end should not have line breaks)")
    @PostMapping(value = "native/" + "nsId/{" + NS_KEY + "}/" + "dbId/{" + DB_KEY + "}/" + TABLE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public Object createTableBySQL(
            @PathVariable(value = NS_KEY) int nsId,
            @PathVariable(value = DB_KEY) int dbId,
            HttpServletRequest request, HttpServletResponse response,
            @RequestBody String createTableSql) throws Exception {
        log.debug("create database by sql.");
        int studioUserId = authenticationService.checkUserAuthWithCookie(request, response);
        authenticationService.checkUserIsAdmin(studioUserId);
        return ResponseEntityBuilder.ok(manageService.crateTableBySql(nsId, dbId, createTableSql, studioUserId));
    }

    @ApiOperation(value = "Create a table using Wizard mode")
    @PostMapping(value = "guide/" + "nsId/{" + NS_KEY + "}/" + "dbId/{" + DB_KEY + "}/" + TABLE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public Object createTableByGuide(
            @PathVariable(value = NS_KEY) int nsId,
            @PathVariable(value = DB_KEY) int dbId,
            HttpServletRequest request, HttpServletResponse response,
            @RequestBody TableCreateReq createInfo) throws Exception {
        log.debug("create table by object.");
        int studioUserId = authenticationService.checkUserAuthWithCookie(request, response);
        authenticationService.checkUserIsAdmin(studioUserId);
        return ResponseEntityBuilder.ok(manageService.createTable(nsId, dbId, createInfo,
                studioUserId));
    }

    @ApiOperation(value = "Use wizard mode to create a table and view SQL statements")
    @PostMapping(value = "guide/" + "nsId/{" + NS_KEY + "}/" + "dbId/{" + DB_KEY + "}/" + TABLE + "/sql",
            produces = MediaType.APPLICATION_JSON_VALUE)
    public Object createTableByGuideSQL(
            @PathVariable(value = NS_KEY) int nsId,
            @PathVariable(value = DB_KEY) int dbId,
            HttpServletRequest request, HttpServletResponse response,
            @RequestBody TableCreateReq createInfo) throws Exception {
        log.debug("get sql of create table by object.");
        int studioUserId = authenticationService.checkUserAuthWithCookie(request, response);
        authenticationService.checkUserIsAdmin(studioUserId);
        return ResponseEntityBuilder.ok(manageService.createTableSql(nsId, dbId, createInfo));
    }
}
