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

package org.apache.doris.stack.controller.config;

import org.apache.doris.stack.model.request.config.ConfigUpdateReq;
import org.apache.doris.stack.model.request.config.InitStudioReq;
import org.apache.doris.stack.model.response.config.SettingItem;
import org.apache.doris.stack.rest.ResponseEntityBuilder;
import org.apache.doris.stack.service.config.SettingService;
import org.apache.doris.stack.service.user.AuthenticationService;
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
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Api(tags = "configuration information API")
@RestController
@RequestMapping(value = "/api/setting/")
@Slf4j
public class SettingController {

    @Autowired
    private AuthenticationService authenticationService;

    @Autowired
    private SettingService settingService;

    @ApiOperation(value = "Initialize stack service authentication type(super administrator access)")
    @PostMapping(value = "init/studio", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object initStudio(@RequestBody InitStudioReq initStudioReq, HttpServletRequest request,
                             HttpServletResponse response) throws Exception {
        log.debug("init studio.");
        authenticationService.checkSuperAdminUserAuthWithCookie(request, response);
        settingService.initStudio(initStudioReq);
        log.debug("init studio successful.");
        return ResponseEntityBuilder.ok();
    }

    @ApiOperation(value = "Get all configuration information of the current space")
    @GetMapping(value = "", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object getAllConfig(HttpServletRequest request,
                               HttpServletResponse response) throws Exception {
        log.debug("get config info.");
        int userId = authenticationService.checkUserAuthWithCookie(request, response);
        authenticationService.checkUserIsAdmin(userId);
        return ResponseEntityBuilder.ok(settingService.getAllConfig(userId));
    }

    @ApiOperation(value = "Get all global configuration information of stack(super administrator access)")
    @GetMapping(value = "global", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object getAllGlobalConfig(HttpServletRequest request,
                               HttpServletResponse response) throws Exception {
        log.debug("get config info.");
        authenticationService.checkSuperAdminUserAuthWithCookie(request, response);
        return ResponseEntityBuilder.ok(settingService.getAllPublicConfig());
    }

    @ApiOperation(value = "Get the information of a global configuration item by name(super administrator access)")
    @GetMapping(value = "global/{key}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object getGlobalConfigByKey(@PathVariable(value = "key") String key,
                                 HttpServletRequest request,
                                 HttpServletResponse response) throws Exception {
        log.debug("get config info.");
        authenticationService.checkSuperAdminUserAuthWithCookie(request, response);
        return ResponseEntityBuilder.ok(settingService.getConfigByKey(key));
    }

    @ApiOperation(value = "Get the information of a configuration item by name(super administrator/space administrator access)")
    @GetMapping(value = "{key}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object getConfigByKey(@PathVariable(value = "key") String key,
                                 HttpServletRequest request,
                                 HttpServletResponse response) throws Exception {
        log.debug("get config info.");
        int userId = authenticationService.checkUserAuthWithCookie(request, response);
        if (userId < 1) {
            return ResponseEntityBuilder.ok(settingService.getConfigByKey(key));
        } else {
            authenticationService.checkUserIsAdmin(userId);
            return ResponseEntityBuilder.ok(settingService.getConfigByKey(key, userId));
        }
    }

    @ApiOperation(value = "Modify the information of a configuration item according to its name(super administrator/space administrator access)")
    @PutMapping(value = "{key}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object updateConfigByKey(@PathVariable(value = "key") String key,
                                    @RequestBody ConfigUpdateReq updateReq,
                                    HttpServletRequest request,
                                    HttpServletResponse response) throws Exception {
        log.debug("get config info.");
        int userId = authenticationService.checkAllUserAuthWithCookie(request, response);
        SettingItem resutl;
        if (userId < 1) {
            resutl = settingService.superUpdateConfigByKey(key, updateReq);
        } else {
            authenticationService.checkUserIsAdmin(userId);
            resutl = settingService.amdinUpdateConfigByKey(key, userId, updateReq);
        }
        return ResponseEntityBuilder.ok(resutl);
    }
}
