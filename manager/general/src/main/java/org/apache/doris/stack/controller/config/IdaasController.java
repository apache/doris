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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.apache.doris.stack.model.request.config.IdaasSettingReq;
import org.apache.doris.stack.rest.ResponseEntityBuilder;
import org.apache.doris.stack.service.config.IdaasService;
import org.apache.doris.stack.service.user.AuthenticationService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Api(tags = "Idaas authentication information configuration API")
@RestController
@RequestMapping(value = "/api/idaas/")
@Slf4j
public class IdaasController {

    @Autowired
    private IdaasService idaasService;

    @Autowired
    private AuthenticationService authenticationService;

    @ApiOperation(value = "config IDAAS(super administrator access)")
    @PutMapping(value = "setting", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object setting(@RequestBody IdaasSettingReq idaasSettingReq, HttpServletRequest request,
                          HttpServletResponse response) throws Exception {

        log.debug("Admin User update IDAAS service info.");
        authenticationService.checkSuperAdminUserAuthWithCookie(request, response);
        idaasService.update(idaasSettingReq);
        return ResponseEntityBuilder.ok();
    }

    @ApiOperation(value = "get IDAAS configuration (super administrator access)")
    @GetMapping(value = "setting", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object setting(HttpServletRequest request,
                          HttpServletResponse response) throws Exception {

        log.debug("Admin User get IDAAS service info.");
        authenticationService.checkSuperAdminUserAuthWithCookie(request, response);
        return ResponseEntityBuilder.ok(idaasService.setting());
    }
}
