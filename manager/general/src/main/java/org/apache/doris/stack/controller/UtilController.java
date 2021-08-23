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

package org.apache.doris.stack.controller;

import org.apache.doris.stack.rest.ResponseEntityBuilder;
import org.apache.doris.stack.service.UtilService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * @Description：
 */
@Api(tags = "Tool API, which provides a general tool API without permission verification")
@RestController
@RequestMapping(value = "/api/util")
@Slf4j
public class UtilController {

    @Autowired
    private UtilService utilService;

    @ApiOperation(value = "Check the user password specification, "
            + "and check whether the returned password meets the specification。")
    @PostMapping(value = "/password_check", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object passwordCheck(
            HttpServletRequest request, HttpServletResponse response,
            @RequestBody String passwd) throws Exception {
        log.debug("password check.");
        return ResponseEntityBuilder.ok(utilService.passwordCheck(passwd));
    }
}
