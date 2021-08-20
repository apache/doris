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

import org.apache.doris.stack.model.request.config.EmailInfo;
import org.apache.doris.stack.model.request.config.TestEmailReq;
import org.apache.doris.stack.rest.ResponseEntityBuilder;
import org.apache.doris.stack.service.config.EmailService;
import org.apache.doris.stack.service.user.AuthenticationService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Api(tags = "Mailbox information configuration API")
@RestController
@RequestMapping(value = "/api/email/")
@Slf4j
public class EmailController {

    @Autowired
    private AuthenticationService authenticationService;

    @Autowired
    private EmailService emailService;

    @ApiOperation(value = "Get mailbox service configuration information(super administrator/space administrator access)")
    @GetMapping(value = "", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object getEmailSmtp(HttpServletRequest request, HttpServletResponse response) throws Exception {
        log.debug("Admin user update email service info.");
        int userId = authenticationService.checkAllUserAuthWithCookie(request, response);
        if (userId > 0) {
            authenticationService.checkUserIsAdmin(userId);
        }
        return ResponseEntityBuilder.ok(emailService.getEmailConfig(userId));
    }

    @ApiOperation(value = "Configure mailbox service information(super administrator access)")
    @PutMapping(value = "", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object updateEmailSmtp(@RequestBody EmailInfo emailInfo,
                                  HttpServletRequest request,
                                  HttpServletResponse response) throws Exception {
        log.debug("Super Admin user update email service info.");
        authenticationService.checkSuperAdminUserAuthWithCookie(request, response);
        emailService.updateEmailInfo(emailInfo);
        return ResponseEntityBuilder.ok();
    }

    @ApiOperation(value = "Empty mailbox service information(super administrator access)")
    @DeleteMapping(value = "", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object deleteEmailSmtp(HttpServletRequest request, HttpServletResponse response) throws Exception {
        log.debug("Admin user update email service info.");
        authenticationService.checkSuperAdminUserAuthWithCookie(request, response);
        emailService.deleteEmailInfo();
        return ResponseEntityBuilder.ok();
    }

    @ApiOperation(value = "Send a test email to verify the mailbox service(super administrator access)")
    @PostMapping(value = "test", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object testEmail(@RequestBody TestEmailReq testEmailReq, HttpServletRequest request, HttpServletResponse response) throws Exception {
        log.debug("Admin user update email service info.");
        authenticationService.checkSuperAdminUserAuthWithCookie(request, response);

        emailService.sendTestEmail(testEmailReq);
        return ResponseEntityBuilder.ok();
    }
}
