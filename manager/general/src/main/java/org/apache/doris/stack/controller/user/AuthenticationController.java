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

package org.apache.doris.stack.controller.user;

import org.apache.doris.stack.constant.ConstantDef;
import org.apache.doris.stack.model.request.user.PasswordResetReq;
import org.apache.doris.stack.model.request.user.UserLoginReq;
import org.apache.doris.stack.model.response.user.PasswordResetResp;
import org.apache.doris.stack.rest.ResponseEntityBuilder;
import org.apache.doris.stack.service.user.AuthenticationService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.doris.stack.util.LogBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Api(tags = "User authentication API")
@RestController
@RequestMapping(value = "/api/session/")
@Slf4j
public class AuthenticationController {

    @Autowired
    private AuthenticationService authService;

    @ApiOperation(value = "User login (super administrator / all users)")
    @PostMapping(value = "", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object login(@RequestBody UserLoginReq loginReq,
                        HttpServletRequest request,
                        HttpServletResponse response) throws Exception {
        String requestId = authService.getRequestId(request);
        log.debug(new LogBuilder(requestId)
                .add(ConstantDef.LOG_USER_NAME_KEY, loginReq.getUsername())
                .add(ConstantDef.LOG_MESSAGE_KEY, "user login.")
                .build());
        String sessionId = authService.login(loginReq, request);
        authService.setResponseCookie(response, sessionId);
        return ResponseEntityBuilder.ok(sessionId);
    }

    @ApiOperation(value = "User logout (super administrator / all users)")
    @DeleteMapping(value = "", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object logout(HttpServletRequest request,
                         HttpServletResponse response) throws Exception {
        authService.logout(request, response);
        return ResponseEntityBuilder.ok();
    }

    @ApiOperation(value = "The user forgets the password and sends a password reset email to the mailbox")
    @PostMapping(value = "forgot_password", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object forgetPasswd(@RequestBody String email,
                               HttpServletRequest request,
                               HttpServletResponse response) throws Exception {
        log.debug("User forget password.");
        String hostname = "";
        authService.forgetPassword(email, hostname);
        return ResponseEntityBuilder.ok();
    }

    @ApiOperation(value = "Reset the password via the reset password email link")
    @PostMapping(value = "reset_password", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object resetPassword(@RequestBody PasswordResetReq resetReq,
                                HttpServletRequest request,
                                HttpServletResponse response) throws Exception {
        log.debug("reset password by email link.");

        PasswordResetResp resetResp = authService.resetPassword(resetReq);
        authService.setResponseCookie(response, resetResp.getSessionId());
        return ResponseEntityBuilder.ok();
    }

    @ApiOperation(value = "Verify that the token for resetting the password is still valid")
    @GetMapping(value = "password_reset_token_valid", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object resetTokenValid(
            @RequestParam(value = "token") String token,
            HttpServletRequest request,
            HttpServletResponse response) throws Exception {
        log.debug("check password reset token is valid.");
        return ResponseEntityBuilder.ok(authService.resetTokenValid(token));
    }

    @ApiOperation(value = "Certified by Google, todo: not yet implemented")
    @GetMapping(value = "google_auth", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object googeleAuth(HttpServletRequest request,
                                HttpServletResponse response) throws Exception {
        log.debug("google auth.");
        // TODO:google authentication, temporarily not implemented
        return ResponseEntityBuilder.ok();
    }
}
