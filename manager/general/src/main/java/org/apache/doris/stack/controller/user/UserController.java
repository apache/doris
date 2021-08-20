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

import org.apache.doris.stack.model.request.user.PasswordUpdateReq;
import org.apache.doris.stack.model.request.user.UserAddReq;
import org.apache.doris.stack.model.request.user.UserSpaceReq;
import org.apache.doris.stack.model.request.user.UserUpdateReq;
import org.apache.doris.stack.controller.BaseController;
import org.apache.doris.stack.rest.ResponseEntityBuilder;
import org.apache.doris.stack.service.user.AuthenticationService;
import org.apache.doris.stack.service.user.UserService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * @Description：TODO：At present, a user can only be in one space, and a subsequent user may be in multiple spaces.
 * TODO：At present, the user management in the space is implemented here,
 * TODO：and the subsequent users must be modified uniformly
 */
@Api(tags = "User management in space API")
@RestController
@RequestMapping(value = "/api/user/")
@Slf4j
public class UserController extends BaseController {

    @Autowired
    private UserService userService;

    @Autowired
    private AuthenticationService authenticationService;

    /**
     * The space administrator obtains the list of all users in the space.
     * If includedeactivated is true, it means to obtain all users (active and inactive),
     * If includedeactivated is false, the active user is obtained
     *
     * @param includeDeactivated
     * @param request
     * @param response
     * @return
     * @throws Exception
     */
    @ApiOperation(value = "The space administrator obtains the list of all users in the space")
    @GetMapping(value = "", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object getUser(@RequestParam(value = "include_deactivated", defaultValue = "false") boolean includeDeactivated,
                          HttpServletRequest request,
                          HttpServletResponse response) throws Exception {
        int userId = authenticationService.checkUserAuthWithCookie(request, response);
        authenticationService.checkUserIsAdmin(userId);
        log.debug("Admin user {} get user list.", userId);

        return ResponseEntityBuilder.ok(userService.getAllUser(includeDeactivated, userId));
    }

    @ApiOperation(value = "Get the current logged in user information (space administrator/all users)")
    @GetMapping(value = "current", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object getCurrentUser(HttpServletRequest request,
                                 HttpServletResponse response) throws Exception {
        int userId = authenticationService.checkAllUserAuthWithCookie(request, response);
        if (userId < 1) {
            return ResponseEntityBuilder.ok(authenticationService.getSuperUserInfo());
        } else {
            return ResponseEntityBuilder.ok(userService.getCurrentUser(userId));
        }
    }

    @ApiOperation(value = "Get user information according to ID")
    @GetMapping(value = "{" + USER_KEY + "}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object getUserById(@PathVariable(value = USER_KEY) int userId,
                              HttpServletRequest request,
                              HttpServletResponse response) throws Exception {
        int requestUserId = authenticationService.checkUserAuthWithCookie(request, response);
        return ResponseEntityBuilder.ok(userService.getUserById(userId, requestUserId));
    }

    @ApiOperation(value = "Add new user")
    @PostMapping(value = "", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object addUser(@RequestBody UserAddReq userAddReq,
                          HttpServletRequest request,
                          HttpServletResponse response) throws Exception {
        int requestUserId = authenticationService.checkUserAuthWithCookie(request, response);
        authenticationService.checkUserIsAdmin(requestUserId);
        return ResponseEntityBuilder.ok(userService.addUser(userAddReq, requestUserId));
    }

    @ApiOperation(value = "update user information")
    @PutMapping(value = "{" + USER_KEY + "}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object updateUser(@RequestBody UserUpdateReq userUpdateReq,
                             @PathVariable(value = USER_KEY) int userId,
                             HttpServletRequest request,
                             HttpServletResponse response) throws Exception {
        int requestId = authenticationService.checkUserAuthWithCookie(request, response);
        return ResponseEntityBuilder.ok(userService.updateUser(userUpdateReq, requestId, userId));
    }

    @ApiOperation(value = "Reactivate user")
    @PutMapping(value = "{" + USER_KEY + "}" + "/reactivate", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object reactivateUser(@PathVariable(value = USER_KEY) int userId,
                                 HttpServletRequest request,
                                 HttpServletResponse response) throws Exception {
        int requestUserId = authenticationService.checkUserAuthWithCookie(request, response);
        authenticationService.checkUserIsAdmin(requestUserId);
        return ResponseEntityBuilder.ok(userService.reactivateUser(userId, requestUserId));
    }

    @ApiOperation(value = "Modify user password")
    @PutMapping(value = "{" + USER_KEY + "}" + "/password", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object updatePassword(@PathVariable(value = USER_KEY) int userId,
                                 @RequestBody PasswordUpdateReq updateReq,
                                 HttpServletRequest request,
                                 HttpServletResponse response) throws Exception {
        int requestId = authenticationService.checkAllUserAuthWithCookie(request, response);
        if (requestId < 1) {
            return ResponseEntityBuilder.ok(authenticationService.updateSuperUserPassword(updateReq));
        } else {
            return ResponseEntityBuilder.ok(userService.updatePassword(updateReq, userId, requestId));
        }
    }

    @ApiOperation(value = "Deactivating a user does not delete it, but deactivates it")
    @DeleteMapping(value = "{" + USER_KEY + "}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object stopUser(@PathVariable(value = USER_KEY) int userId,
                           HttpServletRequest request,
                           HttpServletResponse response) throws Exception {
        int requestUserId = authenticationService.checkUserAuthWithCookie(request, response);
        authenticationService.checkUserIsAdmin(requestUserId);
        return ResponseEntityBuilder.ok(userService.stopUser(userId, requestUserId));
    }

    @ApiOperation(value = "User moves out of space")
    @DeleteMapping(value = "/move/{" + USER_KEY + "}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object moveUser(@PathVariable(value = USER_KEY) int userId,
                           HttpServletRequest request,
                           HttpServletResponse response) throws Exception {
        int requestUserId = authenticationService.checkUserAuthWithCookie(request, response);
        authenticationService.checkUserIsAdmin(requestUserId);
        return ResponseEntityBuilder.ok(userService.moveUser(userId, requestUserId));
    }

    @ApiOperation(value = "update user qbnewb")
    @PutMapping(value = "{" + USER_KEY + "}" + "/qbnewb", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object setQpnewb(@PathVariable(value = USER_KEY) int userId,
                            HttpServletRequest request,
                            HttpServletResponse response) throws Exception {
        int requestId = authenticationService.checkUserAuthWithCookie(request, response);
        return ResponseEntityBuilder.ok(userService.setQbnewb(requestId, userId));
    }

    @ApiOperation(value = "Resend invitation message")
    @PostMapping(value = "{" + USER_KEY + "}" +  "/send_invite", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object sendInvite(@PathVariable(value = USER_KEY) int userId,
                             HttpServletRequest request,
                             HttpServletResponse response) throws Exception {
        int requestUserId = authenticationService.checkUserAuthWithCookie(request, response);
        authenticationService.checkUserIsAdmin(requestUserId);
        return ResponseEntityBuilder.ok(userService.sendInvite(requestUserId, userId));
    }

    @ApiOperation(value = "delete user")
    @DeleteMapping(value = "delete/{" + USER_KEY + "}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object deleteUser(@PathVariable(value = USER_KEY) int userId,
                             HttpServletRequest request,
                             HttpServletResponse response) throws Exception {
        authenticationService.checkSuperAdminUserAuthWithCookie(request, response);
        userService.deleteUser(userId);
        return ResponseEntityBuilder.ok();
    }

    @ApiOperation(value = "Get the list of all spaces where the user is located. Todo: not implemented yet")
    @GetMapping(value = "spaces", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object getUserSpaces(HttpServletRequest request, HttpServletResponse response) throws Exception {
        int userId = authenticationService.checkUserAuthWithCookie(request, response);
        log.debug("User {} get space list.", userId);
        return ResponseEntityBuilder.ok();
    }

    @ApiOperation(value = "User switches the current space in use. Todo: not implemented yet")
    @PutMapping(value = "space", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object updateUserSpace(@RequestBody UserSpaceReq userSpaceReq,
                                  HttpServletRequest request,
                                  HttpServletResponse response) throws Exception {
        int userId = authenticationService.checkUserAuthWithCookie(request, response);
        log.debug("User {} get space list.", userId);
        return ResponseEntityBuilder.ok();
    }

}
