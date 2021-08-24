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

import org.apache.doris.stack.model.request.space.ClusterCreateReq;
import org.apache.doris.stack.model.request.space.UserSpaceCreateReq;
import org.apache.doris.stack.model.request.space.UserSpaceUpdateReq;
import org.apache.doris.stack.rest.ResponseEntityBuilder;
import org.apache.doris.stack.service.PaloUserSpaceService;
import org.apache.doris.stack.service.user.AuthenticationService;
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
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Api(tags = "Doris Cluster user space managementAPI")
@RestController
@RequestMapping(value = "/api/space/")
@Slf4j
public class PaloUserSpaceController extends BaseController {
    @Autowired
    private AuthenticationService authenticationService;

    @Autowired
    private PaloUserSpaceService spaceService;

    @ApiOperation(value = "Cluster user space management creates a user space and "
            + "returns the space ID (super administrator access)")
    @PostMapping(value = "create", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object create(
            HttpServletRequest request, HttpServletResponse response,
            @RequestBody UserSpaceCreateReq createReq) throws Exception {
        log.debug("Super user create palo user space.");
        authenticationService.checkSuperAdminUserAuthWithCookie(request, response);
        return ResponseEntityBuilder.ok(spaceService.create(createReq));
    }

    @ApiOperation(value = "Get a list of all spaces (super administrator access)")
    @GetMapping(value = "all", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object getAll(HttpServletRequest request, HttpServletResponse response) throws Exception {
        log.debug("Super user get all palo user space.");
        authenticationService.checkSuperAdminUserAuthWithCookie(request, response);
        return ResponseEntityBuilder.ok(spaceService.getAllSpaceBySuperUser());
    }

    @ApiOperation(value = "Verify the correctness of Palo cluster information, return true correctly, "
            + "and return exception directly in case of error. (super administrator / space administrator access)")
    @PostMapping(value = "validate", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object validateCluster(
            HttpServletRequest request, HttpServletResponse response,
            @RequestBody ClusterCreateReq clusterCreateReq) throws Exception {
        log.debug("Palo cluster info validate by superUser.");
        int userId = authenticationService.checkAllUserAuthWithCookie(request, response);
        if (userId > 0) {
            authenticationService.checkUserIsAdmin(userId);
        }
        spaceService.validateCluster(clusterCreateReq);
        return ResponseEntityBuilder.ok(true);
    }

    @ApiOperation(value = "Verify whether the space name meets the requirements. If it meets the requirements, "
            + "it will return true, and the error will directly return an exception. (super administrator / "
            + "space administrator access)")
    @GetMapping(value = "name/{name}/check" , produces = MediaType.APPLICATION_JSON_VALUE)
    public Object nameCheck(
            HttpServletRequest request, HttpServletResponse response,
            @PathVariable(value = "name") String name) throws Exception {
        log.debug("Palo cluster info validate by superUser.");
        int userId = authenticationService.checkAllUserAuthWithCookie(request, response);
        if (userId > 0) {
            authenticationService.checkUserIsAdmin(userId);
        }
        return ResponseEntityBuilder.ok(spaceService.nameCheck(name));
    }

    @ApiOperation(value = "Modify the user space information. If the cluster information already exists, "
            + "the cluster information should not be passed in. (space administrator access)")
    @PutMapping(value = "{" + SPACE_KEY + "}/update", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object update(
            HttpServletRequest request, HttpServletResponse response,
            @PathVariable(value = SPACE_KEY) int spaceId,
            @RequestBody UserSpaceUpdateReq updateReq) throws Exception {
        log.debug("Super user update palo user space.");
        int userId = authenticationService.checkUserAuthWithCookie(request, response);
        authenticationService.checkUserIsAdmin(userId);
        return ResponseEntityBuilder.ok(spaceService.update(userId, spaceId, updateReq));
    }

    @ApiOperation(value = "Obtain and view space information according to "
            + "space ID (super administrator / space administrator access)")
    @GetMapping(value = "{" + SPACE_KEY + "}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object getById(HttpServletRequest request,
                          HttpServletResponse response,
                          @PathVariable(value = SPACE_KEY) int spaceId) throws Exception {
        log.debug("Get space by id.");
        int userId = authenticationService.checkAllUserAuthWithCookie(request, response);
        if (userId > 0) {
            authenticationService.checkUserIsAdmin(userId);
        }
        return ResponseEntityBuilder.ok(spaceService.getById(userId, spaceId));
    }

    @ApiOperation(value = "Delete space (super administrator access)")
    @DeleteMapping(value = "{" + SPACE_KEY + "}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Object deleteSpace(@PathVariable(value = SPACE_KEY) int spaceId,
                                HttpServletRequest request, HttpServletResponse response) throws Exception {
        log.debug("delete space by spaceId: {}", spaceId);
        authenticationService.checkSuperAdminUserAuthWithCookie(request, response);
        spaceService.deleteSpace(spaceId);
        return ResponseEntityBuilder.ok();
    }
}
