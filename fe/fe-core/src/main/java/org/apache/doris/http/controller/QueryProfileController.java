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

package org.apache.doris.http.controller;

import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.http.entity.HttpStatus;
import org.apache.doris.http.entity.ResponseEntity;

import com.google.common.base.Strings;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/rest/v1")
public class QueryProfileController {

    @RequestMapping(path = "/query_profile/{queryId}",method = RequestMethod.GET)
    public Object profile(@PathVariable String queryId){
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build();

        if (Strings.isNullOrEmpty(queryId)) {
            entity.setCode(HttpStatus.BAD_REQUEST.value());
            entity.setMsg("Must specify a query_id[]");
            return entity;
        }
        String profile = ProfileManager.getInstance().getProfile(queryId);
        profile = profile.replaceAll("\n","</br>");
        profile = profile.replaceAll(" ","&nbsp;&nbsp;");
        entity.setData(profile);
        return entity;
    }
}
