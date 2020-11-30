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

package org.apache.doris.httpv2.rest;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.util.SmallFileMgr;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;

import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
public class GetSmallFileAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(GetSmallFileAction.class);

    @RequestMapping(path = "/api/get_small_file", method = RequestMethod.GET)
    public Object execute(HttpServletRequest request, HttpServletResponse response) {
        String token = request.getParameter("token");
        String fileIdStr = request.getParameter("file_id");
        // check param empty
        if (Strings.isNullOrEmpty(token) || Strings.isNullOrEmpty(fileIdStr)) {
            return ResponseEntityBuilder.badRequest("Missing parameter. Need token and file id");
        }

        // check token
        if (!token.equals(Catalog.getCurrentCatalog().getToken())) {
            return ResponseEntityBuilder.okWithCommonError("Invalid token");
        }

        long fileId = -1;
        try {
            fileId = Long.valueOf(fileIdStr);
        } catch (NumberFormatException e) {
            return ResponseEntityBuilder.badRequest("Invalid file id format: " + fileIdStr);
        }

        SmallFileMgr fileMgr = Catalog.getCurrentCatalog().getSmallFileMgr();
        SmallFileMgr.SmallFile smallFile = fileMgr.getSmallFile(fileId);
        if (smallFile == null || !smallFile.isContent) {
            return ResponseEntityBuilder.okWithCommonError("File not found or is not content");
        }

        String method = request.getMethod();
        if (method.equalsIgnoreCase("GET")) {
            try {
                getFile(request, response, smallFile.getContentBytes(), smallFile.name);
            } catch (IOException e) {
                return ResponseEntityBuilder.internalError(e.getMessage());
            }
        }
        return ResponseEntityBuilder.okWithEmpty();
    }
}
