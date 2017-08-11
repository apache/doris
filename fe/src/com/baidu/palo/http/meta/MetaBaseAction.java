// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.http.meta;

import com.baidu.palo.http.ActionController;
import com.baidu.palo.http.BaseRequest;
import com.baidu.palo.http.BaseResponse;
import com.baidu.palo.http.action.WebBaseAction;
import com.baidu.palo.master.MetaHelper;

import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.File;

public class MetaBaseAction extends WebBaseAction {
    private static String CONTENT_DISPOSITION = "Content-disposition";
    
    protected File imageDir;
    
    public MetaBaseAction(ActionController controller, File imageDir) {
        super(controller);
        this.imageDir = imageDir;
    }
    
    @Override
    public boolean needAdmin() {
        return false;
    }
    
    protected void writeFileResponse(BaseRequest request, BaseResponse response, File file) {
        if (file == null || !file.exists()) {
            response.appendContent("File not exist.");
            writeResponse(request, response, HttpResponseStatus.NOT_FOUND);
            return;
        }
        
        // add customed header
        response.addHeader(CONTENT_DISPOSITION, "attachment; filename=" + file.getName());
        response.addHeader(MetaHelper.X_IMAGE_SIZE, String.valueOf(file.length()));
        
        writeFileResponse(request, response, HttpResponseStatus.OK, file);
        return;
    }
}
