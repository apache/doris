// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.http.ActionController;
import com.baidu.palo.http.BaseRequest;
import com.baidu.palo.http.BaseResponse;
import com.baidu.palo.http.action.WebBaseAction;
import com.baidu.palo.master.MetaHelper;
import com.baidu.palo.system.Frontend;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.File;

public class MetaBaseAction extends WebBaseAction {
    private static final Logger LOG = LogManager.getLogger(MetaBaseAction.class);
    private static String CONTENT_DISPOSITION = "Content-disposition";

    public static final String CLUSTER_ID = "cluster_id";
    public static final String TOKEN = "token";

    protected File imageDir;

    public MetaBaseAction(ActionController controller, File imageDir) {
        super(controller);
        this.imageDir = imageDir;
    }

    @Override
    public boolean needAdmin() {
        return false;
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        if (needCheckClientIsFe()) {
            try {
                checkFromValidFe(request, response);
            } catch (InvalidClientException e) {
                response.appendContent("invalid client host.");
                writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
                return;
            }
        } else {
            super.execute(request, response);
        }
    }

    protected boolean needCheckClientIsFe() {
        return true;
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

    private boolean isFromValidFe(BaseRequest request) {
        String clientHost = request.getHostString();
        Frontend fe = Catalog.getInstance().getFeByHost(clientHost);
        if (fe == null) {
            LOG.warn("request is not from valid FE . client: {}", clientHost);
            return false;
        }
        return true;
    }

    private void checkFromValidFe(BaseRequest request, BaseResponse response)
            throws InvalidClientException {
        if (isFromValidFe(request)) {
            throw new InvalidClientException("invalid client host");
        }
    }
}
