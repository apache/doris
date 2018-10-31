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

package org.apache.doris.http.rest;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;

import io.netty.handler.codec.http.HttpMethod;

public class HealthAction extends RestBaseAction {
    public HealthAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction (ActionController controller)
            throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/health", new HealthAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        response.setContentType("application/json");

        RestResult result = new RestResult();
        result.addResultEntry("total_backend_num", Catalog.getCurrentSystemInfo().getBackendIds(false).size());
        result.addResultEntry("online_backend_num", Catalog.getCurrentSystemInfo().getBackendIds(true).size());
        sendResult(request, response, result);
    }
}
