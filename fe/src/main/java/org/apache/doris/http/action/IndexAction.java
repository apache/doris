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

package org.apache.doris.http.action;

import org.apache.doris.common.Version;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;

import io.netty.handler.codec.http.HttpMethod;

public class IndexAction extends WebBaseAction {

    public IndexAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction (ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/index", new IndexAction(controller));
    }

    @Override
    public void executeGet(BaseRequest request, BaseResponse response) {
        getPageHeader(request, response.getContent());
        appendVersionInfo(response.getContent());
        appendHardwareInfo(response.getContent());
        getPageFooter(response.getContent());
        writeResponse(request, response);
    }

    private void appendVersionInfo(StringBuilder buffer) {
        buffer.append("<h2>Version</h2>");
        buffer.append("<pre>version info<br/>");
        buffer.append("Version: " + Version.PALO_BUILD_VERSION + "<br/>");
        buffer.append("Git: " + Version.PALO_BUILD_HASH + "<br/>");
        buffer.append("Build Info: " + Version.PALO_BUILD_INFO + "<br/>");
        buffer.append("Build Time: " + Version.PALO_BUILD_TIME + "<br/>");
        buffer.append("</pre>");
    }
    
    private void appendHardwareInfo(StringBuilder buffer) {
        buffer.append("<h2>Hardware Info</h2>");
        buffer.append("<pre>Hardware info(to be added)</pre>");
    }
}
