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

package org.apache.doris.httpv2.ui;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Version;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.common.proc.ProcService;
import org.apache.doris.httpv2.HttpAuthManager.SessionValue;
import org.apache.doris.mysql.privilege.PrivPredicate;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/rest/v1/ui")
public class UiHomeController {
    @GetMapping("/home/version")
    public UiApiResponse<UiVersionInfo> version(HttpServletRequest request) {
        UiRequestContext.session(request);
        UiVersionInfo version = new UiVersionInfo(
                Version.DORIS_BUILD_VERSION,
                Version.DORIS_BUILD_HASH,
                Version.DORIS_BUILD_INFO,
                Version.DORIS_BUILD_TIME,
                Version.DORIS_FEATURE_LIST);
        return new UiApiResponse<>(version, UiRequestContext.requestId(request));
    }

    @GetMapping("/nodes/frontends")
    public UiApiResponse<UiNodeTable> frontends(HttpServletRequest request) throws Exception {
        return nodes(request, "/frontends");
    }

    @GetMapping("/nodes/backends")
    public UiApiResponse<UiNodeTable> backends(HttpServletRequest request) throws Exception {
        return nodes(request, "/backends");
    }

    private UiApiResponse<UiNodeTable> nodes(HttpServletRequest request, String path) throws Exception {
        SessionValue session = UiRequestContext.session(request);
        if (!canViewNodeStatus(session)) {
            throw UiApiException.forbidden(UiCapability.NODE_STATUS_VIEW);
        }
        ProcResult result = fetchNodeResult(path);
        UiNodeTable table = new UiNodeTable(result.getColumnNames(), result.getRows());
        return new UiApiResponse<>(table, UiRequestContext.requestId(request));
    }

    protected boolean canViewNodeStatus(SessionValue session) {
        return Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(session.currentUser, PrivPredicate.ADMIN_OR_NODE);
    }

    protected ProcResult fetchNodeResult(String path) throws Exception {
        return ProcService.getInstance().open(path).fetchResult();
    }
}
