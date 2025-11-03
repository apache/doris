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

import org.apache.doris.common.Version;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

// This class is a RESTFUL interface to get fe version info.
// It will be used in get fe version info.
// Usage:
//      wget http://fe_host:fe_http_port/api/fe_version_info
@RestController
public class FeVersionInfoAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(ProfileAction.class);

    @RequestMapping(path = "/api/fe_version_info", method = RequestMethod.GET)
    protected Object profile(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        Map<String, Object> feVersionInfo = Maps.newHashMap();
        feVersionInfo.put("dorisBuildVersionPrefix", Version.DORIS_BUILD_VERSION_PREFIX);
        feVersionInfo.put("dorisBuildVersionMajor", Version.DORIS_BUILD_VERSION_MAJOR);
        feVersionInfo.put("dorisBuildVersionMinor", Version.DORIS_BUILD_VERSION_MINOR);
        feVersionInfo.put("dorisBuildVersionPatch", Version.DORIS_BUILD_VERSION_PATCH);
        feVersionInfo.put("dorisBuildVersionRcVersion", Version.DORIS_BUILD_VERSION_RC_VERSION);
        feVersionInfo.put("dorisBuildVersion", Version.DORIS_BUILD_VERSION);
        feVersionInfo.put("dorisBuildHash", Version.DORIS_BUILD_HASH);
        feVersionInfo.put("dorisBuildShortHash", Version.DORIS_BUILD_SHORT_HASH);
        feVersionInfo.put("dorisBuildTime", Version.DORIS_BUILD_TIME);
        feVersionInfo.put("dorisBuildInfo", Version.DORIS_BUILD_INFO);
        feVersionInfo.put("dorisJavaCompileVersion", Version.DORIS_JAVA_COMPILE_VERSION);

        Map<String, Object> result = Maps.newHashMap();
        result.put("feVersionInfo", feVersionInfo);
        return ResponseEntityBuilder.ok(result);
    }
}
