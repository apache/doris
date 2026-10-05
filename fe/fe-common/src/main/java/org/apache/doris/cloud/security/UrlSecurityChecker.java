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

package org.apache.doris.cloud.security;

import org.apache.doris.common.Config;
import org.apache.doris.common.LogUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Method;

public class UrlSecurityChecker extends SecurityChecker {
    private static final Logger LOG = LogManager.getLogger(UrlSecurityChecker.class);
    private static Method jdbcUrlCheckMethod = null;
    private static Method urlSecurityCheckMethod = null;
    private static Method urlSecurityStopCheckMethod = null;

    static {
        try {
            Class clazz = Class.forName(Config.security_checker_class_name);
            jdbcUrlCheckMethod = clazz.getMethod("filterJdbcConnectionSource", String.class);
            urlSecurityCheckMethod = clazz.getMethod("startSSRFNetHookChecking", String.class);
            urlSecurityStopCheckMethod = clazz.getMethod("stopSSRFNetHookChecking", String.class);
        } catch (Exception e) {
            LOG.warn("security_checker_class_name:{} exception:", Config.security_checker_class_name, e);
            LogUtils.stderr("Failed to find com.aliyun.securitysdk.SecurityUtil's method");
            e.printStackTrace();
        }
    }

    protected UrlSecurityChecker() {}

    @Override
    public String getSafeJdbcUrl(String originJdbcUrl) throws Exception {
        if (jdbcUrlCheckMethod != null) {
            return (String) (jdbcUrlCheckMethod.invoke(null, originJdbcUrl));
        }
        throw new Exception("SecurityUtil.filterJdbcConnectionSource not found");
    }

    @Override
    public void startSSRFChecking(String originUri) throws Exception {
        if (urlSecurityCheckMethod != null) {
            urlSecurityCheckMethod.invoke(null, originUri);
            return;
        }
        throw new Exception("SecurityUtil.startSSRFNetHookChecking not found");
    }

    @Override
    public void stopSSRFChecking() {
        if (urlSecurityStopCheckMethod != null) {
            try {
                urlSecurityStopCheckMethod.invoke(null);
            } catch (Exception e) {
                LOG.warn("failed to stop SSRF checking, log and ignore.", e);
            }
            return;
        }
    }
}
