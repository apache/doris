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

import com.google.common.base.Strings;

/**
 * SecurityChecker is used to check if the url is safe
 */
public abstract class SecurityChecker {
    private static class SingletonHolder {
        private static final SecurityChecker INSTANCE = Strings.isNullOrEmpty(Config.security_checker_class_name)
                ? new DummySecurityChecker() : new UrlSecurityChecker();
    }

    protected SecurityChecker() {}

    public static SecurityChecker getInstance() {
        return SingletonHolder.INSTANCE;
    }

    /**
     * Check and return safe jdbc url, avoid sql injection or other security issues
     * @param originJdbcUrl
     * @return
     * @throws Exception
     */
    public abstract String getSafeJdbcUrl(String originJdbcUrl) throws Exception;

    /**
     * Check if the uri is safe, avoid SSRF attack
     * Only handle http:// https://
     * @param originUri
     * @throws Exception
     */
    public abstract void startSSRFChecking(String originUri) throws Exception;

    public abstract void stopSSRFChecking();
}
