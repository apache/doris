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

package org.apache.doris.common.security.authentication;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class HadoopSimpleAuthenticator implements HadoopAuthenticator {
    private static final Logger LOG = LogManager.getLogger(HadoopSimpleAuthenticator.class);
    private final SimpleAuthenticationConfig config;
    private UserGroupInformation ugi;

    public HadoopSimpleAuthenticator(SimpleAuthenticationConfig config) {
        this.config = config;
    }

    @Override
    public UserGroupInformation getUGI() {
        String hadoopUserName = config.getUsername();
        if (hadoopUserName == null) {
            hadoopUserName = "hadoop";
            config.setUsername(hadoopUserName);
            LOG.debug(AuthenticationConfig.HADOOP_USER_NAME + " is unset, use default user: hadoop");
        }
        try {
            // get login user just for simple auth
            ugi = UserGroupInformation.getLoginUser();
            if (ugi.getUserName().equals(hadoopUserName)) {
                return ugi;
            }
        } catch (IOException e) {
            LOG.warn("A SecurityException occurs with simple, do login immediately.", e);
        }
        ugi = UserGroupInformation.createRemoteUser(hadoopUserName);
        UserGroupInformation.setLoginUser(ugi);
        LOG.debug("Login by proxy user, hadoop.username: {}", hadoopUserName);
        return ugi;
    }

    @Override
    public <T> T doAs(PrivilegedExceptionAction<T> action) throws Exception {
        if (ugi != null) {
            return ugi.doAs(action);
        }
        return action.run();
    }
}
