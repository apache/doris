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

public class HadoopSimpleAuthenticator implements HadoopAuthenticator {
    private static final Logger LOG = LogManager.getLogger(HadoopSimpleAuthenticator.class);
    private final UserGroupInformation ugi;

    public HadoopSimpleAuthenticator(SimpleAuthenticationConfig config) {
        String hadoopUserName = config.getUsername();
        if (hadoopUserName == null) {
            hadoopUserName = "hadoop";
            config.setUsername(hadoopUserName);
            if (LOG.isDebugEnabled()) {
                LOG.debug("{} is unset, use default user: hadoop", AuthenticationConfig.HADOOP_USER_NAME);
            }
        }
        ugi = UserGroupInformation.createRemoteUser(hadoopUserName);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Login by proxy user, hadoop.username: {}", hadoopUserName);
        }
    }

    @Override
    public UserGroupInformation getUGI() {
        return ugi;
    }
}
