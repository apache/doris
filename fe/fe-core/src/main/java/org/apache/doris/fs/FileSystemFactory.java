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

package org.apache.doris.fs;

import org.apache.doris.catalog.AuthType;
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.common.UserException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class FileSystemFactory {
    private static final Logger LOG = LogManager.getLogger(FileSystemFactory.class);

    public static FileSystem createDfsFileSystem(String remotePath, Map<String, String> hdfsProperties)
            throws UserException {
        String username = hdfsProperties.get(HdfsResource.HADOOP_USER_NAME);
        Configuration conf = new HdfsConfiguration();
        boolean isSecurityEnabled = false;
        for (Map.Entry<String, String> propEntry : hdfsProperties.entrySet()) {
            conf.set(propEntry.getKey(), propEntry.getValue());
            if (propEntry.getKey().equals(HdfsResource.HADOOP_SECURITY_AUTHENTICATION)
                    && propEntry.getValue().equals(AuthType.KERBEROS.getDesc())) {
                isSecurityEnabled = true;
            }
        }
        try {
            if (isSecurityEnabled) {
                UserGroupInformation.setConfiguration(conf);
                UserGroupInformation.loginUserFromKeytab(
                        hdfsProperties.get(HdfsResource.HADOOP_KERBEROS_PRINCIPAL),
                        hdfsProperties.get(HdfsResource.HADOOP_KERBEROS_KEYTAB));
            }
            if (username == null) {
                return FileSystem.get(java.net.URI.create(remotePath), conf);
            } else {
                return FileSystem.get(java.net.URI.create(remotePath), conf, username);
            }
        } catch (Exception e) {
            LOG.error("errors while connect to " + remotePath, e);
            throw new UserException("errors while connect to " + remotePath, e);
        }
    }
}
