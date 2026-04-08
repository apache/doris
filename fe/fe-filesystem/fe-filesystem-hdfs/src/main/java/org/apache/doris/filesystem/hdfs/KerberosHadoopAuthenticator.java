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

package org.apache.doris.filesystem.hdfs;

import org.apache.doris.filesystem.spi.HadoopAuthenticator;
import org.apache.doris.filesystem.spi.IOCallable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * Kerberos-based implementation of {@link HadoopAuthenticator}.
 * Logs in from a keytab and executes actions as the Kerberos principal via UGI.doAs().
 */
public class KerberosHadoopAuthenticator implements HadoopAuthenticator {

    private static final Logger LOG = LogManager.getLogger(KerberosHadoopAuthenticator.class);

    private final String principal;
    private final String keytab;
    private volatile UserGroupInformation ugi;

    public KerberosHadoopAuthenticator(String principal, String keytab, Configuration conf) {
        this.principal = principal;
        this.keytab = keytab;
        try {
            UserGroupInformation.setConfiguration(conf);
            this.ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
            LOG.info("Kerberos login succeeded for principal={}", principal);
        } catch (IOException e) {
            throw new RuntimeException("Failed to login with Kerberos principal=" + principal
                    + ", keytab=" + keytab, e);
        }
    }

    @Override
    public <T> T doAs(IOCallable<T> action) throws IOException {
        try {
            return ugi.doAs((PrivilegedExceptionAction<T>) action::call);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Kerberos doAs interrupted for principal=" + principal, e);
        }
    }
}
