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

import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * Simple (non-Kerberos) implementation of {@link HadoopAuthenticator}.
 * When a {@code hadoopUsername} is provided, wraps all actions inside
 * {@link UserGroupInformation#doAs} so that HDFS operations use that
 * identity (important for permission checks). Otherwise, executes
 * actions directly as the FE process user.
 */
public class SimpleHadoopAuthenticator implements HadoopAuthenticator {

    private final UserGroupInformation ugi;

    public SimpleHadoopAuthenticator() {
        this.ugi = null;
    }

    public SimpleHadoopAuthenticator(String hadoopUsername) {
        if (hadoopUsername != null && !hadoopUsername.isEmpty()) {
            this.ugi = UserGroupInformation.createRemoteUser(hadoopUsername);
        } else {
            this.ugi = null;
        }
    }

    @Override
    public <T> T doAs(IOCallable<T> action) throws IOException {
        if (ugi == null) {
            return action.call();
        }
        try {
            return ugi.doAs((PrivilegedExceptionAction<T>) action::call);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during HDFS operation as user " + ugi.getUserName(), e);
        }
    }
}
