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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public interface HadoopAuthenticator {

    UserGroupInformation getUGI() throws IOException;

    default <T> T doAs(PrivilegedExceptionAction<T> action) throws IOException {
        try {
            return getUGI().doAs(action);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    static HadoopAuthenticator getHadoopAuthenticator(AuthenticationConfig config) {
        if (config instanceof KerberosAuthenticationConfig) {
            return new HadoopKerberosAuthenticator((KerberosAuthenticationConfig) config);
        } else {
            return new HadoopSimpleAuthenticator((SimpleAuthenticationConfig) config);
        }
    }
}
